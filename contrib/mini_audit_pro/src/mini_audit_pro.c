#include "postgres.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

#define MINI_AUDIT_MAX_NAME 64
#define MINI_AUDIT_MAX_OP 32
#define MINI_AUDIT_MAX_JSON 512

typedef struct AuditEvent
{
	TimestampTz ts;
	char username[MINI_AUDIT_MAX_NAME];
	char dbname[MINI_AUDIT_MAX_NAME];
	char schema_name[MINI_AUDIT_MAX_NAME];
	char table_name[MINI_AUDIT_MAX_NAME];
	char op[MINI_AUDIT_MAX_OP];
	bool has_old;
	bool has_new;
	char row_old[MINI_AUDIT_MAX_JSON];
	char row_new[MINI_AUDIT_MAX_JSON];
} AuditEvent;

typedef struct AuditQueue
{
	slock_t mutex;
	uint32 head;
	uint32 tail;
	uint32 count;
	uint32 capacity;
	pid_t worker_pid;
	AuditEvent events[FLEXIBLE_ARRAY_MEMBER];
} AuditQueue;

static bool mini_audit_enable = true;
static char *mini_audit_tables = NULL;
static char *mini_audit_worker_database = NULL;
static int mini_audit_queue_size = 8192;
static int mini_audit_flush_interval_ms = 100;

static AuditQueue *audit_queue = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

void _PG_init(void);
void _PG_fini(void);

static Size mini_audit_memsize(void);
static void mini_audit_shmem_startup(void);
void mini_audit_worker_main(Datum main_arg) pg_attribute_noreturn();
static bool mini_audit_match_table(const char *schema, const char *table);
static void mini_audit_enqueue(const char *schema, const char *table, const char *op,
						   const char *row_old, const char *row_new);
static void mini_audit_executor_end(QueryDesc *queryDesc);
static void mini_audit_process_utility(PlannedStmt *pstmt, const char *queryString,
							   bool readOnlyTree, ProcessUtilityContext context,
							   ParamListInfo params, QueryEnvironment *queryEnv,
							   DestReceiver *dest, QueryCompletion *qc);
static void mini_audit_flush_batch(AuditEvent *batch, int nitems);

static Size
mini_audit_memsize(void)
{
	Size sz;

	sz = MAXALIGN(sizeof(AuditQueue));
	sz += MAXALIGN(sizeof(AuditEvent) * mini_audit_queue_size);
	return sz;
}

static void
mini_audit_shmem_startup(void)
{
	bool found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	audit_queue = ShmemInitStruct("mini_audit_pro", mini_audit_memsize(), &found);
	if (!found)
	{
		MemSet(audit_queue, 0, mini_audit_memsize());
		SpinLockInit(&audit_queue->mutex);
		audit_queue->capacity = mini_audit_queue_size;
	}
	LWLockRelease(AddinShmemInitLock);
}

static bool
mini_audit_match_table(const char *schema, const char *table)
{
	char *raw;
	char *tok;
	char *end;
	char *saveptr = NULL;
	char fqname[NAMEDATALEN * 2 + 2];

	if (!mini_audit_tables || mini_audit_tables[0] == '\0')
		return false;

	snprintf(fqname, sizeof(fqname), "%s.%s", schema, table);
	raw = pstrdup(mini_audit_tables);
	for (tok = strtok_r(raw, ",", &saveptr); tok != NULL; tok = strtok_r(NULL, ",", &saveptr))
	{
		while (*tok == ' ' || *tok == '\t')
			tok++;
		end = tok + strlen(tok) - 1;
		while (end >= tok && (*end == ' ' || *end == '\t'))
			*end-- = '\0';
		if (pg_strcasecmp(tok, fqname) == 0)
		{
			pfree(raw);
			return true;
		}
	}
	pfree(raw);
	return false;
}

static void
mini_audit_flush_batch(AuditEvent *batch, int nitems)
{
	int		i;

	if (nitems <= 0)
		return;

	PG_TRY();
	{
		StartTransactionCommand();
		SPI_connect();

		for (i = 0; i < nitems; i++)
		{
			Datum values[8];
			char nulls[8] = {' ', ' ', ' ', ' ', ' ', ' ', 'n', 'n'};
			Oid argtypes[8] = {
				TIMESTAMPTZOID, TEXTOID, TEXTOID, TEXTOID,
				TEXTOID, TEXTOID, JSONBOID, JSONBOID
			};

			values[0] = TimestampTzGetDatum(batch[i].ts);
			values[1] = CStringGetTextDatum(batch[i].username);
			values[2] = CStringGetTextDatum(batch[i].dbname);
			values[3] = CStringGetTextDatum(batch[i].schema_name);
			values[4] = CStringGetTextDatum(batch[i].table_name);
			values[5] = CStringGetTextDatum(batch[i].op);

			if (batch[i].has_old)
			{
				nulls[6] = ' ';
				values[6] = DirectFunctionCall1(jsonb_in,
									CStringGetDatum(batch[i].row_old));
			}

			if (batch[i].has_new)
			{
				nulls[7] = ' ';
				values[7] = DirectFunctionCall1(jsonb_in,
									CStringGetDatum(batch[i].row_new));
			}

			SPI_execute_with_args(
				"INSERT INTO mini_audit_log (ts, username, dbname, schema_name, table_name, op, row_old, row_new) "
				"VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
				8, argtypes, values, nulls, false, 0);
		}

		SPI_finish();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		edata = CopyErrorData();
		FlushErrorState();

		if (IsTransactionState())
			AbortCurrentTransaction();

		ereport(LOG,
				(errmsg("mini_audit_pro flush failed: %s", edata->message)));
		FreeErrorData(edata);
	}
	PG_END_TRY();
}

static void
mini_audit_enqueue(const char *schema, const char *table, const char *op,
				   const char *row_old, const char *row_new)
{
	uint32 idx;
	AuditEvent *ev;
	PGPROC *proc = NULL;

	if (!mini_audit_enable || !audit_queue)
		return;
	if (!mini_audit_match_table(schema, table))
		return;

	SpinLockAcquire(&audit_queue->mutex);
	if (audit_queue->count >= audit_queue->capacity)
	{
		SpinLockRelease(&audit_queue->mutex);
		return;
	}
	idx = audit_queue->tail;
	audit_queue->tail = (audit_queue->tail + 1) % audit_queue->capacity;
	audit_queue->count++;
	ev = &audit_queue->events[idx];
	ev->ts = GetCurrentTimestamp();
	strlcpy(ev->username, GetUserNameFromId(GetUserId(), false), sizeof(ev->username));
	strlcpy(ev->dbname, get_database_name(MyDatabaseId), sizeof(ev->dbname));
	strlcpy(ev->schema_name, schema, sizeof(ev->schema_name));
	strlcpy(ev->table_name, table, sizeof(ev->table_name));
	strlcpy(ev->op, op, sizeof(ev->op));
	ev->has_old = (row_old != NULL);
	ev->has_new = (row_new != NULL);
	if (row_old)
		strlcpy(ev->row_old, row_old, sizeof(ev->row_old));
	else
		ev->row_old[0] = '\0';
	if (row_new)
		strlcpy(ev->row_new, row_new, sizeof(ev->row_new));
	else
		ev->row_new[0] = '\0';
	if (audit_queue->worker_pid > 0)
		proc = BackendPidGetProc(audit_queue->worker_pid);
	SpinLockRelease(&audit_queue->mutex);

	if (proc)
		SetLatch(&proc->procLatch);
}

static void
mini_audit_executor_end(QueryDesc *queryDesc)
{
	if (mini_audit_enable && queryDesc && queryDesc->estate &&
		queryDesc->estate->es_result_relation_info)
	{
		EState *estate = queryDesc->estate;
		ResultRelInfo *rri = estate->es_result_relation_info;
		Relation rel = rri->ri_RelationDesc;
		const char *schema = get_namespace_name(RelationGetNamespace(rel));
		const char *table = RelationGetRelationName(rel);

		switch (queryDesc->operation)
		{
			case CMD_INSERT:
				mini_audit_enqueue(schema, table, "INSERT", NULL, NULL);
				break;
			case CMD_UPDATE:
				mini_audit_enqueue(schema, table, "UPDATE", NULL, NULL);
				break;
			case CMD_DELETE:
				mini_audit_enqueue(schema, table, "DELETE", NULL, NULL);
				break;
			default:
				break;
		}
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

static void
mini_audit_process_utility(PlannedStmt *pstmt, const char *queryString,
					   bool readOnlyTree, ProcessUtilityContext context,
					   ParamListInfo params, QueryEnvironment *queryEnv,
					   DestReceiver *dest, QueryCompletion *qc)
{
	Node *parsetree = pstmt->utilityStmt;

	if (prev_ProcessUtility)
		prev_ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
							queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
							queryEnv, dest, qc);

	if (!mini_audit_enable)
		return;

	if (IsA(parsetree, CreateStmt))
	{
		CreateStmt *stmt = (CreateStmt *) parsetree;
		RangeVar *rv = stmt->relation;
		mini_audit_enqueue(rv->schemaname ? rv->schemaname : "public", rv->relname,
						   "CREATE TABLE", NULL, NULL);
	}
	else if (IsA(parsetree, AlterTableStmt))
	{
		AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
		RangeVar *rv = stmt->relation;
		mini_audit_enqueue(rv->schemaname ? rv->schemaname : "public", rv->relname,
						   "ALTER TABLE", NULL, NULL);
	}
	else if (IsA(parsetree, DropStmt))
	{
		DropStmt *stmt = (DropStmt *) parsetree;
		if (stmt->removeType == OBJECT_TABLE)
		{
			ListCell *lc;
			foreach(lc, stmt->objects)
			{
				List *objname = lfirst(lc);
				char *schema = NULL;
				char *table = NULL;

				if (list_length(objname) == 2)
				{
					schema = strVal(list_nth(objname, 0));
					table = strVal(list_nth(objname, 1));
				}
				else if (list_length(objname) == 1)
				{
					schema = "public";
					table = strVal(list_nth(objname, 0));
				}
				if (schema && table)
					mini_audit_enqueue(schema, table, "DROP TABLE", NULL, NULL);
			}
		}
	}
}

void
mini_audit_worker_main(Datum main_arg)
{
	const char *dbname;

	dbname = mini_audit_worker_database;
	if (dbname == NULL || dbname[0] == '\0')
		dbname = "postgres";

	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection((char *) dbname, NULL, 0);

	if (audit_queue)
	{
		SpinLockAcquire(&audit_queue->mutex);
		audit_queue->worker_pid = MyProcPid;
		SpinLockRelease(&audit_queue->mutex);
	}

	while (!got_sigterm)
	{
		AuditEvent batch[128];
		int n = 0;
		int rc;

		ResetLatch(MyLatch);

		if (audit_queue)
		{
			SpinLockAcquire(&audit_queue->mutex);
			while (audit_queue->count > 0 && n < 128)
			{
				batch[n++] = audit_queue->events[audit_queue->head];
				audit_queue->head = (audit_queue->head + 1) % audit_queue->capacity;
				audit_queue->count--;
			}
			SpinLockRelease(&audit_queue->mutex);
		}

		mini_audit_flush_batch(batch, n);

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   mini_audit_flush_interval_ms,
					   PG_WAIT_EXTENSION);
		if (rc & WL_EXIT_ON_PM_DEATH)
			proc_exit(1);
		CHECK_FOR_INTERRUPTS();
	}

	proc_exit(0);
}

void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomBoolVariable("mini_audit.enable",
							 "Enable mini_audit_pro",
							 NULL,
							 &mini_audit_enable,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomStringVariable("mini_audit.tables",
						   "Comma separated list of schema.table to audit",
						   NULL,
						   &mini_audit_tables,
						   "",
						   PGC_SIGHUP,
						   0,
						   NULL,
						   NULL,
						   NULL);

	DefineCustomIntVariable("mini_audit.queue_size",
						"Queue size in number of events",
						NULL,
						&mini_audit_queue_size,
						8192,
						128,
						1048576,
						PGC_POSTMASTER,
						0,
						NULL,
						NULL,
						NULL);

	DefineCustomIntVariable("mini_audit.flush_interval_ms",
						"Background flush interval in ms",
						NULL,
						&mini_audit_flush_interval_ms,
						100,
						10,
						5000,
						PGC_SIGHUP,
						0,
						NULL,
						NULL,
						NULL);

	DefineCustomStringVariable("mini_audit.worker_database",
						   "Database where background worker writes mini_audit_log",
						   NULL,
						   &mini_audit_worker_database,
						   "postgres",
						   PGC_POSTMASTER,
						   0,
						   NULL,
						   NULL,
						   NULL);

	RequestAddinShmemSpace(mini_audit_memsize());
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = mini_audit_shmem_startup;

	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = mini_audit_executor_end;
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = mini_audit_process_utility;

	MemSet(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 1;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "mini_audit_pro");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "mini_audit_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "mini_audit_pro worker");
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;
	RegisterBackgroundWorker(&worker);
}

void
_PG_fini(void)
{
	ExecutorEnd_hook = prev_ExecutorEnd;
	ProcessUtility_hook = prev_ProcessUtility;
	shmem_startup_hook = prev_shmem_startup_hook;
}
