CREATE TABLE IF NOT EXISTS mini_audit_log (
    id          bigserial primary key,
    ts          timestamptz,
    username    text,
    dbname      text,
    schema_name text,
    table_name  text,
    op          text,
    row_old     jsonb,
    row_new     jsonb
);
