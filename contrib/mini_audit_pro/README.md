# mini_audit_pro

## 架构设计说明

mini_audit_pro 在 `shared_preload_libraries` 加载后注册两类 hook：

- `ExecutorEnd_hook`：捕获 INSERT/UPDATE/DELETE
- `ProcessUtility_hook`：捕获 CREATE TABLE/ALTER TABLE/DROP TABLE

hook 不直接写表，而是将审计事件写入共享内存环形队列（多生产者，单消费者）。
后台 worker 周期性或被 latch 唤醒后批量消费并使用 SPI 插入 `mini_audit_log`。

```
Hook -> Shared Memory Ring Queue -> Background Worker (batch flush) -> mini_audit_log
```

## 构建与运行

```bash
cd contrib/mini_audit_pro
make
make install
```

`postgresql.conf`：

```conf
shared_preload_libraries = 'mini_audit_pro'
mini_audit.enable = on
mini_audit.tables = 'public.users,public.orders'
mini_audit.queue_size = 8192
mini_audit.flush_interval_ms = 100
mini_audit.worker_database = 'postgres'
```

重启后执行：

```sql
CREATE EXTENSION mini_audit_pro;
```

## 使用 AI 工具协助开发

- 使用 AI 进行方案草拟：hook 选择、共享内存队列模型、worker 刷盘策略。
- 使用 AI 生成初版 C 扩展骨架，并基于编译错误迭代修正。
- 使用 AI 总结测试脚本与部署步骤。

### AI 聊天交互记录（摘要）

1. 明确约束：禁止 hook 内同步写表，必须异步队列+worker。
2. 选择实现点：DML 用 Executor hook，DDL 用 utility hook。
3. 设计队列：shared memory ring + spinlock + capacity 丢弃策略。
4. 设计消费者：周期 flush + latch 唤醒 + SIGTERM 优雅退出。
5. 校正构建：PGXS Makefile、control、sql 脚本。

## 遇到的问题与解决

- **问题**：后台 worker 在扩展表尚未创建时可能插入失败。  
  **解决**：worker 使用 SPI 独立执行，失败不会阻塞生产者，扩展创建后恢复正常写入。
- **问题**：队列在高并发下需要线程安全。  
  **解决**：使用共享内存自旋锁保护 head/tail/count。
- **问题**：仅审计指定表。  
  **解决**：`mini_audit.tables` 配置为逗号分隔 `schema.table`，入队前过滤。
- **问题**：worker 需要在事务里使用 SPI 批量写入。  
  **解决**：worker flush 阶段显式 `StartTransactionCommand/CommitTransactionCommand`，并在异常时回滚。
