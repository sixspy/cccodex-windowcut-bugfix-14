# 种子同源册（SEED HOMOLOGY REGISTRY）

> 本册为关账系统种子装载正确性口径的唯一登记处。**当日锁定，禁止改口**；
> 偏离本册的实现一律拒写，并在 `audit_trail` 记 `seed_deviation`（种子偏了）。
> 只改本册或只改种子说明而不动装载代码，判失败。

## 决策记录

| 项 | 内容 |
|----|------|
| 缺陷编号 | BUGFIX-14：冷启动后迟到样例只进 `events`，未进 `late_bucket`，迟到明细页空 |
| 决策日期 | 2026-11-21（当日有效，禁止改口） |
| 择定路径 | **甲路径：种子同事务双写** |
| 否决路径 | 乙路径（seed_shadow 影子校验再原子晋升）——本次不采用，禁止两条路径混用 |
| 适用范围 | `db.py` 种子装载、`late_svc.py`、`ingest_svc.py` 及对应测试 |

## 甲路径口径（唯一正确性口径）

1. **同源**：种子装载与运行态入账（`ingest_svc.ingest_event` 的迟到分支）
   走同一套落库动作——同一 `events` 插入与同一 `late_bucket` 插入，
   由 `db.py` 的共享写入函数承载，禁止种子另搞一套只写 `events` 的旁路。
2. **同事务双写**：`disposition='迟到桶'` 的种子事件，必须在**同一事务**内
   先插 `events` 拿到 `event_id`，再插 `late_bucket`；`late_bucket` 插入失败时
   **事件与桶整体回滚**，正式表零增，不得留下孤儿 `events` 行。
3. **同态字段**：桶行 `biz_key / event_ts / amount / reason / created_at`
   与运行态迟到分流完全一致，`reason` 统一为 `关窗后到达`，`event_id`
   回指同源事件。
4. **五处同态**：`fixtures`（源）、`events`（事件行）、`late_bucket`（桶行）、
   迟到页（`/api/late-bucket` 与汇总）、审计（`seed_late` 留痕）数据对得上。
5. **种子锁**：重种子（reseed）必须先占种子锁 `seed:reseed`（复用
   `window_locks` + `locks.try_acquire`）。两人同时触发时，后到者不得写业务，
   只在 `contention_log` 记争用、在 `audit_trail` 记 `reseed_contention` 并返回 409；
   锁释放后允许重新重种子。
6. **偏轨保护**：种子提交前做同源校验（每个 `迟到桶` 事件必须存在对应
   `late_bucket` 行且键/金额一致）。校验不过即抛错回滚，并由外层以独立事务
   记 `seed_deviation`（种子偏了）后再向上抛。

## 禁止事项

- 禁止改用乙路径（`seed_shadow` 表 / 影子晋升）；本仓不新增影子表。
- 禁止甲乙混用：不得同事务双写一部分、影子晋升另一部分。
- 禁止只改本说明或种子 JSON 而不修装载代码。
- 禁止在无失败测试（红）的情况下改 `db.py` / `late_svc.py` / `ingest_svc.py`。
- 库文件（`data/app.db`）不换、登录（`auth.py`）不动、前端页面沿用。
