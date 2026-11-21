"""SQLite 连接、建表、种子。业务窗关账母仓公共库层。"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FIXTURES = ROOT / "fixtures"
FRONTEND = ROOT / "frontend"

# 种子同源册（docs/seed-homology-registry.md）锁定的甲路径常量。
# 重种子与冷启动种子共用同一把锁，两人同时触发时后到只记争用。
SEED_LOCK_KEY = "seed:reseed"
# 与运行态 ingest_svc 迟到分流完全同态的处置原因
LATE_REASON = "关窗后到达"
# 甲路径：迟到桶处置标识，种子与运行态共用
LATE_DISPOSITION = "迟到桶"

PRODUCT_META = {
    "modules": [
        "windows",
        "ingest",
        "late_bucket",
        "materialize",
        "reconcile",
        "locks",
        "audit",
        "hold_staging",
        "reopen",
    ],
    "stubs": ["hold_release", "late_promote", "partition_mat", "reopen"],
}


def connect() -> sqlite3.Connection:
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    # WAL：种子锁等已提交行须对其它连接立即可见（双人重种子争用判定依赖）
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def sha_password(password: str) -> str:
    import hashlib

    return hashlib.sha256(password.encode("utf-8")).hexdigest()


SCHEMA = """
CREATE TABLE IF NOT EXISTS users(
  username TEXT PRIMARY KEY,
  password_sha TEXT NOT NULL,
  role TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS windows(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE NOT NULL,
  open_at TEXT NOT NULL,
  close_at TEXT NOT NULL,
  status TEXT NOT NULL,
  generation INTEGER NOT NULL DEFAULT 1,
  seal_token TEXT
);
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  window_id INTEGER,
  biz_key TEXT NOT NULL,
  event_ts TEXT NOT NULL,
  amount REAL NOT NULL,
  disposition TEXT NOT NULL,
  hold_id INTEGER,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS late_bucket(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL,
  window_name TEXT,
  biz_key TEXT NOT NULL,
  event_ts TEXT NOT NULL,
  amount REAL NOT NULL,
  reason TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS materializations(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  window_id INTEGER NOT NULL,
  status TEXT NOT NULL,
  row_count INTEGER NOT NULL,
  total_amount REAL NOT NULL,
  partition_key TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS hold_staging(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  window_id INTEGER,
  biz_key TEXT NOT NULL,
  event_ts TEXT NOT NULL,
  amount REAL NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS window_locks(
  lock_key TEXT PRIMARY KEY,
  holder TEXT NOT NULL,
  acquired_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS contention_log(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  lock_key TEXT NOT NULL,
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  detail TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_trail(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  target TEXT,
  payload TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reopen_ledger(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  window_id INTEGER NOT NULL,
  from_generation INTEGER NOT NULL,
  to_generation INTEGER NOT NULL,
  mode TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reject_slips(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  window_name TEXT,
  biz_key TEXT NOT NULL,
  event_ts TEXT NOT NULL,
  amount REAL NOT NULL,
  reason TEXT NOT NULL,
  created_at TEXT NOT NULL
);
"""


def ensure_users(conn: sqlite3.Connection) -> None:
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute(
            "INSERT INTO users VALUES (?,?,?)",
            ("admin", sha_password("123456"), "管理员"),
        )
        conn.execute(
            "INSERT INTO users VALUES (?,?,?)",
            ("viewer", sha_password("view123"), "只读"),
        )
        conn.execute(
            "INSERT INTO users VALUES (?,?,?)",
            ("closer", sha_password("close123"), "关账员"),
        )


def _load_fixture(name: str) -> list | dict | None:
    path = FIXTURES / name
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


class SeedHomologyError(RuntimeError):
    """种子偏离同源册：events 与 late_bucket 不同态，整段种子必须回滚。"""


def insert_event(
    conn: sqlite3.Connection,
    *,
    window_id: int | None,
    biz_key: str,
    event_ts: str,
    amount: float,
    disposition: str,
    created_at: str,
) -> int:
    """events 单行写入；种子与运行态共用，保证表同源。"""
    cur = conn.execute(
        "INSERT INTO events(window_id,biz_key,event_ts,amount,disposition,created_at) VALUES (?,?,?,?,?,?)",
        (window_id, biz_key, event_ts, amount, disposition, created_at),
    )
    return cur.lastrowid


def insert_late_bucket_row(
    conn: sqlite3.Connection,
    *,
    event_id: int,
    window_name: str | None,
    biz_key: str,
    event_ts: str,
    amount: float,
    reason: str,
    created_at: str,
) -> None:
    """late_bucket 单行写入；种子与运行态共用，保证表同源。"""
    conn.execute(
        """INSERT INTO late_bucket(event_id,window_name,biz_key,event_ts,amount,reason,created_at)
           VALUES (?,?,?,?,?,?,?)""",
        (event_id, window_name, biz_key, event_ts, amount, reason, created_at),
    )


def insert_late_pair(
    conn: sqlite3.Connection,
    *,
    window_name: str | None,
    biz_key: str,
    event_ts: str,
    amount: float,
    created_at: str,
    reason: str = LATE_REASON,
) -> int:
    """甲路径同源双写（种子同源册锁定）：同一事务内先 events 后 late_bucket。

    桶插入失败时异常向上抛，由外层事务整体回滚——事件与桶同生共灭，
    正式表零增，不留孤儿 events 行。返回同源事件 id。
    """
    event_id = insert_event(
        conn,
        window_id=None,
        biz_key=biz_key,
        event_ts=event_ts,
        amount=amount,
        disposition=LATE_DISPOSITION,
        created_at=created_at,
    )
    insert_late_bucket_row(
        conn,
        event_id=event_id,
        window_name=window_name,
        biz_key=biz_key,
        event_ts=event_ts,
        amount=amount,
        reason=reason,
        created_at=created_at,
    )
    return event_id


def _seed_window_name(conn: sqlite3.Connection, e: dict) -> str | None:
    """种子迟到行归属窗口：优先夹具显式字段，否则按事件时间落窗同源推导。"""
    explicit = e.get("window_name")
    if explicit:
        return explicit
    row = conn.execute(
        """SELECT name FROM windows
           WHERE ? BETWEEN open_at AND close_at ORDER BY id LIMIT 1""",
        (e["event_ts"],),
    ).fetchone()
    return row["name"] if row else None


def _assert_seed_homology(conn: sqlite3.Connection) -> None:
    """提交前同源校验：每个迟到桶事件必须有同态桶行，反之不得有孤儿桶行。"""
    orphan_events = conn.execute(
        """SELECT e.id FROM events e
           LEFT JOIN late_bucket b ON b.event_id = e.id
           WHERE e.disposition = ? AND b.id IS NULL""",
        (LATE_DISPOSITION,),
    ).fetchall()
    orphan_buckets = conn.execute(
        """SELECT b.id FROM late_bucket b
           LEFT JOIN events e ON e.id = b.event_id
           WHERE e.id IS NULL"""
    ).fetchall()
    if orphan_events or orphan_buckets:
        raise SeedHomologyError(
            "种子偏了：events/late_bucket 不同态 "
            f"orphan_events={[r['id'] for r in orphan_events]} "
            f"orphan_buckets={[r['id'] for r in orphan_buckets]}"
        )


def seed_if_empty(conn: sqlite3.Connection) -> None:
    if conn.execute("SELECT COUNT(*) c FROM windows").fetchone()["c"] > 0:
        return
    windows = _load_fixture("windows.json") or [
        {
            "name": "W-1101",
            "open_at": "2026-11-01T00:00:00",
            "close_at": "2026-11-01T23:59:59",
            "status": "已关闭",
            "generation": 1,
        },
        {
            "name": "W-1102",
            "open_at": "2026-11-02T00:00:00",
            "close_at": "2026-11-02T23:59:59",
            "status": "开启",
            "generation": 1,
        },
        {
            "name": "W-1103",
            "open_at": "2026-11-03T00:00:00",
            "close_at": "2026-11-03T23:59:59",
            "status": "开启",
            "generation": 1,
        },
    ]
    for w in windows:
        conn.execute(
            "INSERT INTO windows(name,open_at,close_at,status,generation) VALUES (?,?,?,?,?)",
            (
                w["name"],
                w["open_at"],
                w["close_at"],
                w["status"],
                int(w.get("generation", 1)),
            ),
        )
    events = _load_fixture("events.json") or [
        {
            "window_id": 1,
            "biz_key": "ORD-1",
            "event_ts": "2026-11-01T10:00:00",
            "amount": 12.5,
            "disposition": "窗内",
            "created_at": "2026-11-01T10:00:01",
        },
        {
            "window_id": None,
            "biz_key": "ORD-late",
            "event_ts": "2026-11-01T23:50:00",
            "amount": 3.0,
            "disposition": "迟到桶",
            "created_at": "2026-11-02T00:01:00",
        },
    ]
    late_pairs = 0
    for e in events:
        if e["disposition"] == LATE_DISPOSITION:
            # 甲路径：与运行态 ingest_svc 迟到分流同源的同事务双写。
            # 桶写失败则异常上抛，本事务（events+桶+其余种子）整体回滚。
            insert_late_pair(
                conn,
                window_name=_seed_window_name(conn, e),
                biz_key=e["biz_key"],
                event_ts=e["event_ts"],
                amount=e["amount"],
                created_at=e.get("created_at", e["event_ts"]),
            )
            late_pairs += 1
            continue
        insert_event(
            conn,
            window_id=e.get("window_id"),
            biz_key=e["biz_key"],
            event_ts=e["event_ts"],
            amount=e["amount"],
            disposition=e["disposition"],
            created_at=e.get("created_at", e["event_ts"]),
        )
    # 提交前同源校验：偏轨即抛 SeedHomologyError，由 init_db/reseed 回滚并留痕
    _assert_seed_homology(conn)
    conn.execute(
        "INSERT INTO materializations(window_id,status,row_count,total_amount,partition_key,created_at) VALUES (?,?,?,?,?,?)",
        (1, "成功", 1, 12.5, "W-1101-g1", "2026-11-02T00:10:00"),
    )
    conn.execute(
        "INSERT INTO audit_trail(actor,action,target,payload,created_at) VALUES (?,?,?,?,?)",
        ("system", "seed", "windows", json.dumps({"count": len(windows)}), "2026-11-01T00:00:00"),
    )
    if late_pairs:
        conn.execute(
            "INSERT INTO audit_trail(actor,action,target,payload,created_at) VALUES (?,?,?,?,?)",
            (
                "system",
                "seed_late",
                "late_bucket",
                json.dumps({"pairs": late_pairs, "path": "甲-同事务双写"}, ensure_ascii=False),
                "2026-11-01T00:00:00",
            ),
        )


# 重种子时清空的业务表（users / window_locks / contention_log / audit_trail 保留）
_RESEED_WIPE_TABLES = (
    "reopen_ledger",
    "materializations",
    "hold_staging",
    "reject_slips",
    "late_bucket",
    "events",
    "windows",
)


def _record_seed_deviation(exc: Exception) -> None:
    """偏轨留痕：以独立事务写 audit_trail（种子主事务已回滚），再向上抛。"""
    try:
        with connect() as audit_conn:
            write_audit(
                audit_conn,
                "system",
                "seed_deviation",
                "late_bucket",
                {"error": str(exc), "registry": "docs/seed-homology-registry.md"},
            )
            audit_conn.commit()
    except Exception:  # 留痕本身失败不得掩盖原始缺陷
        pass


def init_db() -> None:
    """冷启动：建表 + 保底用户 + 空库种子。种子失败整体回滚并记偏轨。"""
    with connect() as conn:
        conn.executescript(SCHEMA)
        ensure_users(conn)
        try:
            seed_if_empty(conn)
            conn.commit()
        except Exception as exc:  # 同源册：偏离即回滚，正式零增并留痕
            conn.rollback()
            _record_seed_deviation(exc)
            raise


def reseed_database(actor: str) -> dict:
    """重种子：占种子锁 -> 清业务表 -> 按甲路径同源重灌 -> 校验提交 -> 放锁。

    两人同时触发时后到者拿不到锁：不写任何业务，只记 contention_log 与
    audit_trail(reseed_contention) 并抛 409。
    """
    from fastapi import HTTPException

    from locks import release, try_acquire

    with connect() as conn:
        if not try_acquire(conn, SEED_LOCK_KEY, actor):
            write_audit(
                conn,
                actor,
                "reseed_contention",
                SEED_LOCK_KEY,
                {"result": "后到只记争用，业务零增"},
            )
            conn.commit()
            raise HTTPException(409, "争用：重种子锁被占用，后到只记争用，不写业务")
        try:
            for table in _RESEED_WIPE_TABLES:
                conn.execute(f"DELETE FROM {table}")
            seed_if_empty(conn)
            write_audit(
                conn,
                actor,
                "reseed",
                "windows",
                {"path": "甲-同事务双写", "registry": "docs/seed-homology-registry.md"},
            )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            _record_seed_deviation(exc)
            raise
        finally:
            release(conn, SEED_LOCK_KEY, actor)
            conn.commit()
    return {"ok": True, "path": "甲-同事务双写", "lock_key": SEED_LOCK_KEY}


def write_audit(
    conn: sqlite3.Connection,
    actor: str,
    action: str,
    target: str | None = None,
    payload: dict | None = None,
    created_at: str | None = None,
) -> None:
    from datetime import datetime

    now = created_at or datetime.now().isoformat(timespec="seconds")
    conn.execute(
        "INSERT INTO audit_trail(actor,action,target,payload,created_at) VALUES (?,?,?,?,?)",
        (actor, action, target, json.dumps(payload or {}, ensure_ascii=False), now),
    )
