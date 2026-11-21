"""BUGFIX-14 种子同源回归：冷启动迟到可见 / 单边失败回滚 / 重种子锁争用。

红测先行：本文件在修复前必须全部失败（或报错），修复后全绿。
- test_cold_start_late_visible：冷启动后 fixtures 中 disposition=迟到桶 的
  样例必须与运行态同源进 late_bucket，迟到页/汇总/看板/审计五处同态。
- test_seed_single_side_failure_rolls_back：种子迟到事件写桶单边失败时，
  事件与桶在同事务内一起回滚，正式表零增。
- test_reseed_lock_contention：两人同时触发重种子须占种子锁，后到只记争用
  （contention_log + audit_trail），不写业务；锁释放后可重种子成功。
"""
from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

from app import app
from db import connect, init_db

try:  # 修复前该常量尚不存在，导入失败也应让逐条测试显红而非收集失败
    from db import SEED_LOCK_KEY
except ImportError:  # pragma: no cover - 修复前分支
    SEED_LOCK_KEY = None


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setattr("db.DB_PATH", tmp_path / "seed.db")
    init_db()
    c = TestClient(app)
    tok = c.post(
        "/api/login", json={"username": "admin", "password": "123456"}
    ).json()["token"]
    c.headers.update({"Authorization": "Bearer " + tok})
    return c


def _second_user_headers() -> dict:
    """第二位操作员（closer），用于模拟两人同时触发重种子。"""
    c = TestClient(app)
    tok = c.post(
        "/api/login", json={"username": "closer", "password": "close123"}
    ).json()["token"]
    return {"Authorization": "Bearer " + tok}


def test_cold_start_late_visible(client):
    # 1. 迟到页（明细）：fixtures/events.json 的 ORD-late 必须出现
    late = client.get("/api/late-bucket").json()
    assert late["count"] == 1
    row = late["items"][0]
    assert row["biz_key"] == "ORD-late"
    assert row["amount"] == 3.0
    assert row["reason"] == "关窗后到达"

    # 2. events 行与桶行同源：event_id 指向同一事件
    events = client.get("/api/events").json()["items"]
    seed_late_events = [e for e in events if e["disposition"] == "迟到桶"]
    assert len(seed_late_events) == 1
    assert row["event_id"] == seed_late_events[0]["id"]
    assert row["biz_key"] == seed_late_events[0]["biz_key"]

    # 3. 迟到页汇总与看板同态
    summary = client.get("/api/late-bucket/summary").json()
    assert summary["count"] == 1
    assert summary["amount"] == 3.0
    dash = client.get("/api/dashboard").json()
    assert dash["late_bucket"] == 1

    # 4. 审计同态：种子迟到双写须留痕
    actions = client.get("/api/audit/actions").json()["items"]
    assert any(a["action"] == "seed_late" for a in actions)


class _FailLateBucketConn(sqlite3.Connection):
    """连接级注入：凡写 late_bucket 即失败，模拟种子双写单边失败。"""

    def execute(self, sql, params=()):
        if isinstance(sql, str) and "INSERT INTO late_bucket" in sql:
            self._fail_count = getattr(self, "_fail_count", 0) + 1
            raise sqlite3.OperationalError("injected late_bucket write failure")
        return super().execute(sql, params)


def test_seed_single_side_failure_rolls_back(tmp_path, monkeypatch):
    db_file = tmp_path / "rollback.db"
    monkeypatch.setattr("db.DB_PATH", db_file)

    factory = sqlite3.connect
    conns: list[_FailLateBucketConn] = []

    def failing_connect(path, *args, **kwargs):
        conn = factory(path, *args, factory=_FailLateBucketConn, **kwargs)
        conns.append(conn)
        return conn

    monkeypatch.setattr("db.sqlite3.connect", failing_connect)
    with pytest.raises(sqlite3.OperationalError, match="late_bucket"):
        init_db()

    assert sum(getattr(c, "_fail_count", 0) for c in conns) >= 1
    # 同事务回滚：正式表零增（events 迟到行不得孤儿残留，桶也为空）
    conn = sqlite3.connect(db_file)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM events WHERE disposition='迟到桶'"
        ).fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM late_bucket").fetchone()[0] == 0
        # 同事务内的其它种子写入同样回滚，保证整段种子原子性
        assert conn.execute("SELECT COUNT(*) FROM windows").fetchone()[0] == 0
    finally:
        conn.close()


def test_reseed_lock_contention(client, monkeypatch):
    # 第一人（admin）占住种子锁（模拟两人几乎同时触发重种子）
    with connect() as holder:
        holder.execute(
            "INSERT INTO window_locks(lock_key,holder,acquired_at) VALUES (?,?,?)",
            (SEED_LOCK_KEY, "admin", "2026-11-21T00:00:00"),
        )
        holder.commit()

        # 第二人（closer）触发重种子：409 争用，只记争用不写业务
        r = client.post("/api/admin/reseed", headers=_second_user_headers())
        assert r.status_code == 409
        payload = r.json()
        assert "争用" in payload["detail"]

    # 争用已登记：contention_log 与审计两处留痕
    contention = client.get("/api/contention").json()["items"]
    assert any(x["lock_key"] == SEED_LOCK_KEY for x in contention)
    actions = client.get("/api/audit/actions").json()["items"]
    assert any(a["action"] == "reseed_contention" for a in actions)

    # 争用期间正式数据零变化（仍是冷启动那份种子，迟到仍可见）
    assert client.get("/api/late-bucket").json()["count"] == 1

    # 锁释放后重种子成功：数据被清空再按同源路径重灌
    ok = client.post("/api/admin/reseed")
    assert ok.status_code == 200
    assert ok.json()["ok"] is True
    late = client.get("/api/late-bucket").json()
    assert late["count"] == 1
    assert late["items"][0]["biz_key"] == "ORD-late"
    actions2 = client.get("/api/audit/actions").json()["items"]
    assert any(a["action"] == "reseed" for a in actions2)
