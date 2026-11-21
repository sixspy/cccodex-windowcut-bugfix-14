"""事件入账：窗内 / 拒收 / 迟到桶分流。"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException

from auth import AuthHeader, require_user
from db import LATE_REASON, connect, insert_event, insert_late_pair, write_audit
from locks import release, require_lock
from models import EventIn

router = APIRouter(tags=["ingest"])


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


@router.post("/api/events")
def ingest_event(body: EventIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    lock_key = f"ingest:{body.window_name}"
    with connect() as conn:
        require_lock(conn, lock_key, user)
        try:
            w = conn.execute(
                "SELECT * FROM windows WHERE name=?", (body.window_name,)
            ).fetchone()
            if not w:
                raise HTTPException(404, "窗口不存在")
            ts = body.event_ts
            created = _now()
            if w["status"] == "已关闭":
                # 与种子装载同源（db.insert_late_pair，甲路径同事务双写）：
                # events + late_bucket 同一事务，桶失败则事件一并回滚。
                eid = insert_late_pair(
                    conn,
                    window_name=body.window_name,
                    biz_key=body.biz_key,
                    event_ts=ts,
                    amount=body.amount,
                    created_at=created,
                    reason=LATE_REASON,
                )
                write_audit(
                    conn,
                    user,
                    "ingest_late",
                    body.biz_key,
                    {"window": body.window_name},
                )
                conn.commit()
                return {"disposition": "迟到桶", "event_id": eid}

            if not (w["open_at"] <= ts <= w["close_at"]):
                insert_event(
                    conn,
                    window_id=None,
                    biz_key=body.biz_key,
                    event_ts=ts,
                    amount=body.amount,
                    disposition="拒收",
                    created_at=created,
                )
                conn.execute(
                    """INSERT INTO reject_slips(window_name,biz_key,event_ts,amount,reason,created_at)
                       VALUES (?,?,?,?,?,?)""",
                    (
                        body.window_name,
                        body.biz_key,
                        ts,
                        body.amount,
                        "窗外且窗仍开启",
                        created,
                    ),
                )
                write_audit(
                    conn,
                    user,
                    "ingest_reject",
                    body.biz_key,
                    {"window": body.window_name},
                )
                conn.commit()
                raise HTTPException(409, "事件时间落在窗外且窗仍开启：拒收")

            eid = insert_event(
                conn,
                window_id=w["id"],
                biz_key=body.biz_key,
                event_ts=ts,
                amount=body.amount,
                disposition="窗内",
                created_at=created,
            )
            write_audit(
                conn,
                user,
                "ingest_in_window",
                body.biz_key,
                {"window_id": w["id"]},
            )
            conn.commit()
            return {"disposition": "窗内", "window_id": w["id"], "event_id": eid}
        finally:
            release(conn, lock_key, user)
            conn.commit()


@router.get("/api/events")
def list_events(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM events ORDER BY id DESC LIMIT 200"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.get("/api/rejects")
def list_rejects(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM reject_slips ORDER BY id DESC LIMIT 100"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}
