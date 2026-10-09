import json
from uuid import uuid4

import httpx
import pytest

from codehub.delivery import DeliveryStore
from codehub.domain import Conflict, DomainError
from codehub.search import SearchIndex


def op(revision, title):
    return {
        "event_id": str(uuid4()),
        "tenant_id": str(uuid4()),
        "entity_id": str(uuid4()),
        "project_id": str(uuid4()),
        "client_revision": revision,
        "kind": "issue.update",
        "payload": {"title": title, "state": "open", "body": "Connection lifecycle"},
    }


def test_search_survives_restart_and_out_of_order_work(tmp_path):
    index = SearchIndex(tmp_path / "search.sqlite")
    latest = op(3, "Regional connection pool")
    earlier = {
        **latest,
        "event_id": str(uuid4()),
        "client_revision": 2,
        "payload": {"title": "Legacy routing", "state": "open", "body": "Connection lifecycle"},
    }
    index.index("a" * 64, latest)
    index.index("b" * 64, earlier)
    restarted = SearchIndex(tmp_path / "search.sqlite")
    assert restarted.search(latest["tenant_id"], "Regional")[0]["revision"] == 3
    assert restarted.search(latest["tenant_id"], "Legacy") == []
    assert restarted.search(str(uuid4()), "Regional") == []
    assert restarted.index("a" * 64, latest)["effect_id"] == "a" * 64
    with pytest.raises(Conflict):
        restarted.index("a" * 64, earlier)


def test_deliveries_persist_dedupe_and_send_actual_content(tmp_path, monkeypatch):
    monkeypatch.setenv("CODEHUB_WEBHOOK_HOSTS", "notifications.example")
    event_id = str(uuid4())
    payload = op(1, "Cache lifetime")
    calls = []

    def send(url, **kwargs):
        calls.append((url, kwargs))
        return httpx.Response(202, content=b"accepted", request=httpx.Request("POST", url))

    path = tmp_path / "deliveries.sqlite"
    first = DeliveryStore(path).deliver(
        "c" * 64, event_id, "https://notifications.example/events", payload, sender=send
    )
    second = DeliveryStore(path).deliver(
        "c" * 64, event_id, "https://notifications.example/events", payload, sender=send
    )
    assert len(calls) == 1
    assert first["state"] == second["state"] == "delivered"
    assert calls[0][1]["headers"]["Idempotency-Key"] == "c" * 64
    assert json.loads(calls[0][1]["content"])["operation"] == payload
    with pytest.raises(Conflict):
        DeliveryStore(path).deliver(
            "c" * 64, event_id, "https://notifications.example/events", op(1, "Other"), sender=send
        )
    with pytest.raises(DomainError):
        DeliveryStore(path).deliver("d" * 64, event_id, "http://unlisted.example/events", payload, sender=send)


def test_failed_delivery_is_retained_and_retry_uses_same_identity(tmp_path, monkeypatch):
    monkeypatch.setenv("CODEHUB_WEBHOOK_HOSTS", "notifications.example")
    identities = []

    def unavailable(url, **kwargs):
        identities.append(kwargs["headers"]["Idempotency-Key"])
        raise httpx.ConnectError("offline")

    def recovered(url, **kwargs):
        identities.append(kwargs["headers"]["Idempotency-Key"])
        return httpx.Response(200, request=httpx.Request("POST", url))

    store = DeliveryStore(tmp_path / "deliveries.sqlite")
    event_id, payload = str(uuid4()), op(1, "Recovery")
    with pytest.raises(httpx.ConnectError):
        store.deliver("e" * 64, event_id, "https://notifications.example/events", payload, sender=unavailable)
    result = store.deliver("e" * 64, event_id, "https://notifications.example/events", payload, sender=recovered)
    assert result["attempts"] == 2
    assert identities == ["e" * 64, "e" * 64]


def test_legacy_fts_migration_preserves_effect_dedupe_and_updates_one_indexed_row(tmp_path):
    path = tmp_path / "search.sqlite"
    index = SearchIndex(path)
    first = op(1, "Legacy routing")
    index.index("a" * 64, first)
    with index.connect() as db:
        db.execute("UPDATE documents_fts SET rowid=999")
        db.execute("PRAGMA user_version=0")
    index = SearchIndex(path)
    with index.connect() as db:
        assert (
            db.execute("SELECT d.rowid=f.rowid FROM documents d JOIN documents_fts f USING(tenant_id,id)").fetchone()[0]
            == 1
        )
    assert index.index("a" * 64, first)["effect_id"] == "a" * 64
    latest = {
        **first,
        "event_id": str(uuid4()),
        "client_revision": 2,
        "payload": {**first["payload"], "title": "Regional routing"},
    }
    index.index("b" * 64, latest)
    restarted = SearchIndex(path)
    assert restarted.search(first["tenant_id"], "Legacy") == []
    assert restarted.search(first["tenant_id"], "Regional")[0]["revision"] == 2
    with restarted.connect() as db:
        assert db.execute("SELECT COUNT(*) FROM documents_fts").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM processed").fetchone()[0] == 2


def test_fts_rowid_delete_vm_work_stays_bounded_as_index_grows(tmp_path):
    index = SearchIndex(tmp_path / "search.sqlite")
    work = []
    with index.connect() as db:
        for total in (200, 2000):
            prior = db.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
            for number in range(prior + 1, total + 1):
                db.execute(
                    "INSERT INTO documents VALUES(?,?,?,?,?,?)", ("tenant", str(number), "project", 1, "issue", "{}")
                )
                db.execute(
                    "INSERT INTO documents_fts(rowid,tenant_id,id,text) VALUES(?,?,?,?)",
                    (number, "tenant", str(number), "routing"),
                )
            steps = [0]

            def progress():
                steps[0] += 1
                return 0

            db.set_progress_handler(progress, 1)
            db.execute("DELETE FROM documents_fts WHERE rowid=?", (total,))
            db.set_progress_handler(None, 0)
            work.append(steps[0])
    assert work[1] < 3 * work[0], work
