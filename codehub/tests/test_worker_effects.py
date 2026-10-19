"""Failure semantics at the durable worker's outbound-service boundary."""

from types import SimpleNamespace
import threading
from uuid import uuid4

import httpx
import pytest

from codehub.search import SearchIndex
from codehub.worker import EffectProcessor, Worker


def test_unavailable_group_does_not_stop_other_group_outbox_publication():
    worker = object.__new__(Worker)
    worker.stop = threading.Event()
    worker.storage_override = None
    worker.queue = "work"
    published, confirmed = [], []

    def unavailable(**_kwargs):
        raise RuntimeError("database unavailable")

    worker.router = SimpleNamespace(
        groups=lambda: {
            "group-0": SimpleNamespace(pending=unavailable),
            "group-1": SimpleNamespace(pending=lambda **_kwargs: ["a" * 64], publish_confirmed=confirmed.append),
        }
    )
    channel = SimpleNamespace(confirm_delivery=lambda: None, basic_publish=lambda **kwargs: published.append(kwargs))
    connection = SimpleNamespace(
        is_open=True, process_data_events=lambda **_kwargs: worker.stop.set(), close=lambda: None
    )
    worker.broker = lambda: (connection, channel)
    worker.publish_loop()
    assert confirmed == ["a" * 64]
    assert len(published) == 1 and b'"group":"group-1"' in published[0]["body"]


class OutboxObservation:
    def __init__(self, effect):
        self.item = effect
        self.completed = []
        self.builds = []

    def effect(self, _effect_id):
        return self.item

    def complete(self, effect_id):
        self.completed.append(effect_id)

    def record_build(self, *args):
        self.builds.append(args)


def test_one_unavailable_search_replica_cannot_complete_work_and_retry_is_safe(tmp_path):
    effect_id = "a" * 64
    operation = {
        "event_id": str(uuid4()),
        "tenant_id": str(uuid4()),
        "entity_id": str(uuid4()),
        "project_id": str(uuid4()),
        "client_revision": 1,
        "kind": "issue.create",
        "payload": {"title": "Regional retries", "body": "Bound request concurrency", "state": "open"},
    }
    first = SearchIndex(tmp_path / "first.sqlite")
    second = SearchIndex(tmp_path / "second.sqlite")
    unavailable = True

    def route(request):
        nonlocal unavailable
        if request.url.host == "second" and unavailable:
            return httpx.Response(503)
        index = first if request.url.host == "first" else second
        import json

        body = json.loads(request.content)
        result = index.index(body["effect_id"], body["operation"])
        return httpx.Response(200, json=result)

    storage = OutboxObservation({"state": "pending", "effect_kind": "search", "payload": operation})
    settings = SimpleNamespace(search_urls=("http://first", "http://second"))
    with httpx.Client(transport=httpx.MockTransport(route)) as client:
        processor = EffectProcessor(storage, settings, client=client)
        with pytest.raises(httpx.HTTPStatusError):
            processor.process(effect_id)
        assert storage.completed == []
        assert first.document(operation["tenant_id"], operation["entity_id"])["revision"] == 1
        assert second.document(operation["tenant_id"], operation["entity_id"]) is None
        unavailable = False
        processor.process(effect_id)
    assert storage.completed == [effect_id]
    assert second.document(operation["tenant_id"], operation["entity_id"])["revision"] == 1
    with first.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM processed").fetchone()[0] == 1


def test_missing_or_unknown_work_does_not_report_completion():
    with httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(200))) as client:
        missing = OutboxObservation(None)
        from codehub.domain import DomainError

        with pytest.raises(DomainError):
            EffectProcessor(missing, SimpleNamespace(), client=client).process("b" * 64)
        assert missing.completed == []
        invalid = OutboxObservation({"state": "pending", "effect_kind": "unknown", "payload": {}})
        with pytest.raises(DomainError):
            EffectProcessor(invalid, SimpleNamespace(), client=client).process("c" * 64)
        assert invalid.completed == []


def test_partial_regional_artifact_copy_is_not_complete_and_retries_identical_content():
    import io
    import tarfile

    output = io.BytesIO()
    source = b"def healthy_regions(regions):\n    return sorted(regions)\n"
    with tarfile.open(fileobj=output, mode="w") as archive:
        member = tarfile.TarInfo("routing.py")
        member.size = len(source)
        archive.addfile(member, io.BytesIO(source))
    project, event = str(uuid4()), str(uuid4())
    item = {
        "event_id": event,
        "state": "pending",
        "effect_kind": "build",
        "payload": {"project_id": project, "commit_sha": "a" * 40},
    }
    storage = OutboxObservation(item)
    unavailable = True
    copied = {}

    def route(request):
        if request.method == "GET":
            return httpx.Response(200, content=output.getvalue())
        if request.url.host == "second" and unavailable:
            return httpx.Response(503)
        existing = copied.get(request.url.host)
        if existing is not None:
            assert existing == request.content
        copied[request.url.host] = request.content
        return httpx.Response(200)

    settings = SimpleNamespace(
        repository_url="http://repository", delivery_url="http://first", delivery_urls=("http://first", "http://second")
    )
    with httpx.Client(transport=httpx.MockTransport(route)) as client:
        processor = EffectProcessor(storage, settings, client=client)
        with pytest.raises(httpx.HTTPStatusError):
            processor.process("d" * 64)
        assert storage.completed == [] and storage.builds == []
        assert set(copied) == {"first"}
        unavailable = False
        processor.process("d" * 64)
    assert storage.completed == ["d" * 64]
    assert copied["first"] == copied["second"]
    assert len(storage.builds) == 1
