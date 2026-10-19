"""Normal topology promotion policy and persistence failure checks."""

import pymysql
import pytest
from codehub import topology


class Cursor:
    def __init__(self, host, servers, statements):
        self.host, self.servers, self.statements = host, servers, statements
        self.query = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def execute(self, query):
        self.query = query
        self.statements.append((self.host, query))

    def fetchone(self):
        if self.query == "SHOW REPLICA STATUS":
            return {"Seconds_Behind_Source": self.servers["lag"], "Last_SQL_Error": self.servers["error"]}
        return {"server_uuid": self.host, "read_only": self.host == "candidate"}


class Connection:
    def __init__(self, host, servers, statements):
        self.host, self.servers, self.statements = host, servers, statements

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        pass

    def cursor(self):
        return Cursor(self.host, self.servers, self.statements)


@pytest.fixture
def manager(tmp_path, monkeypatch):
    monkeypatch.setenv("CODEHUB_DATA_DIR", str(tmp_path))
    clock = [100.0]
    monkeypatch.setattr(topology.time, "monotonic", lambda: clock[0])
    servers = {"writer": True, "candidate": True, "lag": 0, "error": ""}
    statements, routes = [], []

    def connect(host):
        if not servers[host]:
            raise pymysql.OperationalError(2003, "Peer not reachable")
        return Connection(host, servers, statements)

    configuration = {
        "writer_host": "writer",
        "candidate_host": "candidate",
        "routing_service": "mysql-writer",
        "automatic_promotion": True,
        "failure_threshold": 3,
        "maximum_lag_seconds": 10,
        "probe_interval_seconds": 1,
    }
    instance = topology.TopologyManager(
        configuration, connect=connect, route=lambda host, service: routes.append((host, service))
    )
    return instance, servers, statements, routes, clock


def fail_probes(manager, number=3):
    instance, servers, _statements, _routes, clock = manager
    instance.probe_once()
    servers["writer"] = False
    servers["lag"] = None
    for _ in range(number):
        clock[0] += 1
        instance.probe_once()


def test_real_probe_threshold_and_recent_safe_lag_gate_promotion(manager):
    instance, servers, statements, routes, clock = manager
    instance.probe_once()
    servers["writer"] = False
    servers["lag"] = None
    for _ in range(2):
        clock[0] += 1
        instance.probe_once()
        assert not instance.state["promoted"]
    clock[0] += 1
    instance.probe_once()
    assert instance.state["promoted"]
    assert routes == [("candidate", "mysql-writer")]
    assert ("candidate", "STOP REPLICA") in statements
    assert ("candidate", "SET PERSIST super_read_only=OFF") in statements
    assert ("candidate", "SET PERSIST read_only=OFF") in statements
    assert ("candidate", "SET PERSIST_ONLY skip_replica_start=ON") in statements


@pytest.mark.parametrize("reason", ["stale", "lagging", "sql_error", "candidate_unreachable"])
def test_unsafe_candidate_cannot_be_automatically_promoted(manager, reason):
    instance, servers, _statements, routes, clock = manager
    instance.probe_once()
    servers["writer"] = False
    if reason == "stale":
        servers["lag"] = None
        clock[0] += 11
    elif reason == "lagging":
        servers["lag"] = 11
    elif reason == "sql_error":
        servers["error"] = "Transaction could not be replayed"
    elif reason == "candidate_unreachable":
        servers["candidate"] = False
    for _ in range(3):
        instance.probe_once()
    assert not instance.state["promoted"]
    assert routes == []


def test_route_patch_failure_does_not_record_completed_promotion(manager):
    instance, servers, _statements, _routes, clock = manager
    instance.probe_once()
    servers["writer"] = False
    servers["lag"] = None

    def rejected(_host, _service):
        raise OSError("Routing API unavailable")

    instance._route = rejected
    for _ in range(2):
        clock[0] += 1
        instance.probe_once()
    with pytest.raises(OSError):
        instance.probe_once()
    assert not instance.state["promoted"]
    reloaded = topology.TopologyManager(instance.configuration, connect=instance._connect, route=rejected)
    assert not reloaded.state["promoted"]


def test_completed_promotion_survives_restart_without_automatic_failback(manager):
    instance, servers, _statements, routes, _clock = manager
    fail_probes(manager)
    assert instance.state["promoted"]
    restarted = topology.TopologyManager(
        instance.configuration, connect=instance._connect, route=lambda host, service: routes.append((host, service))
    )
    servers["writer"] = True
    restarted.probe_once()
    assert restarted.state["promoted"]
    assert restarted.state["writer_host"] == "candidate"
    assert routes == [("candidate", "mysql-writer")]


def test_boot_unavailability_cannot_promote_before_writer_was_observed_healthy(manager):
    instance, servers, statements, routes, _clock = manager
    servers["writer"] = False
    for _ in range(5):
        instance.probe_once()
    assert instance.state["consecutive_failures"] == 5
    assert not instance.state["writer_observed_healthy"]
    assert not instance.state["promoted"] and routes == []
    assert not any(query == "STOP REPLICA" for _host, query in statements)
    servers["writer"] = True
    instance.probe_once()
    assert instance.state["writer_observed_healthy"]
    servers["writer"] = False
    for _ in range(3):
        instance.probe_once()
    assert instance.state["promoted"] and routes == [("candidate", "mysql-writer")]


def test_persisted_healthy_observation_applies_only_to_the_configured_writer(manager):
    instance, servers, _statements, routes, _clock = manager
    instance.probe_once()
    assert instance.state["writer_observed_healthy"]
    servers["new-writer"] = False
    restarted = topology.TopologyManager(
        instance.configuration | {"writer_host": "new-writer"},
        connect=instance._connect,
        route=lambda host, service: routes.append((host, service)),
    )
    for _ in range(5):
        restarted.probe_once()
    assert not restarted.state["writer_observed_healthy"]
    assert not restarted.state["promoted"] and routes == []
