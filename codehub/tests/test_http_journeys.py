"""Authenticated HTTP/Git customer journey against an explicitly selected deployment."""

import base64
import hashlib
import io
import os
import secrets
import subprocess
import time
import zipfile
from uuid import uuid4

import httpx
import pytest

from codehub.config import secret

pytestmark = pytest.mark.integration


def test_customer_journey_over_actual_http_git_and_effects(tmp_path):
    base = os.environ.get("CODEHUB_TEST_API_URL")
    repository = os.environ.get("CODEHUB_TEST_REPOSITORY_URL")
    if not base or not repository:
        pytest.skip("Requires explicitly selected HTTP deployment and repository endpoint")
    timeout = int(os.environ.get("CODEHUB_TEST_EFFECT_TIMEOUT", "120"))
    tenant, project, issue, change, review = (str(uuid4()) for _ in range(5))
    owner, reviewer = str(uuid4()), str(uuid4())
    owner_token, reviewer_token = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    receipts = []
    client = httpx.Client(base_url=base.rstrip("/"), timeout=30)

    def mutation(entity_id, revision, kind, payload, token=owner_token, project_id=project):
        envelope = {
            "event_id": str(uuid4()),
            "tenant_id": tenant,
            "entity_id": entity_id,
            "project_id": project_id,
            "client_revision": revision,
            "kind": kind,
            "payload": payload,
        }
        response = client.post("/v1/operations", json=envelope, headers={"Authorization": f"Bearer {token}"})
        response.raise_for_status()
        assert response.json()["event_id"] == envelope["event_id"]
        receipts.append(envelope)
        return envelope

    for user_id, username, token in [
        (owner, f"morgan-{owner[:8]}", owner_token),
        (reviewer, f"jamie-{reviewer[:8]}", reviewer_token),
    ]:
        response = client.post(
            "/v1/identity/users",
            json={"user_id": user_id, "username": username, "api_token": token},
            headers={"Authorization": f"Bearer {secret('CODEHUB_BOOTSTRAP_TOKEN')}"},
        )
        response.raise_for_status()
    mutation(
        tenant, 1, "organization.create", {"slug": f"harbor-{tenant[:8]}", "name": "Harbor Systems"}, project_id=None
    )
    mutation(reviewer, 1, "membership.set", {"user_id": reviewer, "role": "developer"}, project_id=None)
    mutation(
        project,
        1,
        "project.create",
        {"slug": "request-router", "name": "Request Router", "default_ref": "refs/heads/main"},
    )
    mutation(
        issue,
        1,
        "issue.create",
        {"title": "Regional route selection", "body": "Prefer stable connection routing", "state": "open"},
    )
    mutation(str(uuid4()), 1, "comment.create", {"issue_id": issue, "body": "Add deterministic selection coverage"})

    checkout = tmp_path / "request-router"
    checkout.mkdir()
    auth = base64.b64encode(f"morgan:{owner_token}".encode()).decode()
    env = {
        **os.environ,
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "http.extraHeader",
        "GIT_CONFIG_VALUE_0": f"Authorization: Basic {auth}",
    }

    def git(*args):
        result = subprocess.run(
            ["git", "-c", "commit.gpgsign=false", "-C", str(checkout), *args], env=env, capture_output=True, timeout=30
        )
        if result.returncode:
            raise AssertionError(f"Git customer operation failed ({args[0]}, code {result.returncode})")
        return result.stdout.decode().strip()

    git("init", "-b", "main")
    git("config", "user.name", "Morgan Lee")
    git("config", "user.email", "morgan@example.test")
    code = checkout / "routing.py"
    code.write_text("def choose_region(regions):\n    return sorted(regions)[0]\n", encoding="utf-8")
    git("add", "routing.py")
    git("commit", "-m", "Introduce stable route selection")
    git("remote", "add", "origin", f"{repository.rstrip('/')}/git/{project}")
    git("push", "origin", "main")
    base_sha = git("rev-parse", "HEAD")
    main_record = str(uuid4())
    mutation(main_record, 1, "repository.push", {"ref": "refs/heads/main", "commit_sha": base_sha})
    git("checkout", "-b", "bounded-selection")
    code.write_text(
        "def choose_region(regions):\n    if not regions:\n        raise ValueError('No healthy region')\n    return sorted(regions)[0]\n",
        encoding="utf-8",
    )
    git("add", "routing.py")
    git("commit", "-m", "Reject empty region inventory")
    head_sha = git("rev-parse", "HEAD")
    git("push", "origin", "bounded-selection")
    mutation(str(uuid4()), 1, "repository.push", {"ref": "refs/heads/bounded-selection", "commit_sha": head_sha})
    details = {
        "title": "Handle empty regional inventory",
        "head_sha": head_sha,
        "base_sha": base_sha,
        "head_ref": "refs/heads/bounded-selection",
        "base_ref": "refs/heads/main",
        "state": "open",
    }
    mutation(change, 1, "change.create", details)
    mutation(
        review,
        1,
        "review.create",
        {
            "change_id": change,
            "head_sha": head_sha,
            "verdict": "approve",
            "body": "The empty-inventory case is explicit",
        },
        token=reviewer_token,
    )
    git("checkout", "main")
    git("merge", "--ff-only", "bounded-selection")
    git("push", "origin", "main")
    mutation(main_record, 2, "repository.push", {"ref": "refs/heads/main", "commit_sha": head_sha})
    mutation(change, 2, "change.update", {**details, "base_sha": head_sha, "state": "merged"})
    mutation(
        issue,
        2,
        "issue.update",
        {"title": "Regional route selection", "body": "Empty inventory is handled", "state": "closed"},
    )
    git("fsck", "--full")
    clone = tmp_path / "fresh-clone"
    result = subprocess.run(
        ["git", "clone", f"{repository.rstrip('/')}/git/{project}", str(clone)],
        env=env,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, "Fresh authenticated Git clone failed"
    assert (clone / "routing.py").read_bytes() == code.read_bytes()

    headers = {"Authorization": f"Bearer {owner_token}"}
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = client.get(f"/v1/projects/{project}/builds", headers=headers)
        response.raise_for_status()
        matching = [item for item in response.json()["items"] if item["commit_sha"] == head_sha]
        if matching:
            artifact = client.get(f"/v1/projects/{project}/artifacts/{matching[0]['artifact_sha256']}", headers=headers)
            artifact.raise_for_status()
            assert hashlib.sha256(artifact.content).hexdigest() == matching[0]["artifact_sha256"]
            with zipfile.ZipFile(io.BytesIO(artifact.content)) as bundle:
                assert bundle.read("routing.py") == code.read_bytes()
            search = client.get(f"/v1/tenants/{tenant}/search", params={"q": "Regional"}, headers=headers)
            search.raise_for_status()
            if any(item["id"] == issue and item["revision"] == 2 for item in search.json()["items"]):
                break
        time.sleep(1)
    else:
        raise AssertionError("Actual build/artifact/search effects did not complete in the configured interval")
    events = client.get(f"/v1/tenants/{tenant}/events", headers=headers)
    events.raise_for_status()
    observed = {item["event_id"] for item in events.json()["items"]}
    assert {item["event_id"] for item in receipts} <= observed
    client.close()
