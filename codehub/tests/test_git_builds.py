import hashlib
import io
import json
import subprocess
import sys
import tarfile
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from codehub.artifacts import ArtifactStore
from codehub.builds import build_bundle
from codehub.domain import DomainError
from codehub.repository import Repository


def git(path, *args):
    return (
        subprocess.run(
            ["git", "-c", "commit.gpgsign=false", "-C", str(path), *args], check=True, capture_output=True, timeout=15
        )
        .stdout.decode()
        .strip()
    )


def test_real_git_push_archive_build_and_persistent_artifact(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    git(source, "init", "-b", "main")
    git(source, "config", "user.name", "Morgan Lee")
    git(source, "config", "user.email", "morgan@example.test")
    (source / "routing.py").write_text("def choose_region(regions):\n    return min(regions)\n", encoding="utf-8")
    git(source, "add", "routing.py")
    git(source, "commit", "-m", "Choose a stable region")
    commit = git(source, "rev-parse", "HEAD")
    project = str(uuid4())
    repositories = Repository(tmp_path / "repositories")
    path = repositories.ensure(project)
    git(source, "push", str(path), "HEAD:refs/heads/main")
    assert repositories.commit(project, commit)["commit_sha"] == commit
    assert repositories.fsck(project)["refs"]["refs/heads/main"] == commit
    content, manifest = build_bundle(repositories.archive(project, commit), project_id=project, commit_sha=commit)
    repeated, _ = build_bundle(repositories.archive(project, commit), project_id=project, commit_sha=commit)
    assert content == repeated
    assert manifest["files"]["routing.py"] == hashlib.sha256((source / "routing.py").read_bytes()).hexdigest()
    with zipfile.ZipFile(io.BytesIO(content)) as bundle:
        assert json.loads(bundle.read("CODEHUB-BUILD.json"))["commit_sha"] == commit
        assert bundle.read("routing.py") == (source / "routing.py").read_bytes()
    checksum = hashlib.sha256(content).hexdigest()
    ArtifactStore(tmp_path / "artifacts").put(checksum, content)
    assert ArtifactStore(tmp_path / "artifacts").get(checksum) == content
    clone = tmp_path / "clone"
    subprocess.run(
        ["git", "clone", "--branch", "main", str(path), str(clone)], check=True, capture_output=True, timeout=15
    )
    assert (clone / "routing.py").read_bytes() == (source / "routing.py").read_bytes()


def archive_file(name, content=b"print('hello')\n", *, link=False):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        info = tarfile.TarInfo(name)
        if link:
            info.type = tarfile.SYMTYPE
            info.linkname = "/etc/passwd"
            archive.addfile(info)
        else:
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))
    return output.getvalue()


def test_sql_ref_publication_preserves_pending_push_and_propagates_accepted_force_push(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    git(source, "init", "-b", "main")
    git(source, "config", "user.name", "Morgan Lee")
    git(source, "config", "user.email", "morgan@example.test")
    (source / "router.py").write_text("version = 1\n")
    git(source, "add", "router.py")
    git(source, "commit", "-m", "Introduce routing")
    first = git(source, "rev-parse", "HEAD")
    repository = Repository(tmp_path / "local")
    project, entity = str(uuid4()), str(uuid4())
    path = repository.ensure(project)
    git(source, "push", str(path), "HEAD:refs/heads/main")
    initial = {"id": entity, "ref": "refs/heads/main", "commit_sha": first, "revision": 1}
    repository.publish_reference(project, initial)
    (source / "router.py").write_text("version = 2\n")
    git(source, "commit", "-am", "Handle empty destinations")
    second = git(source, "rev-parse", "HEAD")
    git(source, "push", str(path), "HEAD:refs/heads/main")
    # A stale metadata read during the two-stage upload must not rewind Git.
    repository.publish_reference(project, initial)
    assert repository.ref(project, "refs/heads/main") == second
    accepted = {**initial, "commit_sha": second, "revision": 2}
    repository.publish_reference(project, accepted)
    replica = Repository(tmp_path / "replica")
    replica_path = replica.ensure(project)
    git(replica_path, "fetch", str(path), "+refs/heads/*:refs/remotes/peer/*")
    replica.publish_reference(project, initial)
    replica.publish_reference(project, accepted)
    assert replica.ref(project, "refs/heads/main") == second
    # An explicit, accepted later revision can legitimately rewind a branch.
    git(source, "push", "--force", str(path), first + ":refs/heads/main")
    rewound = {**initial, "revision": 3}
    repository.publish_reference(project, rewound)
    replica.publish_reference(project, rewound)
    assert replica.ref(project, "refs/heads/main") == first
    restarted = Repository(tmp_path / "replica")
    restarted.publish_reference(project, accepted)
    assert restarted.ref(project, "refs/heads/main") == first
    git(replica_path, "update-ref", "-d", "refs/heads/main")
    restarted.publish_reference(project, rewound)
    assert restarted.ref(project, "refs/heads/main") == first


@pytest.mark.parametrize("crash_position", ["before", "after"])
def test_sql_reference_publication_recovers_crashed_cas_after_sql_advances(tmp_path, crash_position):
    source = tmp_path / "source"
    source.mkdir()
    git(source, "init", "-b", "main")
    git(source, "config", "user.name", "Morgan Lee")
    git(source, "config", "user.email", "morgan@example.test")
    commits = []
    for revision in (1, 2, 3):
        (source / "router.py").write_text(f"version = {revision}\n")
        git(source, "add", "router.py")
        git(source, "commit", "-m", f"Update routing {revision}")
        commits.append(git(source, "rev-parse", "HEAD"))
    project, entity = str(uuid4()), str(uuid4())
    repository = Repository(tmp_path / "replica")
    target = repository.ensure(project)
    git(source, "push", str(target), commits[0] + ":refs/heads/main")
    git(target, "fetch", str(source), "HEAD")
    reference = {"id": entity, "ref": "refs/heads/main", "revision": 1, "commit_sha": commits[0]}
    repository.publish_reference(project, reference)
    script = """
import json,os,sys
from pathlib import Path
from codehub.repository import Repository
repo=Repository(Path(sys.argv[1])); original=repo.git
def interrupted(project,*args,**kwargs):
    if args[0]=='update-ref' and sys.argv[4]=='before': os._exit(73)
    result=original(project,*args,**kwargs)
    if args[0]=='update-ref': os._exit(73)
    return result
repo.git=interrupted
repo.publish_reference(sys.argv[2],json.loads(sys.argv[3]))
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(repository.root),
            project,
            json.dumps({**reference, "revision": 2, "commit_sha": commits[1]}),
            crash_position,
        ],
        capture_output=True,
        timeout=15,
    )
    assert result.returncode == 73
    assert repository.ref(project, reference["ref"]) == commits[0 if crash_position == "before" else 1]
    restarted = Repository(repository.root)
    restarted.publish_reference(project, {**reference, "revision": 3, "commit_sha": commits[2]})
    assert restarted.ref(project, reference["ref"]) == commits[2]


@pytest.mark.parametrize("name", ["../outside.py", "/absolute.py"])
def test_build_rejects_unsafe_tree_paths(name):
    with pytest.raises(DomainError):
        build_bundle(archive_file(name), project_id=str(uuid4()), commit_sha="a" * 40)


def test_build_rejects_links_and_invalid_python():
    with pytest.raises(DomainError):
        build_bundle(archive_file("pointer", link=True), project_id=str(uuid4()), commit_sha="a" * 40)
    with pytest.raises(DomainError):
        build_bundle(archive_file("broken.py", b"def invalid(:\n"), project_id=str(uuid4()), commit_sha="a" * 40)


@pytest.mark.parametrize("same_project", [True, False])
@pytest.mark.parametrize("default_ref", ["refs/heads/main", "refs/heads/trunk"])
def test_real_smart_http_requests_overlap_without_racing_default_head(tmp_path, monkeypatch, same_project, default_ref):
    repository = Repository(tmp_path / "repositories")
    projects = [str(uuid4()), str(uuid4())]
    if same_project:
        projects[1] = projects[0]
    for project in set(projects):
        path = repository.ensure(project)
        if default_ref == "refs/heads/main":
            # A reader needs no HEAD write even while a Git writer owns its lock.
            (path / "HEAD.lock").write_bytes(b"")
    backend_barrier = threading.Barrier(2)
    original_popen, original_git = subprocess.Popen, repository.git
    updates = []

    def popen(command, *args, **kwargs):
        if command[:2] == ["git", "http-backend"]:
            backend_barrier.wait(timeout=5)
        return original_popen(command, *args, **kwargs)

    def git_update(project, *args, **kwargs):
        if args[:2] == ("symbolic-ref", "HEAD"):
            updates.append(project)
            time.sleep(0.05)  # Make competing default-HEAD updates overlap.
        return original_git(project, *args, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", popen)
    monkeypatch.setattr(repository, "git", git_update)

    def read(project):
        return repository.backend(
            project,
            "info/refs",
            method="GET",
            query="service=git-upload-pack",
            content_type="",
            actor_id="customer",
            body=b"",
            default_ref=default_ref,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        pending = [pool.submit(read, project) for project in projects]
        responses = [future.result(timeout=10) for future in pending]
    assert all(response.status_code == 200 and b"# service=git-upload-pack" in response.body for response in responses)
    assert len(updates) == (0 if default_ref == "refs/heads/main" else len(set(projects)))
    for project in set(projects):
        assert (repository.path(project) / "HEAD").read_text().strip() == f"ref: {default_ref}"
