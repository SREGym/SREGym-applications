"""Public immutable-operation semantics shared by storage and workers."""

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID


class DomainError(ValueError):
    status_code = 422


class Conflict(DomainError):
    status_code = 409


class Forbidden(DomainError):
    status_code = 403


class NotFound(DomainError):
    status_code = 404


def identifier(value: Any, name: str) -> str:
    try:
        parsed = UUID(value)
    except (ValueError, TypeError, AttributeError) as exc:
        raise DomainError(f"{name} must be a UUID") from exc
    if str(parsed) != value:
        raise DomainError(f"{name} must use canonical UUID spelling")
    return value


def canonical(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (ValueError, TypeError) as exc:
        raise DomainError("Payload must be finite JSON") from exc


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def text(payload: dict, field: str, maximum: int = 4096) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip() or len(value) > maximum or "\x00" in value:
        raise DomainError(f"{field} must contain 1..{maximum} characters")
    return value


def slug(payload: dict, field: str = "slug") -> str:
    value = text(payload, field, 80)
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", value):
        raise DomainError(f"{field} must contain lowercase letters, digits or hyphens")
    return value


def sha(payload: dict, field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}", value):
        raise DomainError(f"{field} must be a full Git object identifier")
    return value


KINDS = frozenset(
    {
        "organization.create",
        "membership.set",
        "project.create",
        "project.update",
        "issue.create",
        "issue.update",
        "comment.create",
        "change.create",
        "change.update",
        "review.create",
        "repository.push",
        "webhook.create",
        "webhook.update",
    }
)
PAYLOAD_FIELDS = {
    "organization": {"slug", "name"},
    "membership": {"user_id", "role"},
    "project": {"slug", "name", "default_ref"},
    "issue": {"title", "body", "state"},
    "comment": {"issue_id", "body"},
    "change": {"title", "head_sha", "base_sha", "head_ref", "base_ref", "state"},
    "review": {"change_id", "head_sha", "verdict", "body"},
    "repository": {"ref", "commit_sha"},
    "webhook": {"url", "events", "enabled"},
}
ENTITY_TYPES = {
    "organization": "organization",
    "membership": "membership",
    "project": "project",
    "issue": "issue",
    "comment": "comment",
    "change": "change",
    "review": "review",
    "repository": "repository",
    "webhook": "webhook",
}


@dataclass(frozen=True)
class Operation:
    event_id: str
    tenant_id: str
    entity_id: str
    project_id: str | None
    client_revision: int
    kind: str
    payload: dict

    def __post_init__(self):
        for field in ("event_id", "tenant_id", "entity_id"):
            identifier(getattr(self, field), field)
        if self.project_id is not None:
            identifier(self.project_id, "project_id")
        if type(self.client_revision) is not int or self.client_revision < 1:
            raise DomainError("client_revision must be a positive integer")
        if self.kind not in KINDS or not isinstance(self.payload, dict):
            raise DomainError("Unsupported operation kind or payload")
        if self.entity_type in {"organization", "membership"} and self.project_id is not None:
            raise DomainError("Organization and membership operations have no project")
        if self.entity_type not in {"organization", "membership"} and self.project_id is None:
            raise DomainError("Project-scoped operations require project_id")
        if set(self.payload) - PAYLOAD_FIELDS[self.entity_type]:
            raise DomainError("Payload contains unsupported fields")
        if len(canonical(self.payload).encode()) > 128 * 1024:
            raise DomainError("Payload exceeds 128 KiB")

    @property
    def record_key(self) -> str:
        return f"{self.tenant_id}/{self.entity_id}"

    @property
    def entity_type(self) -> str:
        return ENTITY_TYPES[self.kind.split(".", 1)[0]]

    def as_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "tenant_id": self.tenant_id,
            "entity_id": self.entity_id,
            "project_id": self.project_id,
            "client_revision": self.client_revision,
            "kind": self.kind,
            "payload": self.payload,
        }


@dataclass(frozen=True)
class Effect:
    kind: str
    destination: str
    payload: dict

    def identity(self, event_id: str) -> str:
        return hashlib.sha256(f"{event_id}/{self.kind}/{self.destination}".encode()).hexdigest()


def effects_for(op: Operation, subscriptions: list[dict]) -> list[Effect]:
    effects: list[Effect] = []
    if op.entity_type in {"project", "issue", "comment", "change", "review", "repository"}:
        effects.append(Effect("search", op.entity_id, op.as_dict()))
    if op.kind in {"repository.push", "change.create", "change.update"}:
        commit = op.payload.get("commit_sha") or op.payload.get("head_sha")
        if commit:
            effects.append(Effect("build", op.project_id or op.entity_id, {**op.as_dict(), "commit_sha": commit}))
    for subscription in subscriptions:
        if op.kind in subscription["events"] and subscription["enabled"]:
            effects.append(
                Effect("delivery", subscription["id"], {"operation": op.as_dict(), "url": subscription["url"]})
            )
    return effects
