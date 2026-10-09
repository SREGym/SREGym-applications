from typing import Protocol

from codehub.domain import DomainError, Operation, identifier

MAX_ENTITY_LOOKUP_IDS = 128
MAX_ENTITY_DOCUMENT_BYTES = 256 * 1024


def entity_ids(values) -> tuple[str, ...]:
    if not isinstance(values, (list, tuple)) or not 1 <= len(values) <= MAX_ENTITY_LOOKUP_IDS:
        raise DomainError("ids must contain 1..128 UUIDs")
    result = tuple(identifier(value, "entity_id") for value in values)
    if len(set(result)) != len(result):
        raise DomainError("ids must not contain duplicates")
    return result


def lookup_ids(body) -> tuple[str, ...]:
    if not isinstance(body, dict) or set(body) != {"ids"}:
        raise DomainError("Lookup requires only ids")
    return entity_ids(body["ids"])


class Storage(Protocol):
    def accept(self, operation: Operation, actor_id: str) -> dict: ...

    def get_entity(self, tenant_id: str, entity_id: str, actor_id: str) -> dict: ...

    def get_entities(self, tenant_id: str, entity_ids: tuple[str, ...], actor_id: str) -> list[dict]: ...

    def events(self, tenant_id: str, actor_id: str, *, after: str = "", limit: int = 100) -> list[dict]: ...
