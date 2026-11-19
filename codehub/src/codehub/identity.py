"""Tenant ownership and membership rules."""

from codehub.domain import DomainError, Forbidden, identifier, slug, text


def organization_document(op, actor_id):
    if op.entity_id != op.tenant_id or op.project_id is not None or op.client_revision != 1:
        raise DomainError("An organization is its tenant root and starts at revision one")
    return {"slug": slug(op.payload), "name": text(op.payload, "name", 200), "owner_id": actor_id}


def membership_document(op, actor_id, lookup):
    user_id = identifier(op.payload.get("user_id"), "user_id")
    role = op.payload.get("role")
    if op.project_id is not None or op.entity_id != user_id:
        raise DomainError("Membership identity must equal the member identity")
    if role not in {"owner", "admin", "developer", "viewer"}:
        raise DomainError("Unsupported membership role")
    if not lookup("users", user_id):
        raise DomainError("Member does not exist")
    if role == "owner" and lookup("memberships_role", actor_id) != "owner":
        raise Forbidden("Only an owner can grant ownership")
    return {"user_id": user_id, "role": role}
