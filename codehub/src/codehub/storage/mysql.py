"""MySQL transactions are the acceptance and durable-work boundary."""

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path

import pymysql

from codehub.changes import change_document, review_document
from codehub.config import Settings
from codehub.domain import (
    Conflict,
    DomainError,
    Forbidden,
    NotFound,
    Operation,
    canonical,
    digest,
    effects_for,
    identifier,
)
from codehub.identity import membership_document, organization_document
from codehub.issues import comment_document, issue_document
from codehub.projects import project_document, ref_name
from codehub.storage.base import MAX_ENTITY_DOCUMENT_BYTES, entity_ids
from codehub.webhooks import webhook_document


TABLES = {
    "organization": "organizations",
    "membership": "memberships",
    "project": "projects",
    "issue": "issues",
    "comment": "comments",
    "change": "changes",
    "review": "reviews",
    "repository": "repository_refs",
    "webhook": "webhooks",
}


class MySQLStorage:
    def __init__(self, settings: Settings):
        self.settings = settings

    @contextmanager
    def connection(self, *, read=False):
        use_reader = read and bool(self.settings.mysql_read_host)
        connection = pymysql.connect(
            host=self.settings.mysql_read_host if use_reader else self.settings.mysql_host,
            port=(self.settings.mysql_read_port or self.settings.mysql_port)
            if use_reader
            else self.settings.mysql_port,
            user=self.settings.mysql_user,
            password=self.settings.mysql_password,
            database=self.settings.mysql_database,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
            connect_timeout=5,
            read_timeout=15,
            write_timeout=15,
        )
        try:
            yield connection
        finally:
            connection.close()

    def migrate(self, filename: Path):
        with self.connection() as connection, connection.cursor() as cursor:
            for statement in filename.read_text(encoding="utf-8").split(";"):
                if statement.strip():
                    cursor.execute(statement)
            connection.commit()

    def readiness(self) -> dict:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT @@server_uuid AS server_uuid, @@read_only AS read_only")
            server = cursor.fetchone()
            cursor.execute("SELECT MAX(version) AS schema_version FROM schema_versions")
            server.update(cursor.fetchone())
            return server

    def register_user(self, user_id: str, username: str, api_token: str):
        identifier(user_id, "user_id")
        if not isinstance(username, str) or not username or len(username) > 80:
            raise DomainError("username must contain 1..80 characters")
        if not isinstance(api_token, str) or len(api_token) < 32 or len(api_token) > 256:
            raise DomainError("api_token must contain 32..256 characters")
        token_digest = hashlib.sha256(api_token.encode()).hexdigest()
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT id,username,token_sha256 FROM users WHERE id=%s FOR UPDATE", (user_id,))
            previous = cursor.fetchone()
            if previous:
                if previous["username"] != username or previous["token_sha256"] != token_digest:
                    raise Conflict("User identity already has different credentials")
                return {"id": user_id, "username": username}
            try:
                cursor.execute(
                    "INSERT INTO users(id,username,token_sha256) VALUES(%s,%s,%s)", (user_id, username, token_digest)
                )
                connection.commit()
            except pymysql.err.IntegrityError as exc:
                connection.rollback()
                raise Conflict("Username or credential is already registered") from exc
        return {"id": user_id, "username": username}

    def authenticate(self, api_token: str) -> str:
        if not api_token or len(api_token) > 256:
            raise Forbidden("Valid user credentials are required")
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM users WHERE token_sha256=%s", (hashlib.sha256(api_token.encode()).hexdigest(),)
            )
            user = cursor.fetchone()
            if not user:
                raise Forbidden("Valid user credentials are required")
            return user["id"]

    @staticmethod
    def authorize(cursor, tenant_id: str, actor_id: str, *, write: bool = False, admin: bool = False):
        cursor.execute("SELECT role FROM memberships WHERE tenant_id=%s AND user_id=%s", (tenant_id, actor_id))
        member = cursor.fetchone()
        allowed = (
            {"owner", "admin"}
            if admin
            else {"owner", "admin", "developer"}
            if write
            else {"owner", "admin", "developer", "viewer"}
        )
        if not member or member["role"] not in allowed:
            raise Forbidden("Membership does not permit this operation")
        return member["role"]

    def project(self, project_id: str, actor_id: str | None, *, write: bool = False):
        identifier(project_id, "project_id")
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT * FROM projects WHERE id=%s", (project_id,))
            project = cursor.fetchone()
            if not project:
                raise NotFound("Project does not exist")
            if actor_id is not None:
                self.authorize(cursor, project["tenant_id"], actor_id, write=write)
            return project

    def _document(self, cursor, op, actor_id, existing):
        def lookup(table, entity_id):
            if table == "memberships_role":
                cursor.execute(
                    "SELECT role FROM memberships WHERE tenant_id=%s AND user_id=%s", (op.tenant_id, entity_id)
                )
                row = cursor.fetchone()
                return row["role"] if row else None
            if table not in {"users", "issues", "changes"}:
                raise ValueError("Unsupported relationship lookup")
            cursor.execute(f"SELECT * FROM {table} WHERE id=%s", (entity_id,))
            row = cursor.fetchone()
            if row and table != "users" and row["tenant_id"] != op.tenant_id:
                return None
            return row

        entity_type = op.entity_type
        if entity_type == "organization":
            return organization_document(op, actor_id)
        if entity_type == "membership":
            return membership_document(op, actor_id, lookup)
        if entity_type == "project":
            return project_document(op, actor_id, existing)
        if entity_type == "issue":
            return issue_document(op, actor_id, existing)
        if entity_type == "comment":
            return comment_document(op, actor_id, lookup)
        if entity_type == "change":
            return change_document(op, actor_id, existing)
        if entity_type == "review":
            return review_document(op, actor_id, lookup)
        if entity_type == "webhook":
            return webhook_document(op, actor_id, existing)
        if entity_type == "repository":
            from codehub.domain import sha

            return {
                "ref": ref_name(op.payload, "ref"),
                "commit_sha": sha(op.payload, "commit_sha"),
                "author_id": actor_id,
            }
        raise DomainError("Unsupported entity type")

    @staticmethod
    def _persist_document(cursor, op, document):
        entity_type = op.entity_type
        if entity_type == "membership":
            cursor.execute(
                "INSERT INTO memberships(tenant_id,user_id,role) VALUES(%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE role=VALUES(role)",
                (op.tenant_id, document["user_id"], document["role"]),
            )
        else:
            values = {"id": op.entity_id, **document}
            if entity_type != "organization":
                values["tenant_id"] = op.tenant_id
                if entity_type != "project":
                    values["project_id"] = op.project_id
            if "events" in values:
                values["events"] = canonical(values["events"])
            columns = ",".join(f"`{name}`" for name in values)
            placeholders = ",".join("%s" for _ in values)
            cursor.execute(f"SELECT id FROM {TABLES[entity_type]} WHERE id=%s", (op.entity_id,))
            if cursor.fetchone():
                updates = ",".join(f"`{name}`=%s" for name in values if name != "id")
                cursor.execute(
                    f"UPDATE {TABLES[entity_type]} SET {updates} WHERE id=%s",
                    (*[value for name, value in values.items() if name != "id"], op.entity_id),
                )
            else:
                # A conflicting secondary unique key must fail, never update a different entity's row.
                cursor.execute(
                    f"INSERT INTO {TABLES[entity_type]}({columns}) VALUES({placeholders})", tuple(values.values())
                )
        cursor.execute(
            "INSERT INTO entities(tenant_id,id,project_id,entity_type,revision,document) VALUES(%s,%s,%s,%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE revision=VALUES(revision),document=VALUES(document)",
            (op.tenant_id, op.entity_id, op.project_id, entity_type, op.client_revision, canonical(document)),
        )

    def accept(self, op: Operation, actor_id: str) -> dict:
        with self.connection() as connection, connection.cursor() as cursor:
            try:
                # Tenant row serializes dependent-domain changes, including revision decisions.
                if op.kind == "organization.create":
                    cursor.execute("SELECT id FROM users WHERE id=%s FOR UPDATE", (actor_id,))
                    if not cursor.fetchone():
                        raise Forbidden("Actor does not exist")
                else:
                    cursor.execute("SELECT id FROM organizations WHERE id=%s FOR UPDATE", (op.tenant_id,))
                    if not cursor.fetchone():
                        raise NotFound("Organization does not exist")
                    self.authorize(
                        cursor,
                        op.tenant_id,
                        actor_id,
                        write=True,
                        admin=op.entity_type in {"membership", "webhook"},
                    )
                cursor.execute("SELECT * FROM journal WHERE event_id=%s", (op.event_id,))
                previous = cursor.fetchone()
                operation_digest = digest({**op.as_dict(), "actor_id": actor_id})
                if previous:
                    if previous["payload_sha256"] != operation_digest:
                        raise Conflict("Event identity is already associated with a different operation")
                    return self._receipt(op, actor_id, operation_digest, replayed=True)
                cursor.execute(
                    "SELECT event_id FROM journal WHERE record_key=%s AND client_revision=%s",
                    (op.record_key, op.client_revision),
                )
                if cursor.fetchone():
                    raise Conflict("Record revision is already associated with another event")
                cursor.execute("SELECT * FROM entities WHERE tenant_id=%s AND id=%s", (op.tenant_id, op.entity_id))
                entity = cursor.fetchone()
                existing = json.loads(entity["document"]) if entity else None
                if entity and (entity["entity_type"] != op.entity_type or entity["project_id"] != op.project_id):
                    raise Conflict("Entity type and project membership are immutable")
                if op.kind.endswith(".create") and (entity or op.client_revision != 1):
                    raise Conflict("Create requires a new entity at revision one")
                if not entity and op.client_revision != 1:
                    raise Conflict("A new record starts at revision one")
                if op.kind.endswith(".update") and not entity:
                    raise NotFound("Update requires an existing entity")
                if op.project_id and op.entity_type != "project":
                    cursor.execute("SELECT tenant_id FROM projects WHERE id=%s", (op.project_id,))
                    project = cursor.fetchone()
                    if not project or project["tenant_id"] != op.tenant_id:
                        raise DomainError("Project must belong to the requested organization")
                elif op.entity_type not in {"organization", "membership", "project"}:
                    raise DomainError("This entity requires a project")
                document = self._document(cursor, op, actor_id, existing)
                if op.entity_type == "membership":
                    cursor.execute(
                        "SELECT role FROM memberships WHERE tenant_id=%s AND user_id=%s",
                        (op.tenant_id, document["user_id"]),
                    )
                    member = cursor.fetchone()
                    if member and member["role"] == "owner" and document["role"] != "owner":
                        if self.authorize(cursor, op.tenant_id, actor_id) != "owner":
                            raise Forbidden("Only an owner can change an owner's role")
                        cursor.execute(
                            "SELECT COUNT(*) AS n FROM memberships WHERE tenant_id=%s AND role='owner'", (op.tenant_id,)
                        )
                        if cursor.fetchone()["n"] <= 1:
                            raise Conflict("The last organization owner cannot be demoted")
                if op.entity_type == "organization":
                    self._persist_document(cursor, op, document)
                    cursor.execute(
                        "INSERT INTO memberships(tenant_id,user_id,role) VALUES(%s,%s,'owner')",
                        (op.tenant_id, actor_id),
                    )
                cursor.execute(
                    "INSERT INTO journal(event_id,record_key,tenant_id,entity_id,project_id,client_revision,kind,payload,"
                    "actor_id,payload_sha256) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        op.event_id,
                        op.record_key,
                        op.tenant_id,
                        op.entity_id,
                        op.project_id,
                        op.client_revision,
                        op.kind,
                        canonical(op.payload),
                        actor_id,
                        operation_digest,
                    ),
                )
                if op.entity_type != "organization" and (not entity or op.client_revision > entity["revision"]):
                    self._persist_document(cursor, op, document)
                cursor.execute("SELECT id,url,events,enabled FROM webhooks WHERE project_id=%s", (op.project_id,))
                subscriptions = cursor.fetchall() if op.project_id else []
                for subscription in subscriptions:
                    subscription["events"] = json.loads(subscription["events"])
                for effect in effects_for(op, subscriptions):
                    cursor.execute(
                        "INSERT INTO outbox(effect_id,event_id,effect_kind,destination,payload) VALUES(%s,%s,%s,%s,%s)",
                        (
                            effect.identity(op.event_id),
                            op.event_id,
                            effect.kind,
                            effect.destination,
                            canonical(effect.payload),
                        ),
                    )
                connection.commit()
                return self._receipt(op, actor_id, operation_digest, replayed=False)
            except pymysql.err.IntegrityError as exc:
                connection.rollback()
                raise Conflict("A domain identity or relationship constraint was violated") from exc
            except Exception:
                connection.rollback()
                raise

    def _receipt(self, op, actor_id, operation_digest, *, replayed):
        return {
            **op.as_dict(),
            "record_key": op.record_key,
            "actor_id": actor_id,
            "operation_sha256": operation_digest,
            "region": self.settings.region,
            "replayed": replayed,
        }

    def get_entity(self, tenant_id: str, entity_id: str, actor_id: str):
        with self.connection(read=True) as connection, connection.cursor() as cursor:
            self.authorize(cursor, tenant_id, actor_id)
            cursor.execute("SELECT * FROM entities WHERE tenant_id=%s AND id=%s", (tenant_id, entity_id))
            row = cursor.fetchone()
            if not row:
                raise NotFound("Entity does not exist")
            row["document"] = json.loads(row["document"])
            return row

    def get_entities(self, tenant_id: str, ids, actor_id: str):
        identifier(tenant_id, "tenant_id")
        ids = entity_ids(ids)
        placeholders = ",".join("%s" for _ in ids)
        with self.connection(read=True) as connection, connection.cursor() as cursor:
            self.authorize(cursor, tenant_id, actor_id)
            # Bound every document before transporting it from SQL. The cap
            # includes room for metadata added to accepted 128 KiB payloads.
            cursor.execute(
                "SELECT tenant_id,id,project_id,entity_type,revision,"
                "CASE WHEN OCTET_LENGTH(document)<=%s THEN document ELSE NULL END AS document "
                f"FROM entities WHERE tenant_id=%s AND id IN ({placeholders}) ORDER BY id",
                (MAX_ENTITY_DOCUMENT_BYTES, tenant_id, *ids),
            )
            rows = list(cursor.fetchall())
            for row in rows:
                if row["document"] is None:
                    raise DomainError("Entity document exceeds API capacity")
                row["document"] = json.loads(row["document"])
            return rows

    def list_entities(self, tenant_id, actor_id, *, project_id=None, entity_type=None, after="", limit=100):
        with self.connection(read=True) as connection, connection.cursor() as cursor:
            self.authorize(cursor, tenant_id, actor_id)
            query = "SELECT * FROM entities WHERE tenant_id=%s AND id>%s"
            values = [tenant_id, after]
            if project_id:
                query += " AND project_id=%s"
                values.append(project_id)
            if entity_type:
                query += " AND entity_type=%s"
                values.append(entity_type)
            query += " ORDER BY id LIMIT %s"
            cursor.execute(query, (*values, limit))
            rows = cursor.fetchall()
            for row in rows:
                row["document"] = json.loads(row["document"])
            return rows

    def events(self, tenant_id, actor_id, *, after="", limit=100):
        with self.connection() as connection, connection.cursor() as cursor:
            self.authorize(cursor, tenant_id, actor_id)
            cursor.execute(
                "SELECT event_id,tenant_id,entity_id,project_id,client_revision,kind,payload,actor_id,payload_sha256 "
                "FROM journal WHERE tenant_id=%s AND event_id>%s ORDER BY event_id LIMIT %s",
                (tenant_id, after, limit),
            )
            rows = cursor.fetchall()
            for row in rows:
                row["payload"] = json.loads(row["payload"])
            return rows

    def pending(self, *, limit=100):
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT effect_id FROM outbox WHERE state!='done' AND available_at<=CURRENT_TIMESTAMP(6) "
                "ORDER BY available_at,effect_id LIMIT %s",
                (limit,),
            )
            return [row["effect_id"] for row in cursor.fetchall()]

    def publish_confirmed(self, effect_id):
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE outbox SET state=IF(state='done','done','published'),published_at=CURRENT_TIMESTAMP(6),"
                "available_at=DATE_ADD(CURRENT_TIMESTAMP(6),INTERVAL 30 SECOND) WHERE effect_id=%s",
                (effect_id,),
            )
            connection.commit()

    def effect(self, effect_id):
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT * FROM outbox WHERE effect_id=%s", (effect_id,))
            row = cursor.fetchone()
            if row:
                row["payload"] = json.loads(row["payload"])
            return row

    def complete(self, effect_id):
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE outbox SET state='done',completed_at=CURRENT_TIMESTAMP(6),last_error=NULL WHERE effect_id=%s",
                (effect_id,),
            )
            connection.commit()

    def retry(self, effect_id, error):
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE outbox SET state='pending',attempts=attempts+1,last_error=%s,"
                "available_at=TIMESTAMPADD(SECOND,LEAST(300,POW(2,LEAST(attempts+1,8))),CURRENT_TIMESTAMP(6)) "
                "WHERE effect_id=%s AND state!='done'",
                (error[:500], effect_id),
            )
            retained = cursor.rowcount > 0
            connection.commit()
            return retained

    def record_build(self, effect_id, event_id, project_id, commit_sha, checksum, size, count):
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "INSERT IGNORE INTO artifacts(sha256,project_id,size_bytes,content_type) VALUES(%s,%s,%s,'application/zip')",
                (checksum, project_id, size),
            )
            cursor.execute(
                "INSERT INTO builds(effect_id,event_id,project_id,commit_sha,artifact_sha256,file_count) "
                "VALUES(%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE effect_id=VALUES(effect_id)",
                (effect_id, event_id, project_id, commit_sha, checksum, count),
            )
            cursor.execute("SELECT commit_sha,artifact_sha256 FROM builds WHERE effect_id=%s", (effect_id,))
            result = cursor.fetchone()
            if result["commit_sha"] != commit_sha or result["artifact_sha256"] != checksum:
                raise Conflict("A build identity cannot produce different content")
            connection.commit()

    def builds(self, project_id, actor_id):
        self.project(project_id, actor_id)
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT * FROM builds WHERE project_id=%s ORDER BY created_at DESC LIMIT 100", (project_id,))
            return cursor.fetchall()
