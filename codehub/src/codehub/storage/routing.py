"""Tenant/group routing from normal, inspectable deployment configuration."""

import json
from dataclasses import replace
from pathlib import Path

import pymysql

from codehub.domain import DomainError, NotFound
from codehub.storage.mysql import MySQLStorage


class StorageRouter:
    def __init__(self, settings):
        self.settings = settings

    def routes(self):
        if self.settings.routes_file is None:
            return {"*": {"group": "group-0"}}
        value = json.loads(self.settings.routes_file.read_text(encoding="utf-8"))
        if not isinstance(value, dict) or not value or any(not isinstance(route, dict) for route in value.values()):
            raise DomainError("Database routing configuration is invalid")
        return value

    def make_storage(self, route):
        password = self.settings.mysql_password
        if route.get("password_file"):
            password = Path(route["password_file"]).read_text(encoding="utf-8").strip()
        return MySQLStorage(
            replace(
                self.settings,
                mysql_host=route.get("writer_host", self.settings.mysql_host),
                mysql_port=int(route.get("port", self.settings.mysql_port)),
                mysql_read_host=route.get("reader_host", self.settings.mysql_read_host),
                mysql_read_port=int(
                    route.get("read_port", route.get("port", self.settings.mysql_read_port or self.settings.mysql_port))
                ),
                mysql_database=route.get("database", self.settings.mysql_database),
                mysql_user=route.get("user", self.settings.mysql_user),
                mysql_password=password,
            )
        )

    def for_tenant(self, tenant_id):
        routes = self.routes()
        route = routes.get(tenant_id, routes.get("*"))
        if route is None:
            raise NotFound("Organization has no configured database route")
        return self.make_storage(route)

    def groups(self):
        groups = {}
        for route in self.routes().values():
            group = route.get("group", "group-0")
            if group in groups and groups[group][0] != route:
                raise DomainError("A database group has inconsistent routes")
            groups[group] = (route, self.make_storage(route))
        return {group: storage for group, (_route, storage) in groups.items()}

    def for_project(self, project_id, actor_id, *, write=False):
        unavailable = None
        for storage in self.groups().values():
            try:
                storage.project(project_id, actor_id, write=write)
                return storage
            except NotFound:
                continue
            except pymysql.MySQLError as exc:
                unavailable = exc
        if unavailable:
            raise unavailable
        raise NotFound("Project does not exist in configured database groups")

    def authenticated_project(self, project_id, token, *, write=False):
        unavailable = None
        for storage in self.groups().values():
            try:
                actor = storage.authenticate(token)
                storage.project(project_id, actor, write=write)
                return storage, actor
            except NotFound:
                continue
            except pymysql.MySQLError as exc:
                unavailable = exc
        if unavailable:
            raise unavailable
        raise NotFound("Project does not exist in configured database groups")
