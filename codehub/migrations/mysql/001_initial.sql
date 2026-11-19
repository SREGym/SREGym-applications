CREATE TABLE IF NOT EXISTS schema_versions (
  version INTEGER PRIMARY KEY, applied_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS users (
  id CHAR(36) PRIMARY KEY, username VARCHAR(80) NOT NULL UNIQUE,
  token_sha256 CHAR(64) NOT NULL UNIQUE,
  created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS organizations (
  id CHAR(36) PRIMARY KEY, slug VARCHAR(80) NOT NULL UNIQUE, name VARCHAR(200) NOT NULL,
  owner_id CHAR(36) NOT NULL, FOREIGN KEY (owner_id) REFERENCES users(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS memberships (
  tenant_id CHAR(36) NOT NULL, user_id CHAR(36) NOT NULL,
  role ENUM('owner','admin','developer','viewer') NOT NULL,
  PRIMARY KEY(tenant_id,user_id), FOREIGN KEY(tenant_id) REFERENCES organizations(id),
  FOREIGN KEY(user_id) REFERENCES users(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS entities (
  tenant_id CHAR(36) NOT NULL, id CHAR(36) NOT NULL, project_id CHAR(36),
  entity_type VARCHAR(32) NOT NULL, revision BIGINT NOT NULL, document JSON NOT NULL,
  PRIMARY KEY(tenant_id,id), INDEX(project_id,entity_type),
  FOREIGN KEY(tenant_id) REFERENCES organizations(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS projects (
  id CHAR(36) PRIMARY KEY, tenant_id CHAR(36) NOT NULL, slug VARCHAR(80) NOT NULL,
  name VARCHAR(200) NOT NULL, default_ref VARCHAR(200) NOT NULL, author_id CHAR(36) NOT NULL,
  UNIQUE(tenant_id,slug), FOREIGN KEY(tenant_id) REFERENCES organizations(id),
  FOREIGN KEY(author_id) REFERENCES users(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS issues (
  id CHAR(36) PRIMARY KEY, tenant_id CHAR(36) NOT NULL, project_id CHAR(36) NOT NULL,
  title VARCHAR(200) NOT NULL, body TEXT NOT NULL, state ENUM('open','closed') NOT NULL,
  author_id CHAR(36) NOT NULL, FOREIGN KEY(project_id) REFERENCES projects(id),
  FOREIGN KEY(author_id) REFERENCES users(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS comments (
  id CHAR(36) PRIMARY KEY, tenant_id CHAR(36) NOT NULL, project_id CHAR(36) NOT NULL,
  issue_id CHAR(36) NOT NULL, body TEXT NOT NULL, author_id CHAR(36) NOT NULL,
  FOREIGN KEY(project_id) REFERENCES projects(id), FOREIGN KEY(issue_id) REFERENCES issues(id),
  FOREIGN KEY(author_id) REFERENCES users(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS changes (
  id CHAR(36) PRIMARY KEY, tenant_id CHAR(36) NOT NULL, project_id CHAR(36) NOT NULL,
  title VARCHAR(200) NOT NULL, head_sha CHAR(40) NOT NULL, base_sha CHAR(40) NOT NULL,
  head_ref VARCHAR(200) NOT NULL, base_ref VARCHAR(200) NOT NULL,
  state ENUM('open','closed','merged') NOT NULL, author_id CHAR(36) NOT NULL,
  FOREIGN KEY(project_id) REFERENCES projects(id), FOREIGN KEY(author_id) REFERENCES users(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS reviews (
  id CHAR(36) PRIMARY KEY, tenant_id CHAR(36) NOT NULL, project_id CHAR(36) NOT NULL,
  change_id CHAR(36) NOT NULL, head_sha CHAR(40) NOT NULL,
  verdict ENUM('approve','request_changes','comment') NOT NULL, body TEXT NOT NULL,
  author_id CHAR(36) NOT NULL, FOREIGN KEY(project_id) REFERENCES projects(id),
  FOREIGN KEY(change_id) REFERENCES changes(id), FOREIGN KEY(author_id) REFERENCES users(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS repository_refs (
  id CHAR(36) PRIMARY KEY, tenant_id CHAR(36) NOT NULL, project_id CHAR(36) NOT NULL,
  ref VARCHAR(200) NOT NULL, commit_sha CHAR(40) NOT NULL, author_id CHAR(36) NOT NULL,
  UNIQUE(project_id,ref), FOREIGN KEY(project_id) REFERENCES projects(id),
  FOREIGN KEY(author_id) REFERENCES users(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS webhooks (
  id CHAR(36) PRIMARY KEY, tenant_id CHAR(36) NOT NULL, project_id CHAR(36) NOT NULL,
  url VARCHAR(2048) NOT NULL, events JSON NOT NULL, enabled BOOLEAN NOT NULL,
  author_id CHAR(36) NOT NULL, FOREIGN KEY(project_id) REFERENCES projects(id),
  FOREIGN KEY(author_id) REFERENCES users(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS journal (
  event_id CHAR(36) PRIMARY KEY, record_key VARCHAR(73) NOT NULL,
  tenant_id CHAR(36) NOT NULL, entity_id CHAR(36) NOT NULL, project_id CHAR(36),
  client_revision BIGINT NOT NULL, kind VARCHAR(48) NOT NULL, payload JSON NOT NULL,
  actor_id CHAR(36) NOT NULL, payload_sha256 CHAR(64) NOT NULL,
  accepted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  UNIQUE(record_key,client_revision), INDEX(tenant_id,event_id), INDEX(project_id,event_id),
  FOREIGN KEY(actor_id) REFERENCES users(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS outbox (
  effect_id CHAR(64) PRIMARY KEY, event_id CHAR(36) NOT NULL, effect_kind VARCHAR(24) NOT NULL,
  destination VARCHAR(256) NOT NULL, payload JSON NOT NULL,
  state ENUM('pending','published','done') NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0, last_error VARCHAR(500),
  available_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  published_at TIMESTAMP(6), completed_at TIMESTAMP(6),
  UNIQUE(event_id,effect_kind,destination), INDEX(state,available_at),
  FOREIGN KEY(event_id) REFERENCES journal(event_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS builds (
  effect_id CHAR(64) PRIMARY KEY, event_id CHAR(36) NOT NULL, project_id CHAR(36) NOT NULL,
  commit_sha CHAR(40) NOT NULL, artifact_sha256 CHAR(64) NOT NULL,
  file_count INTEGER NOT NULL, created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  FOREIGN KEY(project_id) REFERENCES projects(id), FOREIGN KEY(event_id) REFERENCES journal(event_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS artifacts (
  sha256 CHAR(64) PRIMARY KEY, project_id CHAR(36) NOT NULL, size_bytes BIGINT NOT NULL,
  content_type VARCHAR(100) NOT NULL, FOREIGN KEY(project_id) REFERENCES projects(id)
) ENGINE=InnoDB;

INSERT IGNORE INTO schema_versions(version) VALUES (1);
