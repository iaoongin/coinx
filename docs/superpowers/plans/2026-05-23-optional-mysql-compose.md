# Optional MySQL Compose Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional MySQL service to Docker Compose without changing the default external database workflow.

**Architecture:** Keep `app` as the default Compose service and add `mysql` behind a `mysql` profile. Store MySQL data in a named volume and document the two startup modes clearly.

**Tech Stack:** Docker Compose, MySQL 8.0, project `.env` configuration.

---

### Task 1: Add Optional MySQL Service

**Files:**
- Modify: `compose.yml`

- [ ] **Step 1: Add the `mysql` service behind a profile**

Add a `mysql` service using `profiles: ["mysql"]`, MySQL image environment variables that reference `DB_NAME`, `DB_USER`, and `DB_PASSWORD`, a separate `MYSQL_ROOT_PASSWORD`, a healthcheck, a named volume, and a read-only mount for `sql/schema.sql` into `/docker-entrypoint-initdb.d`.

- [ ] **Step 2: Keep `app` independent**

Do not add a hard `depends_on: mysql` to `app`, because plain `docker compose up -d` must continue to support external databases.

- [ ] **Step 3: Validate default Compose config**

Run:

```bash
docker compose config
```

Expected: the rendered config includes `app` and does not include an active default dependency on `mysql`.

- [ ] **Step 4: Validate profile Compose config**

Run:

```bash
docker compose --profile mysql config
```

Expected: the rendered config includes both `app` and `mysql`, plus the `mysql_data` volume.

### Task 2: Document Environment and Usage

**Files:**
- Modify: `.env.example`
- Modify: `README.md`

- [ ] **Step 1: Add optional MySQL variables to `.env.example`**

Document `MYSQL_ROOT_PASSWORD` and explain that the MySQL container references `DB_NAME`, `DB_USER`, and `DB_PASSWORD` for first-run initialization.

- [ ] **Step 2: Document bundled MySQL mode**

Add README commands for:

```bash
docker compose up -d
docker compose --profile mysql up -d
docker compose --profile mysql down
docker compose stop mysql
```

- [ ] **Step 3: Explain the profile lifecycle**

State that after using the profile, plain `docker compose up -d` may leave the previous MySQL container running, and users should stop or remove it explicitly when switching back to an external database.
