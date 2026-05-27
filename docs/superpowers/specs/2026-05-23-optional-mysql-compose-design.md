# Optional MySQL Compose Integration Design

## Goal

Make MySQL an optional Docker Compose integration for local or self-contained deployments while keeping the current external database workflow unchanged.

## Approach

Add a `mysql` service to `compose.yml` behind the Docker Compose `mysql` profile. The default `docker compose up -d` path continues to start only the `app` service, so existing `.env` files that point at remote or host MySQL instances keep working.

When users want the bundled database, they run `docker compose --profile mysql up -d` and set the application database variables to target the Compose service name:

```env
DB_HOST=mysql
DB_PORT=3306
DB_USER=coinx
DB_PASSWORD=coinx_password
DB_NAME=coinx
```

The MySQL service uses those same `DB_NAME`, `DB_USER`, and `DB_PASSWORD` values for first-run database/user initialization. `MYSQL_ROOT_PASSWORD` remains separate because the application should not connect as root.

## Compose Services

- `app`: unchanged default service. It reads `.env`, exposes `WEB_PORT`, and can connect to either an external database or the optional Compose MySQL service.
- `mysql`: optional service using the `mysql` profile. It stores data in a named volume, initializes the application database/user from `DB_*` variables, keeps root password in `MYSQL_ROOT_PASSWORD`, and runs `sql/schema.sql` on first empty-volume startup.

## Data Persistence

The MySQL service uses a named volume, `mysql_data`, mounted at `/var/lib/mysql`. Container updates or restarts preserve database data. Users must explicitly run a volume-removing command, such as `docker compose down -v`, to delete the database files. Because official MySQL init scripts only run when the data directory is empty, later schema changes still need a migration or manual import path.

## Operational Notes

After starting with `--profile mysql`, running plain `docker compose up -d` later does not necessarily remove the previously created MySQL container. To switch back to an external database, users should update `DB_*` values and stop the optional service with either:

```bash
docker compose --profile mysql down
```

or:

```bash
docker compose stop mysql
```

## Testing

Validate the default and optional profile configurations with:

```bash
docker compose config
docker compose --profile mysql config
```
