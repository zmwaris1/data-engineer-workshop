docker commands to start up a postgres container and pgadmin for data ingestion and verification.

`docker network create pg-network`

```
docker run -it \
  -e POSTGRES_USER="root" \
 -e POSTGRES_PASSWORD="root" \
 -e POSTGRES_DB="ny_taxi" \
 -v $(pwd):/var/lib/postgresql/data \
 -p 5432:5432 \
 --network=pg-network \
 --name pg-database \
 postgres:13```

```

```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
 -e PGADMIN_DEFAULT_PASSWORD="root" \
 -p 8080:80 \
 --network=pg-network \
 --name pgadmin-2 \
 dpage/pgadmin4
```

Similarly we can use docker-compose to serve the same purpose:

Command for docker-compose:

To start the containers:
`docker compose up`

To stop the containers:
`docker compose down`