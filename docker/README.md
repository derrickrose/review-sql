# Run pgadmin and postgres on docker

## create a docker network

```commandline
docker network create pgnet
```

## run postgres on docker

```shell
docker run -d --name postgres -p 5432:5432  -e POSTGRES_USER=frils -e POSTGRES_PASSWORD=frils -e POSTGRES_DB=review --network pgnet postgres
```

## run pgadmin on docker

Note:

- I had to twick the tad from dpage/pgadmin4 to pgadmin:latest for smoother run (check docker tag command)

```shell
docker run -d --name pgadmin -e 'PGADMIN_DEFAULT_EMAIL=randofrils@gmail.com' -e PGADMIN_DEFAULT_PASSWORD=frils -p 80:80 -p 443:443 --network pgnet pgadmin:latest
```

## to inspect pgadmin container

```commandline
docker inspect pgadmin
```

```sql

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name;
```