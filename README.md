# LEARN SQL

https://mode.com/sql-tutorial/introduction-to-sql

postgres 
https://www.udemy.com/course/postgresqlmasterclass/learn/lecture/22230284?start=0#overview

mysql
https://www.udemy.com/course/the-ultimate-mysql-bootcamp-go-from-sql-beginner-to-expert/learn/lecture/34511736#overview
https://www.udemy.com/course/mysql-and-sql-from-beginner-to-advanced/learn/lecture/5074612?start=0#overview
https://www.udemy.com/course/the-ultimate-mysql-bootcamp-go-from-sql-beginner-to-expert/learn/lecture/34511736?start=0#overview

analytics 
https://www.udemy.com/course/advanced-sql-mysql-for-analytics-business-intelligence/learn/lecture/16322508
https://www.udemy.com/course/the-advanced-sql-course-2021/learn/lecture/27230694?start=15#overview
https://www.udemy.com/course/the-advanced-sql-course-2021/learn/lecture/27230694?start=15#overview

# practice here

https://app.mode.com/editor/frils/

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

- I had to twick the image tag from dpage/pgadmin4 to pgadmin:latest for smoother run (check docker tag command)

```shell
docker run -d --name pgadmin -e 'PGADMIN_DEFAULT_EMAIL=randofrils@gmail.com' -e PGADMIN_DEFAULT_PASSWORD=frils -p 80:80 -p 443:443 --network pgnet pgadmin:latest
```

## to inspect pgadmin container

```commandline
docker inspect pgadmin
```

## to run both using the docker-compose file

Prerequisites : being inside the folder containing the yaml file then run the following command

```commandline
docker compose up 
```

then got to http://localhost:8180/browser/

```sql

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name;
```