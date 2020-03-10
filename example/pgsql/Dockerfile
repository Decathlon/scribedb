FROM postgres:11


RUN mkdir -p /docker-entrypoint-initdb.d
COPY ./scripts/hr.sql /docker-entrypoint-initdb.d
