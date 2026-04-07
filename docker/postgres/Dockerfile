# Custom Postgres image: apache/age + PostGIS 3
# apache/age is based on postgres:16-bookworm, so we can apt-install postgis.
FROM apache/age:release_PG16_1.5.0

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-16-postgis-3 \
        postgresql-16-postgis-3-scripts \
    && rm -rf /var/lib/apt/lists/*

USER postgres
