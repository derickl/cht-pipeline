#!/bin/bash
set -e

for POSTGRES_TABLE in $POSTGRES_TABLES
do

echo Creating $POSTGRES_TABLE

## DO NOT put any additional SQL here
#
#  Put all SQL into DBT. Bootstrapping should be the absolute minimum
#
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS $POSTGRES_SCHEMA AUTHORIZATION $POSTGRES_USER;
    CREATE TABLE $POSTGRES_SCHEMA.$POSTGRES_TABLE (
        "@version" TEXT,
        "@timestamp" TIMESTAMP,
        "_id" TEXT,
        "_rev" TEXT,
        doc jsonb,
        doc_as_upsert BOOLEAN,
        UNIQUE ("_id", "_rev")
    );
EOSQL

done