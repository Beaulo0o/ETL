#!/bin/bash
set -e

# Создаём пользователя и базу
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    DROP DATABASE IF EXISTS retail_db;
    DROP USER IF EXISTS etl_user;
    CREATE USER etl_user WITH PASSWORD 'etl_password';
    CREATE DATABASE retail_db;
    GRANT ALL PRIVILEGES ON DATABASE retail_db TO etl_user;
EOSQL

# Создаём схемы
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d retail_db <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION etl_user;
    CREATE SCHEMA IF NOT EXISTS ods AUTHORIZATION etl_user;
    CREATE SCHEMA IF NOT EXISTS mart AUTHORIZATION etl_user;
    CREATE SCHEMA IF NOT EXISTS audit AUTHORIZATION etl_user;
    
    GRANT ALL ON SCHEMA staging TO etl_user;
    GRANT ALL ON SCHEMA ods TO etl_user;
    GRANT ALL ON SCHEMA mart TO etl_user;
    GRANT ALL ON SCHEMA audit TO etl_user;
    
    ALTER DEFAULT PRIVILEGES FOR USER etl_user IN SCHEMA staging GRANT ALL ON TABLES TO etl_user;
    ALTER DEFAULT PRIVILEGES FOR USER etl_user IN SCHEMA ods GRANT ALL ON TABLES TO etl_user;
    ALTER DEFAULT PRIVILEGES FOR USER etl_user IN SCHEMA mart GRANT ALL ON TABLES TO etl_user;
EOSQL

echo "✅ Database initialization completed!"
