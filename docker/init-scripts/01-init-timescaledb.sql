-- Initialize TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create a test user with appropriate permissions
CREATE USER test_user WITH PASSWORD 'test_password';
GRANT ALL PRIVILEGES ON DATABASE tsdb TO test_user;
GRANT ALL ON SCHEMA public TO test_user;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO test_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO test_user;

-- Create a sample schema for testing (optional)
CREATE SCHEMA IF NOT EXISTS test_schema;
GRANT ALL ON SCHEMA test_schema TO test_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA test_schema GRANT ALL ON TABLES TO test_user;
