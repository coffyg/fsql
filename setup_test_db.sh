#!/bin/bash
#
# This script sets up a PostgreSQL database for fsql tests.
# It can either:
# 1. Use an existing PostgreSQL instance (if available)
# 2. Create a Docker container with PostgreSQL
# 3. Create necessary tables and schema
#
# Usage: ./setup_test_db.sh [--dbuser=USER] [--dbpass=PASS] [--dbname=NAME] [--dbhost=HOST] [--dbport=PORT]
#
# If using an existing PostgreSQL instance with a superuser, set:
# PGPASSWORD=postgres ./setup_test_db.sh
#

# Stop on errors
set -e

# Default values
DB_USER="test_user"
DB_PASS="test_password"
DB_NAME="test_db"
DB_HOST="localhost"
DB_PORT="5433"  # Use 5433 instead of 5432 to avoid conflicts

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --dbuser=*)
      DB_USER="${1#*=}"
      shift
      ;;
    --dbpass=*)
      DB_PASS="${1#*=}"
      shift
      ;;
    --dbname=*)
      DB_NAME="${1#*=}"
      shift
      ;;
    --dbhost=*)
      DB_HOST="${1#*=}"
      shift
      ;;
    --dbport=*)
      DB_PORT="${1#*=}"
      shift
      ;;
    *)
      echo "Unknown parameter: $1"
      exit 1
      ;;
  esac
done

# Setup PostgreSQL for testing
echo "Setting up test database ${DB_NAME} on ${DB_HOST}:${DB_PORT}..."

# Check if we can connect to PostgreSQL
if ! pg_isready -h $DB_HOST -p $DB_PORT > /dev/null 2>&1; then
  echo "PostgreSQL is not running on $DB_HOST:$DB_PORT. Starting PostgreSQL in Docker..."
  
  # Check if container already exists
  if docker ps -a | grep -q postgres_fsql_test; then
    echo "Container already exists, stopping and removing..."
    docker stop postgres_fsql_test || true
    docker rm postgres_fsql_test || true
  fi
  
  # Run PostgreSQL container
  docker run --name postgres_fsql_test -e POSTGRES_USER=$DB_USER \
    -e POSTGRES_PASSWORD=$DB_PASS -e POSTGRES_DB=$DB_NAME \
    -p $DB_PORT:5432 -d postgres:14

  # Wait for PostgreSQL to start
  echo "Waiting for PostgreSQL to start..."
  for i in {1..30}; do
    if pg_isready -h $DB_HOST -p $DB_PORT > /dev/null 2>&1; then
      echo "PostgreSQL is now running."
      break
    fi
    echo -n "."
    sleep 1
  done
  
  # Create extensions
  PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"
  
  # Create test tables
  echo "Creating test tables..."
  
  # Create schema from fsql_test.sql
  if [ -f "fsql_test.sql" ]; then
    echo "Using schema from fsql_test.sql..."
    PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f fsql_test.sql
  else
    # Create tables manually if fsql_test.sql doesn't exist
    echo "Creating tables manually..."
    
    # AI Model table
    PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
      CREATE TABLE IF NOT EXISTS ai_model (
        uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        key TEXT NOT NULL,
        name TEXT,
        description TEXT,
        type TEXT NOT NULL,
        provider TEXT NOT NULL,
        settings JSONB,
        default_negative_prompt TEXT
      );"
    
    # Realm table
    PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
      CREATE TABLE IF NOT EXISTS realm (
        uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        name TEXT NOT NULL
      );"
    
    # Website table
    PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
      CREATE TABLE IF NOT EXISTS website (
        uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        domain TEXT NOT NULL,
        realm_uuid UUID REFERENCES realm(uuid)
      );"
  fi
  
  echo "Test database setup complete!"
else
  echo "PostgreSQL is already running on $DB_HOST:$DB_PORT"
  
  # Check if we can connect with the specified user
  if ! PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT 1" > /dev/null 2>&1; then
    echo "Cannot connect to PostgreSQL with specified credentials. Creating user and database..."
    
    # Try to create the database and user with superuser credentials
    if [ -z "$PGPASSWORD" ]; then
      echo "Error: PGPASSWORD environment variable not set. Please set it to the superuser password."
      echo "Example: PGPASSWORD=postgres ./setup_test_db.sh"
      exit 1
    fi
    
    # Drop database if it exists
    psql -h $DB_HOST -p $DB_PORT -U postgres -c "DROP DATABASE IF EXISTS $DB_NAME;" 2>/dev/null || true
    # Drop role if it exists
    psql -h $DB_HOST -p $DB_PORT -U postgres -c "DROP ROLE IF EXISTS $DB_USER;" 2>/dev/null || true
    
    # Create user and database
    psql -h $DB_HOST -p $DB_PORT -U postgres -c "CREATE ROLE $DB_USER WITH LOGIN PASSWORD '$DB_PASS';"
    psql -h $DB_HOST -p $DB_PORT -U postgres -c "CREATE DATABASE $DB_NAME WITH OWNER $DB_USER;"
    
    # Create extension
    psql -h $DB_HOST -p $DB_PORT -U postgres -d $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"
    
    # Create tables
    echo "Creating test tables..."
    
    # Create schema from fsql_test.sql
    if [ -f "fsql_test.sql" ]; then
      echo "Using schema from fsql_test.sql..."
      PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f fsql_test.sql
    else
      # Create tables manually if fsql_test.sql doesn't exist
      PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        CREATE TABLE IF NOT EXISTS ai_model (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          key TEXT NOT NULL,
          name TEXT,
          description TEXT,
          type TEXT NOT NULL,
          provider TEXT NOT NULL,
          settings JSONB,
          default_negative_prompt TEXT
        );"
      
      PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        CREATE TABLE IF NOT EXISTS realm (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
          updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
          name TEXT NOT NULL
        );"
      
      PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
        CREATE TABLE IF NOT EXISTS website (
          uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
          created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
          updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
          domain TEXT NOT NULL,
          realm_uuid UUID REFERENCES realm(uuid)
        );"
    fi
  else
    echo "Connection successful! Tables should already exist."
  fi
fi

echo
echo "Database setup complete. Now you can run:"
echo "./test.sh"