#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
until pg_isready -h $DHIS2_DATABASE_HOST -U $DHIS2_DATABASE_USERNAME -d $DHIS2_DATABASE_NAME; do
  echo "Waiting for PostgreSQL..."
  sleep 2
done

# Check if database is empty (needs Sierra Leone data)
TABLE_COUNT=$(psql -h $DHIS2_DATABASE_HOST -U $DHIS2_DATABASE_USERNAME -d $DHIS2_DATABASE_NAME -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public';" 2>/dev/null || echo "0")

if [ "$TABLE_COUNT" -lt 10 ]; then
  echo "Database appears empty. Downloading Sierra Leone demo data..."
  
  # Download Sierra Leone database dump
  if [ ! -f /tmp/sierra-leone.sql.gz ]; then
    echo "Downloading Sierra Leone database from: $DHIS2_DB_DUMP_URL"
    wget -O /tmp/sierra-leone.sql.gz "$DHIS2_DB_DUMP_URL"
  fi
  
  echo "Importing Sierra Leone database..."
  gunzip -c /tmp/sierra-leone.sql.gz | psql -h $DHIS2_DATABASE_HOST -U $DHIS2_DATABASE_USERNAME -d $DHIS2_DATABASE_NAME
  
  echo "Sierra Leone database import completed!"
else
  echo "Database already contains data (${TABLE_COUNT} tables). Skipping import."
fi

echo "Starting DHIS2..."
exec "$@"