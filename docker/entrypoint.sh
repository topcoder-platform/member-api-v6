#!/bin/bash
set -e

echo "Running Prisma migrations..."

if [ -z "$DATABASE_URL" ]; then
  echo "ERROR: DATABASE_URL is not set in environment variables."
  # Optional: print sanitized env vars to help debug
  # env | grep -v "PASSWORD\|SECRET\|KEY\|TOKEN"
else
  echo "DATABASE_URL is present (length: ${#DATABASE_URL})"
fi

./node_modules/.bin/prisma migrate deploy --schema=prisma/schema.prisma

echo "Starting application..."
exec node app.js
