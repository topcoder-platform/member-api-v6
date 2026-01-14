#!/bin/bash
set -e

echo "Running Prisma migrations..."
yarn prisma migrate deploy --schema=prisma/schema.prisma

echo "Starting application..."
exec node app.js
