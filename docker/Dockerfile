# syntax=docker/dockerfile:1.7

# Keep a Debian/glibc base because upstream generated Prisma clients ship Debian engines.
FROM node:22.22.0 AS base

WORKDIR /member-api-v6
RUN npm install --global pnpm@10.33.2

FROM base AS build

COPY . .
RUN --mount=type=secret,id=npmrc,target=/root/.npmrc,required=false \
    pnpm install --frozen-lockfile
RUN pnpm lint && pnpm build

FROM base AS runtime

ENV NODE_OPTIONS="--enable-source-maps"

# Retain the validated install because startup migrations and maintenance scripts
# require Prisma CLI, ts-node, schemas, and generated clients at runtime.
COPY --from=build --chown=node:node /member-api-v6 /member-api-v6
COPY --from=build /member-api-v6/docker/entrypoint.sh /entrypoint.sh
RUN chmod 0555 /entrypoint.sh

USER node
ENTRYPOINT ["/entrypoint.sh"]
