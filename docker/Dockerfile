# syntax=docker/dockerfile:1.7

# Keep a Debian/glibc build because upstream generated Prisma clients ship Debian engines.
FROM node:26.5.0-bookworm-slim AS build-base

WORKDIR /member-api-v6
RUN apt-get update \
    && apt-get upgrade -y --no-install-recommends \
    && apt-get install -y --no-install-recommends git openssh-client openssl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && npm install --global pnpm@11.15.1

FROM build-base AS build

COPY . .
RUN --mount=type=secret,id=npmrc,target=/root/.npmrc,required=false \
    pnpm install --frozen-lockfile
RUN pnpm lint && pnpm build
RUN sh docker/prune-internal-api-packages.sh \
    && rm docker/prune-internal-api-packages.sh

# The pinned Wolfi/glibc runtime supports the checked-in Debian/OpenSSL 3 Prisma
# engines without shipping a shell, package manager, or build toolchain.
FROM cgr.dev/chainguard/node:latest@sha256:c002402b355201714dfbeb3b1a7ca99152cccced4b334496db2442a3a9bf4883 AS runtime

WORKDIR /member-api-v6

ENV NODE_OPTIONS="--enable-source-maps"

# Retain the validated install because startup migrations and maintenance scripts
# require Prisma CLI, ts-node, schemas, and generated clients at runtime.
COPY --from=build --chown=65532:65532 /member-api-v6 /member-api-v6

USER 65532
ENTRYPOINT ["/usr/bin/node", "/member-api-v6/docker/entrypoint.js"]
