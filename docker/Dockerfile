# syntax=docker/dockerfile:1.7

# Keep a Debian/glibc build because upstream generated Prisma clients ship Debian engines.
FROM node:26.12.1-bookworm-slim AS build-base

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
RUN pnpm run test:dependency-compat
RUN pnpm lint && pnpm build \
    && rm -rf dist/src/scripts \
    && rm -f "dist/docs/Member API.postman_collection.json" \
        dist/tsconfig.build.tsbuildinfo \
    && find dist -type f -name '*.d.ts' -delete
RUN sh docker/prune-internal-api-packages.sh \
    && rm docker/prune-internal-api-packages.sh \
    && pnpm prune --prod \
    && find node_modules -type f \
        \( -name yarn.lock -o -name package-lock.json -o -name pnpm-lock.yaml \
        -o -name npm-shrinkwrap.json \) -delete \
    && rm -f pnpm-lock.yaml \
        node_modules/.modules.yaml \
        node_modules/.package-map.json \
        node_modules/.pnpm-workspace-state-v1.json \
        node_modules/.pnpm/lock.yaml

# Keep the hardened Wolfi/glibc runtime; Prisma's Debian engines also need the
# OpenSSL 3 shared libraries, which newer runtime images no longer include.
FROM cgr.dev/chainguard/node:latest@sha256:6cb70d6b2a324e491263949ea9f58938ed19031ef478e0bdb0b1810f96f8b400 AS runtime

# Use the patched libraries from the build stage. The generic library directory
# lets both the dynamic linker and Prisma detect OpenSSL 3 without shell tools.
COPY --from=build-base /usr/lib/*-linux-gnu/libssl.so.3 /usr/lib/*-linux-gnu/libcrypto.so.3 /usr/lib/

# The pinned runtime includes maintenance utilities that the service does not use.
# Remove every executable except Node, plus the global npm/node-gyp payload, so
# the final image has no shell, package manager, or build toolchain.
USER 0
RUN ["/usr/bin/node", "-e", "const fs=require('node:fs'); for (const entry of fs.readdirSync('/usr/bin')) if (entry !== 'node') fs.rmSync('/usr/bin/'+entry,{force:true,recursive:true}); fs.rmSync('/usr/lib/node_modules',{force:true,recursive:true})"]

WORKDIR /member-api-v6

ENV NODE_OPTIONS="--enable-source-maps"

# Copy only compiled application code, production dependencies, static runtime
# assets, configuration, and the Prisma CLI migration inputs. The Chainguard
# runtime itself contains neither npm nor pnpm, and no source tree, development
# dependency, repository metadata, or lockfile is copied from the build stage.
COPY --from=build --chown=65532:65532 /member-api-v6/node_modules ./node_modules
COPY --from=build --chown=65532:65532 /member-api-v6/dist ./dist
COPY --from=build --chown=65532:65532 /member-api-v6/config/default.js ./config/default.js
COPY --from=build --chown=65532:65532 /member-api-v6/prisma/migrations ./prisma/migrations
COPY --from=build --chown=65532:65532 /member-api-v6/prisma/schema.prisma ./prisma/schema.prisma
COPY --from=build --chown=65532:65532 /member-api-v6/prisma.config.ts ./prisma.config.ts
COPY --from=build --chown=65532:65532 /member-api-v6/docker/entrypoint.js ./docker/entrypoint.js

USER 65532
# Exercise the migration engine in the final runtime, without a database or
# network access, so missing engines/shared libraries fail the image build.
RUN --network=none --mount=type=bind,source=docker/check-prisma-runtime.js,target=/tmp/check-prisma-runtime.js \
    ["/usr/bin/node", "/tmp/check-prisma-runtime.js"]
ENTRYPOINT ["/usr/bin/node", "/member-api-v6/docker/entrypoint.js"]
