#!/usr/bin/env bash
set -Eeuo pipefail

readonly APP_NAME="${1:?Usage: $0 <application-name>}"
build_args=(--file docker/Dockerfile --tag "${APP_NAME}:latest")

# CircleCI may create this file for private registry access. Pass it through
# BuildKit without copying credentials into the image or build context layers.
if [[ -f .npmrc ]]; then
  build_args+=(--secret id=npmrc,src=.npmrc)
fi

DOCKER_BUILDKIT=1 docker build "${build_args[@]}" .
