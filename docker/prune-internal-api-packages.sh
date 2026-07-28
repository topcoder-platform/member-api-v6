#!/bin/sh

set -eu

# Retain only the generated Prisma client imported from an internal API package.
#
# @param $1 pnpm package alias under node_modules/@topcoder
# @param $2 generated client directory to retain under the package's packages folder
# @returns no output; removes unused internal API source and package metadata in place
# @throws exits non-zero when the alias does not resolve inside this install or the client is absent
prune_internal_api_package() {
  package_alias=$1
  client_directory=$2
  package_path=$(readlink -f "$package_alias")

  case "$package_path" in
    "$PWD"/node_modules/.pnpm/*/node_modules/*) ;;
    *)
      echo "Refusing to prune unexpected package path: $package_path" >&2
      exit 1
      ;;
  esac

  if [ ! -d "$package_path/packages/$client_directory" ]; then
    echo "Generated Prisma client is missing: $package_path/packages/$client_directory" >&2
    exit 1
  fi

  find "$package_path" -mindepth 1 -maxdepth 1 ! -name packages -exec rm -rf -- {} +
  find "$package_path/packages" -mindepth 1 -maxdepth 1 ! -name "$client_directory" -exec rm -rf -- {} +
}

prune_internal_api_package node_modules/@topcoder/challenge-api-v6 challenge-prisma-client
prune_internal_api_package node_modules/@topcoder/engagements-api-v6 engagements-prisma-client
prune_internal_api_package node_modules/@topcoder/learning-paths-api academy-prisma-client
prune_internal_api_package node_modules/@topcoder/resource-api-v6 resources-prisma-client
prune_internal_api_package node_modules/@topcoder/standardized-skills-api skills-prisma-client
prune_internal_api_package node_modules/@topcoder/tc-finance-api finance-prisma-client
prune_internal_api_package node_modules/@topcoder/tc-identity-service identity-prisma-client
