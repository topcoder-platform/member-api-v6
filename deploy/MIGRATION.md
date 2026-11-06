# OIDC deployment migration

This change replaces the Auth0 credential broker and SSM-to-shell deployment path with [tc-deploy-scripts v2.0.10](https://github.com/topcoder-platform/tc-deploy-scripts/releases/tag/v2.0.10), fetched by tag and verified against commit `e81a6772a33819c1b57c88a95d04494e59c559b8`. Build and deployment run in separate jobs. The deployer cannot read Parameter Store, Secrets Manager or decrypt KMS data, including through another assumed role. ECS continues resolving the existing runtime secret references using the execution role.

## Development target

The read-only inventory on 2026-11-04 verified the existing dev service and runtime roles recorded in `dev.json`. The PR targets `develop` and includes its current Prisma runtime and dependency fixes. The service has one running task and circuit-breaker rollback enabled. The shared CircleCI OIDC provider and account-wide enhanced ECR scanning already exist. The service deployment role, restricted context, immutable repository tags and container health check are still required before merge. The proposed `dev-healthcheck.json` uses the existing load-balancer path `/v6/members/health` on port 3000.

## Production target

The read-only inventory on 2026-11-03 verified account `409275337247`, cluster `member-api-serverless`, service `member-api-v6`, family `member-api-v6`, and container `member-api-v6`. `prod.json` preserves the observed task/execution roles and names the proposed service-specific deployment role. [prod-baseline.json](https://github.com/topcoder-platform/v6-services-iac/blob/main/services/member-api-v6/deploy/prod-baseline.json) records references only; it contains no parameter values. Do not recreate the service or replace its complete task definition from this baseline.

## Infrastructure prerequisites, applied separately by an administrator

1. In each account, register the CircleCI OIDC issuer `https://oidc.circleci.com/org/947bef5e-1669-415d-b83c-4fc85155b7df` with client ID `947bef5e-1669-415d-b83c-4fc85155b7df`. The current production account has only the legacy Auth0 provider. Do not remove that provider until all its consumers have migrated.
2. For dev, reuse the existing OIDC provider and install the private [iam/dev-role.cloudformation.json](https://github.com/topcoder-platform/v6-services-iac/blob/main/services/member-api-v6/deploy/iam/dev-role.cloudformation.json) with an administrator/IaC identity. Production has its separate `iam/prod-role.cloudformation.json`. It creates the proposed deployment role and applies the same least-privilege policy as a permissions boundary. The trust binds this CircleCI project, repository, checkout branch and configuration branch. It does not grant infrastructure creation to CircleCI. Both templates include `inspector2:ListFindings` and `inspector2:ListCoverage` in the role and boundary, restricted to the deployment region; AWS requires `Resource: "*"` for these scan-metadata list APIs. Secret-access denies remain explicit.
3. Configure ECR repository `member-api-v6` with unconditional `IMMUTABLE` tags and supported scanning. In dev, preserve the existing enhanced continuous scanning; its repository-level `scanOnPush: false` flag does not disable registry-level enhanced scans. Existing tags are not retagged. The release fails for MEDIUM/HIGH/CRITICAL/UNDEFINED findings, failed scans, or a mismatched digest.
4. In the infrastructure that owns the task definition, add the reviewed container check from `dev-healthcheck.json` or `prod-healthcheck.json` as appropriate, keeping all roles, runtime secret references, resources, mounts and sidecars. Verify the health endpoint against the running container in dev first. Preserve circuit-breaker rollback and positive desired capacity. Remove any `RESET_DB` secret binding and plaintext secret environment entries before adoption. Keep the execution role's runtime read permissions scoped to the references that service needs.
5. Create restricted `member-api-v6-dev-deploy` and `member-api-v6-prod-deploy` CircleCI contexts without Auth0/static AWS credentials. Restrict production to the reviewed `master` checkout and configuration ref, authorized operators, and approval job. Remove legacy credentials from project settings too: removing `org-global` alone does not remove project-level variables. Confirm `CIRCLE_OIDC_TOKEN_V3` is issued. Disable secret access for forks and SSH debugging in deployment jobs.

## Development acceptance before merge/release

The verified dev manifest is now committed; service-specific IAM templates and the reference-only baseline remain in the private infrastructure repository. **Keep this PR in draft until the four remaining dev prerequisites above are installed and verified.** Merge to `develop` only after preparation, then monitor the first OIDC deployment. Branches other than `develop` and `master` build only and receive no deployment context.

Local validation fetched the actual v2.0.10 tag in the pinned executor, verified its commit and passed all 82 shared tests. CircleCI configuration validation also passed. A passing PR build validates the application image; it does not establish a successful dev rollout.

Run the shared offline suite, validate the CircleCI config and IAM policies, and test a dev release plus an unhealthy-image rollback. Verify CloudTrail contains no SSM/Secrets Manager calls from the deployment principal. Remove its old broker grants only after migration. Check the runtime secret references and roles stayed unchanged. Production requires the manual approval job and a passing digest/scan/health verification.

The pinned executor is `cimg/aws@sha256:4cf3768db00ddf9bc439b8c84e8a60c11a14edf81c8baf76f74132144f8340d5`. Python/AWS CLI/Buildx/Git compatibility was tested with the shared offline suite. No package installation or service build runs in the deployment job. The Dockerfile retains the existing in-image quality gates. This PR does not deploy, change IAM, rotate credentials, or claim a successful live rollout.

## Infrastructure source

IAM templates and detailed AWS baselines are maintained in the private [v6-services-iac service directory](https://github.com/topcoder-platform/v6-services-iac/tree/main/services/member-api-v6). Provision infrastructure separately with an administrator identity; application CI does not fetch that repository or install IAM. Required application manifests and health-check proposals remain here.
