# member-api-v6

## Shared authentication and bus client (v6 refresh)

This branch uses `tc-core-library-js` **4.0.0-rc.1**. Use its named
`createJwtVerifier` / `createJwtAuthenticator`, `createM2MClient`, and
`createBusApiClient` exports for shared authentication and bus API publishing.
Service-specific roles, scopes, sessions and ownership checks remain in this service.
The former bus wrapper dependency and local JWT cryptography must not be reintroduced.

RSA verification requires an exact configured issuer and audience (`JWT_AUDIENCE`
for client tokens, `AUTH0_AUDIENCE` for machine/API tokens, or this service's explicit
issuer-policy settings). HMAC secrets must be at least 32 bytes; no default secret
is supplied. Expiry is enforced. Machine identity comes from the shared verifier,
not the presence of scopes. Supply real development credentials through the service's
existing environment configuration before deployment.

Bus publishers use `createBusApiClient(config).postEvent(event)` with the existing
event envelope and optional key. Configure `BUSAPI_URL` for v6 and the existing
OAuth credentials/token provider. The client uses bounded HTTPS requests and does
not retry POSTs automatically. Application retry/outbox behavior remains local.

See the [core migration guide](https://github.com/topcoder-platform/tc-core-library-js/blob/v4.0.0-rc.1/MIGRATION.md)
for the API mapping, compatibility requirements, and rollout scope.

## Shared JWT deployment configuration

Development deployments load `/config/common/global-appvar` alongside the service's
application parameters. JWT verification uses the same four settings as the other
v6 services: `VALID_ISSUERS` (JSON array or comma-separated exact issuers),
`AUTH_SECRET` (the existing shared HS256 signing key, at least 32 UTF-8 bytes),
`JWT_AUDIENCE` (the browser application's audience), and `AUTH0_AUDIENCE` (the
machine/API audience). RSA keys are fetched over HTTPS from the trusted issuer's
`/.well-known/jwks.json`. Audience settings must reach the shared verifier; OAuth
client IDs/secrets used for outbound requests are independent of JWT audiences.

The development browser audience is `BXWXUWnilVUPdN01t2Se29Tw2ZYNGZvH`, and the
machine audience is `https://m2m.topcoder-dev.com/`. Use the existing Parameter Store
secret rather than creating a per-service key. Reconcile any service-specific
overrides of these four keys with the global values before deployment. Parameters
are consumed on deployment/task startup; changing Parameter Store alone does not
replace running tasks. Existing role, scope, ownership and active-session checks
continue after JWT verification.

Protected and optional-authentication routes share one authenticator instance,
including its bounded JWKS cache.
