# E2EE Content Prototype

Status: experimental feasibility contract for DR-035. Not a negotiated Hub capability or a production protocol. Neither v1 nor v2 accepts these objects in place of its payloads. The implementation is isolated under `experimental/e2ee` and `client-ts/src/experimental`; it is not exported from the client entry point.

## Scope

Test Go 1.25-compatible standard cryptography against Web Crypto without introducing dependencies. This contract covers authenticated encrypted content and an isolated endpoint command journal, not pairing, public-key trust establishment, protected key storage, authenticated membership distribution, native cryptography or integration. Authenticated key wrapping is implemented as described below. Passing these tests does not establish E2EE for AgentWharf.

## Encoding

Context is the exact ordered JSON string array:

`["agentwharf.e2ee.prototype.v1", scope, session, sender, key_id, message_id, type]`

Every variable is 1..128 ASCII characters from `[A-Za-z0-9_.:/-]`. Scope is exactly `command`, `event` or `launch`. Context is supplied by the caller from independently verified local expectations, never accepted merely from the received message. Session and message IDs must be stable before encryption. `key_id` identifies a unique session key epoch; key lifecycle is outside this prototype.

Content is at most 32768 bytes. AES-256-GCM uses a fresh random 12-byte nonce and a 16-byte tag; context bytes are additional authenticated data. One key must encrypt at most 2^20 new objects before rotation (random-nonce collision probability remains approximately 2^-57 at this bound). That budget must be enforced across all senders by the eventual lifecycle implementation; this stateless primitive does not enforce it. Retries reuse the original sealed object.

Ed25519 signs the concatenation of context bytes, nonce and ciphertext-with-tag. The signature authenticates the sending endpoint independently of the shared decryption key. The verification key must come from authenticated local membership, not the envelope or platform directory. A valid signature does not establish the sender's control role or freshness.

The sealed JSON object has exactly `nonce`, `ciphertext`, `signature`, each canonical unpadded base64url. Unknown members, noncanonical encoding, nonce/signature length errors and oversized content are rejected. Go `DecodeEnvelope` also rejects duplicate JSON members. The TypeScript API currently takes an already parsed object; duplicate-member rejection must be supplied by the eventual wire decoder and is not proven by this prototype. The signature is verified before attempting decryption. Failure returns a content-free error.

## Proposed Protocol Container (not negotiated)

`protocol.EncryptedContentPayload` adds `version=1`, `scope`, `key_id`, `sender`, `message_id`, and `type` to the three envelope fields. It contains exactly nine fields, is at most 48 KiB, and uses the context identifier grammar above. The containing Session ID is supplied by the eventual outer frame and authenticated by the endpoint context. The decoder rejects duplicate, missing, unknown, null and trailing fields and verifies canonical unpadded base64url and decoded byte bounds. It does not verify signatures or establish any trust. This is a proposed encoding, not permission to use it in a v1/v2 `session.send`; Hub behavior remains unchanged until explicit capability and durable session-mode admission exist. A generic payload `version` field must never implicitly enable encryption.

## Authenticated Session-Key Wrapping

`keywrap.go` and `client-ts/src/experimental/keywrap.ts` use HPKE Auth mode (RFC 9180): DHKEM(P-256, HKDF-SHA256), HKDF-SHA256, AES-256-GCM. Only 32-byte content keys are wrapped. The receiving and sending public keys must be obtained from authenticated local membership; this operation does not establish initial trust.

Context is canonical JSON `["agentwharf.e2ee.keywrap.v1", session, key_id, sender, recipient]`, with the identifier grammar above. SHA-256 of these bytes is HPKE `info`, and the full bytes are AEAD AAD. Each wrap uses a fresh sender context. The output has `enc` (65 decoded bytes) and `ciphertext` (48 decoded bytes), both canonical unpadded base64url. Wrong context, recipient or authenticated sender fails with a content-free error. This does not by itself enforce membership freshness, epoch rotation or replay policy.

Dependencies are pinned: Go `github.com/cloudflare/circl v1.6.5` (BSD-3-Clause plus bundled notices), TS `@hpke/core 1.9.0` with lockfile `@hpke/common 1.10.1` (MIT). Go 1.25 has no standard-library HPKE, and Web Crypto exposes primitives rather than HPKE; these libraries avoid inventing key-schedule/encapsulation code or upgrading all runtime toolchains. CIRCL raises transitive x/crypto, x/sync and x/text versions, recorded in go.mod/go.sum; protocol and Hub tests must run against that resolved graph. Native support is still unverified. npm audit currently reports zero known vulnerabilities; that is not an independent cryptographic review. Libraries are linked in-process, with no service or network call. Do not load browser libraries from a CDN.

Cross-language tests execute both directions with public synthetic P-256 fixture keys and test changed context, wrong sender/recipient, malformed encoding and invalid content-key length. These tests verify the actual APIs despite CIRCL's historical package comment referencing an HPKE draft. They do not replace an external security review or published standard test vectors.

## Initial Pairing Primitive (not an enrollment API)

The local machine generates a fresh P-256 invitation key, 256-bit secret and 128-bit random invitation ID, valid for five minutes. The local-only offer has exactly `version=1`, `id`, `machine`, `public_key`, `secret`, `expires_at`. The complete offer is conveyed through the user's local scan/paste action; the secret must never be sent to platform REST, a URL, logs or telemetry. This is a high-entropy out-of-band secret, not a short code or password, and cannot be substituted with the existing server-known device code.

Client identity is the exact canonical object `{device,signing_key,wrapping_key}` (Ed25519 public key, P-256 public key). The encrypted plaintext is `{identity,proof}`, with a 64-byte base64url Ed25519 proof over `utf8("agentwharf.e2ee.pair.proof.v1") || 0x00 || context || 0x00 || canonical_identity`. The sender checks that its signing key matches the claimed public key; the recipient independently verifies the proof before committing enrollment. The proof binds the invitation context as well as both identity public keys. HPKE PSK mode encrypts it to the invitation public key with the offer secret and invitation ID as PSK ID. Context/AAD is canonical JSON `["agentwharf.e2ee.pair.v1",id,machine,public_key,expires_at]`; SHA-256 of those bytes is HPKE info. Go and TS use identical ordering and byte encoding. The relayed request only contains `enc` and `ciphertext`. Recipient acceptance validates identity shape and the public wrapping key, expiration and one-time consumption under a mutex. Invalid ciphertext does not consume the offer; restart cancels it. Production must rate-limit attempts and atomically commit enrollment plus consumption.

This primitive authenticates possession of the locally transferred secret. It does not yet authenticate account binding, persist enrollment itself, or render the scan/paste UI. Signing-key possession is verified as above. The client must not display paired success based on an untrusted relay acknowledgement. Those are explicit prerequisites for wiring it into the real pairing flow. There is no short-secret fallback.

`AcceptCommitted` holds invitation admission serialized while invoking a trusted local, bounded, idempotent enrollment callback. Only after successful commit does it return `{confirmation}`: HMAC-SHA256 over the canonical identity using a 32-byte HPKE exported key with exporter context `agentwharf.e2ee.pair.confirm.v1`. TS `preparePairing` retains a non-exportable Web Crypto verification key and provides `verifyReceipt`; it does not expose that key in the request. The exporter binds the HPKE encapsulation and invitation context, so another request's receipt cannot confirm this one. This is endpoint key confirmation, not independently verifiable evidence of a database commit or an account authorization proof.

Exact request retries after successful commit return the original receipt without invoking the callback again until invitation expiry. A different request is rejected. Commit errors consume/fence the invitation and emit no success receipt because the outcome may be ambiguous. The invitation's direct receipt cache is in-memory; the `DeviceRegistry` integration below additionally persists successful enrollment/receipt pairs for recovery. Unconsumed invitations are cancelled on process restart. Production pairing routing and account authorization remain required before shipping. The legacy `Accept` helper is only a decryption test convenience, never a production authorization API.

Tests execute Go invitation -> TS encrypted identity -> Go acceptance, plus wrong-secret and machine-substitution rejection; Go tests prove expiration and only one acceptance under concurrent requests. These are primitive tests, not user-flow acceptance.

## Domain Content Packet (not negotiated)

`ContentPacket` is the proposed endpoint wrapper `{version:1,public,encrypted}`. Its protected plaintext contains `{public,payload}`. The outer public projection must equal the authenticated inner projection before the original domain payload is delivered. Current projection fields are closed by event type: Session state, message role, permission request ID and decision; arbitrary Provider fields, titles, paths, tool input/output and summaries stay inside `payload`. Unknown projection fields are rejected. This prototype does not yet define all settings/run/file-reference projections and is not accepted as a production Hub capability.

Go protected-container parsing rejects duplicate/trailing fields while preserving payload JSON rather than requiring Go-specific escaping or field ordering. TS uses a bounded strict JSON parser for protected plaintext (48 KiB, depth 64, 8192 nodes), rejects nested/escaped duplicate members, and uses own data properties without triggering the `__proto__` setter. Both endpoints derive the projection from the domain payload and reject inconsistencies even when signed by a valid sender. Typed domain decoding of other fields remains an integration responsibility. Tests cover intact payload recovery, substituted state/request ID, unexpected metadata, version rejection and foreign JSON encoding. Runtime mode binding, endpoint sender role checks, size-budget reconciliation and actual SDK/Adapter hooks remain outstanding.

## Local Identity Persistence

`LoadOrCreateIdentity` on Linux/macOS persists a versioned device ID, Ed25519 seed and P-256 private key in a dedicated local directory, not `machine.json`. The directory is opened without following its final symlink, must belong to the current UID with mode 0700, and lives below a caller-trusted parent. All subsequent opens are directory-FD relative with no-follow and close-on-exec. Regular files must be current-UID-owned, mode 0600 and single-linked. The decoder is bounded and canonical; corrupt/unknown data never triggers automatic key replacement.

An advisory lock serializes goroutines and separate processes. Lock open/create and acquisition retry only bounded contention within a five-second context. New identity data is written to an exclusive staging file, synced, linked without replacing an existing identity, unlinked from staging and directory-synced before success. A leftover staging file or uncertain publication fails closed; automatic interrupted-initialization recovery is not implemented yet. Linux is compile-tested only unless separately stated; macOS real-filesystem tests cover reload, twelve goroutines, four subprocesses, symlinks, hardlinks, permissions, corruption, staging leftovers and cancellation. Non-Linux/macOS platforms return an explicit unavailable error until a native storage backend exists.

This protects against other OS users, not host root, malicious same-UID Provider code or filesystem rollback. It is not encrypted-at-rest keychain storage. Provider execution must not receive key material in environment/arguments or inherited FDs; lifecycle integration is still pending. Keys are not currently generated by a production daemon path.

## Local Session-Key Vault

`SessionKeyVault` generates random 32-byte content keys and stores only HPKE Auth self-wrapped copies in the endpoint SQLite database, indexed by local device/Session/key ID. Concurrent create-only requests read back the durable winning key, not their losing random candidate. Reopen uses the persistent local identity to unwrap it. Corrupt or missing records are not silently replaced; failed storage returns no key. A key epoch's lifecycle and retention remain caller-owned and are not yet connected to production.

Key distribution requires the same endpoint database's immutable DeviceRegistry entry plus a matching current `e2ee_local_sessions`/`e2ee_local_grants` row and signing key. Pairing alone does not grant Session read access. View grants can receive keys without becoming control grants. A SQLite writer transaction serializes the grant/epoch check and wrapping with membership changes. Already-distributed historical keys cannot be recalled. Real SQLite tests cover concurrent creation, close/reopen, other-device denial, wrapping/decryption, enrollment-without-grant rejection, old-epoch rejection after rotation, corruption and insert failure. Session creation and key creation are not yet one atomic production operation; bounded key retention/cleanup still needs integration.

## Local Device Registry

`DeviceRegistry` is a local SQLite registry constructed with a locally authorized machine/account binding, never relay-supplied identity. `Enroll` uses the invitation verification and atomically inserts immutable device public keys with the request fingerprint, confirmation and expiry. Same-ID different-key enrollment is rejected; it never silently replaces a trusted endpoint. Registry enrollment does not grant Session control. `Device` validates stored public-key shape on read. `RecoverReceipt` requires the exact original encrypted request and same local machine/account binding, including on an in-memory invitation retry; it returns no expired receipt.

The registry stores no invitation secrets, content keys or content. It caps devices at 32 and retained receipts at 128 per binding, deletes at most 128 expired receipts per enrollment transaction, and uses five-second database contexts. These are experimental bounds pending product integration. Real SQLite tests close/reopen the database, recover the receipt and identity, reject cross-account lookup and key replacement, and inject receipt insertion failure to prove device/receipt atomic rollback. Private directory provisioning, authenticated account binding, revocation policy, native storage and transport routes are not implemented by this registry.

## Endpoint Command Journal

`experimental/e2ee.CommandJournal` accepts an endpoint-owned SQLite database. It must never use the Hub EventStore or a remotely writable database. The caller is responsible for private directory ownership, durable SQLite settings, trusted enrollment and serializing Provider execution with membership changes. The experimental tables contain only Session/device/key identifiers, public verification keys, epoch, command fingerprints, bounded counts and execution states; no content or secret keys are stored.

`ReplaceGrants` is a local trusted API, not a network operation. It accepts at most 32 unique device grants with independent control permission. New membership advances an exact expected epoch and introduces a never-before-used key ID. This does not itself generate or distribute a fresh key; key-ID uniqueness cannot prove key-byte uniqueness. The authenticated pairing/membership layer must supply that guarantee. Replay and epoch records must not be deleted while old signed commands can still be accepted.

`Admit` verifies scope, exact current session/key, current device control permission, signature and decryption before committing a command reservation. Its SQLite writer transaction serializes admission and membership changes across connections. Only the first committed reservation returns plaintext and `Execute=true`; same-ID changed envelopes conflict. Retries reuse the exact envelope. A claimed command seen again returns `outcome_unknown`, including after restart, and cannot execute automatically again. This is at-most-once admission, not exactly-once Provider execution or proof of completion.

Only the local executor calls `Finish`; platform receipts never do. Terminal outcomes are idempotent but cannot overwrite one another. Storage errors, cancellation and failed transactions return no execution permission or plaintext. Calls have a five-second context bound; the database owner must configure bounded driver busy waits. Each Session accepts at most 4096 new command IDs per key epoch, then fails explicitly; an epoch transition renews the budget (see "Coordinated seal-budget rotation"). This is an experimental resource bound, not an approved production Session limit. The journal does not create Hub seq values.

Residual prerequisites before runtime integration: authenticated membership transition delivery; private database provisioning and corruption/rollback detection; fresh key material; serialized membership/execution lifecycle; signed command expiration/admission freshness; cleanup after permanently fencing a session. A command admitted before a concurrent revocation may still execute unless the eventual local executor provides that serialization. None of these are advertised as solved by this journal.

## Endpoint Event Sealing Budget

`SessionKeyVault.SealEvent` loads the local wrapped key, validates the event projection, checks the current Session epoch and commits a usage reservation before encryption. Its local per-key bound is 16384 seals. It accepts only event scope signed by this machine identity. Failed/canceled attempts after reservation consume their allocation; no ciphertext is returned on storage failure. Old epoch keys remain available for history decryption but cannot be used for new events through this API. It does not allocate seq or implement an event outbox; retries must retain the original packet. Tests cover reopen near exhaustion, two independent database connections competing for the last reservation, rotation and injected reservation failure. The machine runtime seals at `SealRotationThreshold`-driven rotation; see "Coordinated seal-budget rotation" below.

This is not yet the complete aggregate 2^20 budget: trusted-client persistence/enforcement and a bound on distinct writers over an epoch's entire membership history remain outstanding. Raw `Seal`/`SealPacket` remain unbudgeted primitives and must not be production sending paths. Retry outbox integration remains outstanding.

## Coordinated seal-budget rotation

The machine endpoint owns the event-seal path, so it also owns budget
recovery. `SessionKeyVault.SealsUsed` reports the current epoch's durable
usage, and `SessionKeyVault.RotateSessionKeysForBudget` rekeys the session at
`SealRotationThreshold` (12288 of 16384): a fresh key epoch with unchanged
grants (plus the machine control grant, which only preserves existing local
authority — the machine already seals every event and holds every content
key), a reset per-epoch command admission budget, and full retention of
historical keys for replay, all in one SQLite transaction guarded by the epoch
CAS. Membership itself still changes only through a controller-signed
membership request. A losing concurrent rotation fails with `ErrConflict` and
the caller reloads the winning epoch; the hard bound stays as the backstop,
and one seal failure on capacity or stale epoch triggers the same rotation and
a single bounded retry.

Because every epoch transition resets the command admission budget, the
journal's 4096-command bound is a per-epoch resource guard, not a session
lifetime limit; command dedup rows are separate and never reset.

Command admission distinguishes a retired key epoch from a genuine
authorization failure: `journal.Admit` returns `ErrEpochStale` when the
session exists but the sender sealed under a non-current key, and the
daemon's encrypted delivery paths surface it as the content-free wire reason
`epoch_stale` in a rejected ack instead of tearing down the Adapter. An
encrypted terminal recovers by pulling the endpoint-signed membership
directory, requesting the current wrapped key, and resending once; inbound
events already converge because the wire carries the current `key_id` and the
endpoint wraps the current key for every granted device. A terminal never
moves its binding to an epoch older than the newest endpoint-signed directory
it has verified, so replayed historical key responses cannot downgrade it.

## Composed Endpoint Command Execution

`DecodeContentPacket` provides a 48-KiB raw network decoder with exact top-level members, explicit version, strict public projection and canonical envelope decoding. It rejects duplicated (including escaped-equivalent) members, unknown/null/version-downgrade fields and trailing data before cryptographic use. This Go entry point is not yet wired to production transport.

`CommandExecutor` composes the local vault, authenticated journal and packet decoder. It requires `command` scope explicitly; a separately signed `launch` packet cannot be reused through this delivery entry point. It accepts only send, permission response, interrupt, stop and settings-change commands. It decrypts under the current local grant and validates the protected container/projection before reserving a command. Only after durable reservation does it invoke the local Provider delivery callback. Callback success records local delivery completion, not successful completion of the Provider turn; failure or cancellation records outcome unknown with an independent five-second finalization budget. Raw Provider errors are not returned to the relay. The callback must honor the supplied thirty-second context; no background goroutine is used to pretend an uncooperative effect was canceled.

`SessionKeyVault.TransitionSession` commits a fresh self-wrapped content key, epoch CAS, never-reused key ID and grant replacement in one SQLite transaction. Existing standalone keys are not silently adopted. Failed key insertion rolls back all membership/session writes; a failed CAS leaves no orphan key. Rotation retains old history keys (retention cleanup remains outstanding). The executor uses this atomic path for membership changes. Tests inject SQLite key-insert failure, reject stale/reused epochs and verify distinct new keys with preserved old keys.

A per-executor bounded-context lane serializes membership changes with local delivery. The production daemon must enforce one executor owner for a Session and route all membership changes through it; creating a second executor or directly mutating the journal is not equivalent. Cross-process ownership, startup/session lifetime and real Provider wiring are outstanding. Real SQLite integration tests verify authenticated decode -> committed claim -> callback ordering, reopen duplicate refusal, failure ambiguity, revoked command denial, invalid packet rejection before claim, and canceled revocation while delivery owns the lane. Nothing in this component changes Hub seq or advertises encryption support.

## Local Machine Identity Binding

`MachineOffer` wraps the ephemeral pairing offer with the machine's long-term public identity and an Ed25519 ownership proof. Proof input is the JSON array `["agentwharf.e2ee.machine-offer.v1",version,id,machine,public_key,secret,expires_at,device,signing_key,wrapping_key]`. The entire object is local scan/paste material, never a relay request or log entry. Its self-signature proves key possession, not identity by itself: authenticity relies on the user's direct local transfer. `prepareMachinePairing` validates the machine identity before constructing the client proof and returns a copy of that identity for eventual trusted storage. Production trusted-peer persistence and account binding remain unimplemented.

Go tests reject substituted identity, machine, wrapping key, expiry and proof. The isolated Hermes handshake runner now verifies a signed Go MachineOffer and rejects substituted machine identity before proceeding. Metro needs the scoped NodeNext `.js` -> `.ts` SDK-source resolver in `mobile/metro.config.js`; otherwise SDK internal imports resolved to empty bundle dependencies in this environment. Native tests were rebuilt with the resolver and passed against a separate Metro server on port 8082. The Debug-only `SUPERWHV_E2EE_METRO_8082=1` launch environment selects it; Release is unchanged.

## Signed Session Initialization

An enrolled client may request epoch-one initialization using Ed25519 over the
UTF-8 JSON string array `["agentwharf.e2ee.session-init.v1", machine, account,
session, key_id, device]`. Each identifier follows the existing identifier
bounds. The signature uses canonical unpadded base64url. The raw relay carrier is an
exact six-string-field object (`machine`, `account`, `session`, `key_id`,
`device`, `signature`) bounded to 2048 bytes. The protocol-layer decoder rejects
duplicate/unknown fields, trailing values and outer machine/session mismatch;
it performs no signature authentication and must not authorize endpoint work. The endpoint matches
machine/account to its local registry and verifies the pinned device signing
key before creating any session state. Platform credentials cannot substitute
for this signature. Initial grants contain only the requesting device with
control access; this operation cannot modify an existing session's membership.

The executor serializes initialization with command execution and commits a
fresh wrapped key and epoch/grant in one transaction. Exact retries at epoch one
with the same sole grant/key recover that key wrapped for the enrolled device;
changed keys, grants or later epochs reject the request. HPKE wrapping may use
fresh randomness on retry but the content key is unchanged. No transport route
is activated by this primitive; raw request validation, relay integration and
production relay integration remain outstanding. The TypeScript signing API now
interoperates with Go's bounded strict decoder (duplicate/unknown fields and
trailing JSON rejected). Mobile `prepareNativeSessionInitialization` restores an
existing identity, requires a pinned machine and snapshots its key-store binding
before signing. Its response acceptance delegates to pinned-machine HPKE unwrap
and Keychain persistence. The machine-local account ID is a separate explicit
input from the mobile deployment/account storage namespace; neither is inferred
from relay data. Mobile orchestration tests use mocked native storage and do not
constitute native-device or production initialization evidence.

## Verification

Go and TypeScript unit tests exercise genuine encryption/signing and rejection behavior. The TypeScript interop test invokes the Go fixture command with synthetic keys to verify both directions. The command is a test harness, not a key-management CLI. Real SQLite tests cover role rejection, revoked sender and stale/reused epoch rejection, concurrent admission across two connections, reopen/replay, conflicting IDs, bounded capacity, cancellation and transaction rollback on injected insertion failure. Native React Native compatibility, browser/device rendering, authenticated pairing and the production executor lifecycle remain unverified and unimplemented.


## Encrypted launch settings payload

An endpoint-signed `session.send` plaintext may contain an optional `launch`
object alongside `content`. Required daemon launches bind `provider` inside this
object and reject a handoff selecting a different Provider. Other allowed fields
are `working_directory` (at most 4096
UTF-8 bytes, no NUL/CR/LF), `model_id`, `reasoning_effort_id`, and
`permission_mode_id` (existing settings identifier grammar). Duplicate/unknown
launch fields, nulls and non-string values reject. This object remains inside
the authenticated ciphertext; it is never an outer claim field. Absence preserves
instruction-only behavior.

`CommandExecutor.InspectWire` permits endpoint-only configuration inspection after
current-grant and packet authentication, clearing plaintext on return. It does
not reserve authority across later effects and must not replace durable execution
admission. The daemon applies verified settings to in-memory wrap configuration
without copying them into persisted handoff fields. Required ACP application
validates all choices first, rejects failures with fixed diagnostics, checks
Provider readback, and closes actual pipes on timeout to interrupt blocked I/O.

`ProcessConfig.StartGuard` wraps synchronous child creation. The endpoint executor
verifies the retained signed launch under a SQLite writer reservation shared with
grant transitions. Revocation cannot commit until Start returns. Cancellation
cannot automatically release that transaction during Start; SQL acquisition and
validation remain deadline-bound. The transaction makes no durable mutation and
releases its reservation by rollback after Start. Prompt delivery separately uses
ExecuteWire. The private runtime retains one immutable launch carrier/provider
per session in `e2ee_local_launches`; content-free recovery requires this evidence
and revalidates current grants. Rotation/cleanup of retained launches and full
multi-device lifecycle remain outstanding.


## Authenticated membership changes

`session.membership.change` carries a normal encrypted command packet whose
plaintext is a signed membership request. Exact request fields are `machine`,
`account`, `session`, `device`, `key_id`, `expected_epoch`, `members`, `signature`.
Members are 1..32 exact `{device,control}` objects in strictly increasing device
ID order. `control` is boolean; expected_epoch is a positive JS-safe integer
below MAX_SAFE_INTEGER. Endpoint decoding is bounded to 8192 bytes and rejects
ambiguous/unknown fields. Signing bytes are UTF-8 JSON:

`["agentwharf.e2ee.membership.v1",machine,account,session,device,key_id,expected_epoch,[[device,control],...]]`

The immutable local pairing registry supplies all public keys. A current session
controller authorizes the transition; platform scopes only authorize routing.
The machine serializes controller/epoch validation, fresh key creation, grant
replacement and a request-hash receipt in one SQLite write transaction. The
receipt table retains only the latest successful membership request per session.
An exact retry at that resulting current epoch succeeds without rotating again,
even if the request removed its signer. Changed requests, stale epochs, unpaired
members and view-only actors cannot mutate authority. Later transitions invalidate
older receipts. Key-write failure rolls back all grants/epoch/receipt changes.

Retries may carry the original encrypted packet under a retained historical key.
Opening that packet uses the paired signer; mutation authority is decided only
inside the transaction above, never by possession of the old key. Prompt delivery
and process-start grants remain separate from this operation. Six command types
are now supported by encrypted Store queues; legacy queues remain unchanged.

Go/TypeScript signature interoperability, real SQLite transaction/rollback and
concurrent retries, both PostgreSQL/SQLite queue lifecycles and Hub WebSocket
carrier preservation are verified. New-epoch key distribution, authenticated
member-key snapshots on clients and product membership controls remain to be
integrated before required transport activation.
