# Web administration

利用者向けの入口は[はじめる](getting-started.md)、日常の操作は[日常操作](operations.md)、
設定変更は[設定](configuration.md)、エラーの対処は[困ったとき](troubleshooting.md)を参照してください。
このページは管理画面の全操作・Tailscale・HTTP契約の詳細です。
Docker Desktopでの起動・停止・Tailscale設定は[Dockerで使う](docker.md)を参照してください。

Start the administration UI with the same configuration and data directory used
by the dialogue runtime:

```text
mahoroba admin serve --config config.toml
```

Open [http://127.0.0.1:8788/](http://127.0.0.1:8788/). The administration listener
can start before a database or resident exists and does not require generation
credentials. Its page shows the fixed target data directory. To choose another
directory or port, set them at startup:

```text
mahoroba admin serve --config config.toml --data-dir C:\Mahoroba\data --listen 127.0.0.1:8788
```

The configuration still supplies timezone, generation, and other runtime
settings. The administration port is independent of `server.listen`; its default
is 8788. Administration requires a loopback listener by default and always
accepts only loopback request hosts. The explicit `--container-listen` option
permits a container-interface listener without changing Host or Origin checks;
the supplied Compose file publishes it only on the host's loopback address.
Dialogue has a separate explicit remote-access option described
below. Provider and TTS credentials retain their existing environment/secret-file configuration;
the browser has no credential or arbitrary command-line field.

Administration assumes a single-user local environment and has no login or
user authentication. Loopback Host and same-origin checks protect the browser
boundary; they do not authenticate another OS user or a local process that can
reach the listener. On a shared computer, other users may also operate this
administration UI. Existing filesystem permissions and host locks remain in
effect, but they do not add HTTP user authentication.

## Access dialogue through your Tailscale network

Keep both application listeners on loopback and use Tailscale Serve for the
HTTPS connection from your tailnet. Start administration with the dialogue-only
flag, then use **Start dialogue server**:

```text
mahoroba admin serve --config config.toml --dialogue-allow-remote
```

From this repository, omit `--config` to use the normal configuration lookup:

```text
go run ./cmd/mahoroba admin serve --dialogue-allow-remote
```

Leave `server.listen = "127.0.0.1:8787"` and the administration listener at
`127.0.0.1:8788`. Point the Tailscale Serve root route at **dialogue**, not
administration:

```text
tailscale serve --bg --https=443 --set-path=/ --yes http://127.0.0.1:8787
```

This updates the HTTPS root mapping, including an existing mapping to port 8788.
Open the HTTPS tailnet URL reported by Tailscale. Use Tailscale Serve, not Funnel;
the intended audience is your own tailnet. See the
[Tailscale Serve reference](https://tailscale.com/docs/reference/tailscale-cli/serve).
Administration stays available on the host at `http://127.0.0.1:8788/`.

For a separately started dialogue process, the equivalent flag is:

```text
mahoroba serve --config config.toml --allow-remote
```

Neither flag changes the listen address, enables management routes on dialogue,
or adds application authentication. Access is controlled by your Tailscale
network and its permissions. The flag permits the dialogue page's custom Host;
write requests must still have a matching Origin authority (hostname and port).
HTTP and HTTPS Origins are accepted for this dialogue option so an HTTPS proxy
can forward to a local HTTP upstream. Preserve the external Host when proxying;
`Forwarded` and `X-Forwarded-*` do not override the Origin checks, and no CORS
wildcard is enabled. The administration listener keeps its existing Host and
HTTP Origin checks even with `--dialogue-allow-remote`.

Restart an already-running process to apply the flag. To turn it off, restart
without the flag. The application does not read a TOML setting or environment
variable for this permission, and management API requests cannot enable it.
For Docker, Compose explicitly translates `MAHOROBA_DIALOGUE_ALLOW_REMOTE`
into this startup flag; see the [Docker guide](docker.md).

As an alternative to Tailscale Serve, dialogue can bind directly to the host's
Tailscale address. For example, replace `100.64.0.42` below with the host's
actual Tailscale IPv4 address, then start with the corresponding dialogue flag:

```toml
[server]
listen = "100.64.0.42:8787"
```

The shared setting does not prevent finite administration commands from running.
Starting dialogue with this non-loopback setting still requires the explicit
flag; management's own `--listen` remains loopback-only unless container binding
is explicitly enabled. The CLI and management
**Check dialogue server health** operation remain loopback-only probes, so they
cannot probe a listener bound only to a Tailscale IP. In that direct-bind setup,
check `/healthz` through the permitted Tailscale address. Wildcard bind addresses,
if explicitly configured, are converted to loopback only for local navigation
links and the default healthcheck target.

## Create the first resident

1. Choose **Bootstrap init**. Enter the owner, resident name, seed, and principles.
   Review the principles before running it and save the returned `resident_id`.
2. Choose **Bootstrap approve**, enter that ID, and explicitly confirm approval
   of the principles.
3. Choose **Bootstrap finalize**, enter that ID, review the persona and initial
   memory policy, and run it. **Select this resident after activation** is checked
   by default.
4. **List residents** or **Show resident** confirms that the resident is active.

These are separate operations with the same validation and idempotency rules as
the CLI. Finalization without approval fails. Creating the administration server
does not create a database or silently initialize a resident.

The default bootstrap policy keeps memory recall disabled and is eligible for
ordinary dialogue. Enabled historical v2/v3 memory policies must be migrated with
**Activate memory policy v4** or **Activate memory policy v5** before starting
dialogue. Use v5 for the current Recall ranking; review the expected current
version and acknowledgements described in [configuration](configuration.md#記憶とself-talk).

## Dialogue and maintenance

If administration is already running from an older build, stop it and restart
it with the updated code to load these controls. Restarting the browser alone
does not update a running administration server.

Use **Start dialogue server** in the management page to start dialogue using the
same configuration file and data directory. The page shows Starting, Running,
Stopping, Stopped, or Failed and reports the startup or shutdown error. Running
means the ordinary startup checks and listener activation have completed;
**Check dialogue server health** remains available for a current readiness check.
Closing a browser tab does not stop the managed server.

Use **Stop dialogue server** and wait for **Stopped** before running an offline
operation. The stop action drains the owned runtime and releases its listener
and data-directory lock. Starting and stopping are separate from the finite
command catalog, and repeated requests cannot create overlapping managed
runtimes. A start request is rejected while a finite management operation is
still executing.

The ordinary CLI entrypoint also remains available:

```text
mahoroba serve --config config.toml
```

The dialogue and administration pages link to each other. A managed dialogue
server links back to the actual management listener, including a custom port.
A separately started dialogue CLI uses the standard management address,
`http://127.0.0.1:8788/`. The management page initially derives its dialogue link
from `server.listen` and updates it to the actual bound address after a managed
start.

While dialogue is stopped, administration holds no resident runtime or
persistent data-directory lock. A managed dialogue runtime runs inside the
administration process, and finite operations call the existing CLI dispatcher
inside that same process. No shell or subprocess is involved. Each operation
retains its normal resource ownership and locking rules:

- Commands marked **Offline** require the dialogue server to be stopped. An
  already-running runtime makes them fail safely through the existing host lock.
  The administration page itself can remain open.
- Query-only database, ledger, projection, claim, persona, and autonomy inspection
  can run while dialogue is active. Healthcheck inspects the dialogue listener.
- Backup verification reads the selected bundle. Restore writes a new target
  directory, retaining existing namespace locks and overlap checks.
- The Stop button controls only dialogue started by this administration
  process. A separately started CLI/server is never terminated or taken over.
  If such a runtime holds the data directory or port, Start reports the existing
  lock or bind failure. Stop that separate runtime in its original terminal.
- Exiting the administration command cancels and joins its managed dialogue
  runtime. If HTTP handlers or workers cannot drain safely, the page reports
  Failed with a restart-required message; it does not release live resources or
  permit another managed start. Restart the administration process before
  retrying.

Changing the selected configuration file, data directory, or listener address
requires restarting the administration command. These source settings cannot be
overridden by browser requests. Each operation reads the selected configuration
file using the normal CLI behavior; restart administration after changing the
dialogue port so its navigation link also reflects the new address.

## Available operations

All 41 finite product CLI operations are included. Process entrypoints
(`serve`, `admin serve`) and CLI help are not finite catalog operations. The
dedicated dialogue controls manage the owned `serve` lifecycle.

| Group | CLI operations |
| --- | --- |
| Resident | `admin bootstrap init/approve/finalize`; `admin resident list/show/select/archive` |
| Memory | `admin memory policy activate-v0/activate-autonomy-v0/activate-v4/activate-v5`; `admin memory claim list/show/scope/status`; `admin memory reextract/abstract/split`; `admin memory persona propose/list` |
| Autonomy | `admin autonomy status`; `admin autonomy retention candidates` |
| Inspection and maintenance | `db verify`; `ledger verify`; `projection status/rebuild`; `admin integrity scan`; `admin recovery terminalize`; `admin runtime session-policy select`; `admin diagnostics`; `healthcheck` |
| Backup and export | `backup create/verify/restore`; `export jsonl` |
| Blobs and erasure | `blob recover/gc`; `admin erasure plan content/resident`; `admin erasure decide/apply` |

The legacy policy operations `activate-v0` and `activate-autonomy-v0` only
retry an already-active v2 or v3 policy. For a new policy transition, use
`activate-v5` to enable the current Recall ranking; existing v4 residents remain
supported. See [memory and self-talk setup](configuration.md#記憶とself-talk).

Required fields, choices, repeated identifiers, and explicit boolean options are
rendered from the command catalog. Repeated fields accept one value per line.
The server rejects unknown commands, unknown fields, and duplicated scalar
fields. Configuration and source data-directory arguments are fixed by startup.

For GC, first run with **Apply deletion** unchecked, inspect the plan, and then
supply the exact `sha256:` digest when applying. Erasure preserves the existing
plan → decision when required → apply workflow, including the exact digest and a
separate confirmation. File and directory paths refer to the local server's filesystem.
Artifact commands preserve their existing no-overwrite and filesystem checks.

The result view displays the CLI exit code, output, and diagnostics. A nonzero
exit code is not reported as success; inspect any partial changes or required
actions before retrying. Each output stream is limited to 8 MiB for display, with
an explicit truncation marker. Artifact files themselves are unaffected. The
**Download result** button saves the displayed result.

## HTTP contract

The management listener exposes:

- `GET /`: management page.
- `GET /admin/commands`: the finite operation catalog.
- `POST /admin/execute`: a same-origin JSON request containing `command`,
  `values` (field name → string array), and `confirmed`.
- `GET /admin/runtime`: owned dialogue lifecycle status.
- `POST /admin/runtime/start` and `POST /admin/runtime/stop`: same-origin JSON
  requests containing `confirmed: true`; accepted transitions return status
  immediately and the page polls until startup or shutdown completes.
- `GET /static/...`: embedded assets.

A command response contains `exit_code`, `stdout`, and `stderr`. HTTP 200 means
the invocation returned a result; operation success is indicated by
`exit_code == 0`. The ordinary dialogue listener does not expose these
administrative execution routes.

Runtime status includes `state`, `managed`, `ready`, `url`, `error`, `message`,
and `restart_required`. `managed: false` means this administration process owns
no running runtime; it does not assert that a separately started server is absent.
Startup failures preserve normal configuration, provider, resident-readiness,
and host-lock checks, with loopback-only dialogue access unless explicitly enabled
as described above. The controls never migrate a resident policy or
change the selected resident automatically.
