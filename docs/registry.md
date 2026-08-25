# patchbay_registry

The piece that lets a plugin be mounted before its dependency exists, and be
told about it when it appears -- Cordis's `inject` is demand-driven, and OTP
has no built-in primitive for that. `patchbay_registry` is it.

## Why hand-rolled instead of gproc

`patchbay_registry` is a single hand-rolled `gen_server`, not a pull of
`gproc` from Hex. patchbay has zero non-OTP dependencies, which fits this
project's BEAM-native thesis and keeps the core usable as a standalone
library, and the registry's needs are small enough (name → pid, plus a
subscribe/notify channel) that a purpose-built module is easier to reason
about than adopting a general-purpose process registry. Everything reaches
the registry only through the client API in `patchbay_registry.erl`
(`register`, `unregister`, `lookup`, `await`, `subscribe`, `unsubscribe`,
`names`), so a `gproc`-backed implementation could be swapped in behind that
API later without touching callers.

Single-node only, by design -- nothing here needs to work across BEAM nodes.

## API

| Call | Returns |
|---|---|
| `register(Name, Pid, Props)` | `ok` \| `{error, {already_registered, Pid}}` |
| `unregister(Name)` | `ok` |
| `lookup(Name)` | `{ok, {Pid, Props}}` \| `{error, not_found}` |
| `await(Name, Timeout)` | `{ok, Pid}` \| `{error, timeout}` -- blocks; returns immediately if already present. `Timeout` is a non-negative integer or `infinity` (park with no deadline); anything else raises `badarg` in the caller |
| `subscribe(Name)` | `ok` -- caller gets async notifications, **including an immediate one if `Name` is already registered** |
| `unsubscribe(Name)` | `ok` |
| `names()` | list of every currently registered name |

Notification messages sent to subscribers:

```
{patchbay_registry, registered,   Name, Pid}
{patchbay_registry, unregistered, Name, Reason}
```

## The two things that make this correct under concurrency

**`subscribe` replays an existing registration immediately.** If `Name` is
already registered at the moment of subscribing, the subscriber gets a
`registered` message right away, before `subscribe` returns. This removes
the lookup-then-subscribe race by construction: a caller that subscribes
never needs to call `lookup` first, and there's no gap between "check if it
exists" and "start listening for it to appear" for a registration to fall
into. `patchbay_service` relies on this -- see `docs/plugins.md`.

**`await` owns its own timeout, server-side.** The obvious implementation
(let the caller's `gen_server:call` timeout do the work) leaks: if the
caller gives up and the name never registers, the server never finds out
and the waiting entry sits in `waiters` forever; if the name registers much
later, the server tries to reply to a caller that's no longer listening.
Instead, `await` starts its own timer via `erlang:start_timer/3`, replies
`{error, timeout}` and cleans up when it fires, and monitors the calling
process so a caller that dies mid-wait is reaped too. Client calls use
`infinity` as the `gen_server:call` timeout -- the server-side timer is what
actually bounds the wait.

The state is a plain map (not a record) with one key per reverse index;
the test suite inspects it directly via `sys:get_state/1` to assert on
leak-freedom after timeouts, caller deaths, and subscriber deaths.

## Crash recovery

A registry crash does not lose the world. Every registration and
subscription is written through to a public ETS table,
`patchbay_registry_backup`, owned by `patchbay_sup` -- not by the registry
process -- so a one_for_one restart of the registry rebuilds from it:

- **Registrations survive** if their pid is still alive, with fresh
  monitors established by the new instance.
- **Subscriptions survive**, including subscriber monitors, so services
  keep receiving `registered`/`unregistered` notifications without doing
  anything themselves.
- **Entries whose pid died while the registry was down are pruned**, and
  their subscribers receive `{patchbay_registry, unregistered, Name,
  noproc}` -- the same message the monitor would have delivered had the
  registry been alive at that moment. A service depending on a dep that
  died during downtime therefore sees an honest `dep_down` instead of a
  stale "ready".

Scope and limits, deliberately:

- This covers **registry-process crashes only**. The backup table dies
  with `patchbay_sup` (i.e. with the application), which is correct:
  pids recorded in it are meaningless across an application or VM
  restart.
- In-flight `await` waiters are not persisted; their callers fail when
  the registry process dies (standard `gen_server:call` semantics).
- A bare `patchbay_registry:start_link/0` without the supervisor around
  creates a fallback table owned by the registry process itself,
  degrading to no recovery.

The service layer needs no recovery code of its own: `patchbay_service`
processes never die in a registry crash, and everything they need
(own registration, dependency subscriptions, dependency monitors) is
restored underneath them. The recovery test suite kills the registry
under a real tree and asserts exactly this -- see
`test/patchbay_registry_recovery_tests.erl` and
`test/patchbay_service_recovery_tests.erl`.
