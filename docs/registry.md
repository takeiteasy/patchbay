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
| `await(Name, Timeout)` | `{ok, Pid}` \| `{error, timeout}` -- blocks; returns immediately if already present |
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
