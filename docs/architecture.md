# Architecture

`patchbay` is a Cordis-inspired plugin/service runtime core for the BEAM.
The core idea: reproduce Cordis's user-facing capabilities
(context/service composition, dependency injection that doesn't care about
mount order, a disposer on teardown) using BEAM-native primitives --
supervision trees, message passing, and hot code loading -- rather than a
mutable Lisp image.

It is written in Erlang with zero non-OTP dependencies, so it works as an
embedded runtime or plain library anywhere rebar3 does. The agent harness
built on top of it lives separately at
[takeiteasy/nyaa](https://github.com/takeiteasy/nyaa) (LFE), which shares
this project's issue tracker.

## Layering

```
┌─────────────────────────────────────┐
│  Agent loop (top-level plugin)       │   -- nyaa repo, not yet built
├─────────────────────────────────────┤
│  Tool / skill plugins                │   -- nyaa repo, not yet built
│  (shell, fs, http, lisp-eval, ...)   │
├─────────────────────────────────────┤
│  Sub-agent supervisor                │   patchbay_agent,
│                                      │   patchbay_agent_sup
├─────────────────────────────────────┤
│  patchbay: context, service,         │   -- this repo (Erlang)
│  registry                            │
├─────────────────────────────────────┤
│  OTP (supervisor, gen_server, code)  │
└─────────────────────────────────────┘
```

Plugin code can live in any BEAM language: every contract in patchbay is
plain atoms, tuples, and maps, so an Erlang, LFE, or Elixir module that
exports the right functions is a plugin -- no adapter layer.

## Cordis → OTP mapping

| Cordis concept | OTP realization | Where |
|---|---|---|
| `Context` | a supervisor, one per composition boundary | `patchbay_context` |
| `Service` | a gen_server wrapping a callback module | `patchbay_service` |
| plugin mount | `supervisor:start_child/2` | `patchbay_context:mount/2` |
| plugin unmount | `supervisor:terminate_child/2` + the callback's `terminate/2` | `patchbay_context:unmount/2` |
| `ctx.effect()` | resource acquired in the callback's `init/1`, released in its `terminate/2` | see `docs/plugins.md` |
| `inject` (DI) | subscribe to the registry; replay-on-subscribe makes mount order irrelevant | `patchbay_registry` |
| context tree | nested supervisors -- a sub-supervisor started via `mount` *is* a child context | `patchbay_context` |

Service discovery (Cordis's `inject`, which lets a plugin mount before its
dependency exists) was the key open design question for this runtime -- OTP
has no built-in "wait for a named service" primitive. It's resolved by
`patchbay_registry`: see `docs/registry.md`.

## What's built vs. deferred

Built: `patchbay_context`, `patchbay_service`, `patchbay_registry`,
sub-agent delegation (`patchbay_agent`, `patchbay_agent_sup` -- see
`docs/delegation.md`): a dynamic supervisor that starts and stops one
process per delegated sub-agent on demand, with crash isolation and a
tagged done-message protocol for a sub-agent to report back to its parent.

The demo plugin pair proving mount-order independence, dependency-down/
re-ready transitions, and disposer firing lives in the nyaa repo's test
suite. Platform-level deferred work (tool/skill plugins, the agent loop,
the vault, checkpoint/rollback) is tracked on the shared
`~takeiteasy/nyaa` tracker.
