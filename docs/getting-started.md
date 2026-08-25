# Getting started

## Toolchain

- Erlang/OTP (developed against OTP 29 / ERTS 17.0.5)
- [rebar3](https://rebar3.org/)

That's all -- patchbay is pure Erlang with zero dependencies beyond
`kernel` and `stdlib`.

## Build

```sh
rebar3 compile
```

## Test

```sh
rebar3 eunit
```

The registry suite includes deliberately racy scenarios forced
deterministically with `sys:suspend`/`resume`; if a run ever fails there,
retry before suspecting the code -- and if it reproduces, that's a real
bug worth a ticket.

## Use as a dependency

```erlang
%% rebar.config
{deps, [
    {patchbay, {git, "https://github.com/takeiteasy/patchbay.git", {branch, "trunk"}}}
]}.
```

Then:

```erlang
application:ensure_all_started(patchbay),
{ok, Ctx} = patchbay_context:start_link(my_root, #{}),
ok = supervisor:start_child(Ctx, my_plugin:child_spec(args)),
pong = patchbay_service:call_service(my_plugin, ping).
```

A plugin is any module exporting the callback contract in
[docs/plugins.md](plugins.md) -- in Erlang, LFE, Elixir, or any BEAM
language.

## Walkthrough

Start a shell (`rebar3 shell`) and mount a plugin whose dependency doesn't
exist yet; it waits without blocking and becomes ready on its own once the
dependency appears:

```erlang
%% define a quick provider somewhere, or load one from the test suite,
%% then:
{ok, Ctx} = 'patchbay_context':start_link(demo, #{}),
'patchbay_registry':subscribe('some-dependency'),
ok = supervisor:start_child(Ctx, my_consumer:child_spec(args)).
%% ...mount something registering 'some-dependency' and watch the
%% {patchbay_registry, registered, Name, Pid} notification arrive.
```

See [docs/architecture.md](architecture.md) for how the pieces fit and
[docs/registry.md](registry.md) for why this is race-free by construction.
