# patchbay

A Cordis-inspired plugin/service runtime core for the BEAM: contexts
(supervised composition boundaries), services (callback modules with
mount-order-independent dependency injection), a race-free service
registry, and dynamic sub-agent supervision with crash isolation.

Pure Erlang on OTP -- zero dependencies beyond `kernel`/`stdlib` -- so it
works anywhere rebar3 does, as a library or embedded runtime.

Because every contract is plain atoms, tuples, and maps, plugins can be
written in any BEAM language -- Erlang, LFE, Elixir -- with no adapter
layer.

## Use it

```erlang
%% rebar.config
{deps, [
    {patchbay, {git, "https://github.com/takeiteasy/patchbay.git", {branch, "trunk"}}}
]}.
```

```erlang
application:ensure_all_started(patchbay),
{ok, Ctx} = patchbay_context:start_link(my_root, #{}),
ok = supervisor:start_child(Ctx, my_plugin:child_spec(args)),
pong = patchbay_service:call_service(my_plugin, ping).
```

See [docs/getting-started.md](docs/getting-started.md) for build, test,
and a worked walkthrough; [docs/plugins.md](docs/plugins.md) for the
callback contract; [docs/architecture.md](docs/architecture.md),
[docs/registry.md](docs/registry.md), and
[docs/delegation.md](docs/delegation.md) for how the pieces fit.

The agent harness built on top of patchbay lives at
[takeiteasy/nyaa](https://github.com/takeiteasy/nyaa), which shares this
project's issue tracker (`~takeiteasy/nyaa` on sourcehut).

## License

GPLv3, see [LICENSE](LICENSE).
