-module(patchbay_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

%% Root supervisor for the `patchbay` core application. Starts the
%% registry as its sole permanent child -- everything else (contexts,
%% services) is mounted at runtime by the harness on top of this.

start_link() ->
    supervisor:start_link({local, patchbay_sup}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    Children =
        [%% LIMITATION: `permanent` restarts the registry process on
         %% crash, but a fresh patchbay_registry has empty state -- every
         %% registration and subscription is lost, and nothing
         %% re-registers itself automatically. The tree stays alive
         %% but every dependency relationship in it goes invisible.
         %% Acceptable for this bootstrap (the registry is simple
         %% enough not to crash in practice); revisit once services
         %% can detect and recover from a registry restart, or the
         %% registry itself persists/replays its state.
         #{id => patchbay_registry,
           start => {patchbay_registry, start_link, []},
           restart => permanent,
           shutdown => 5000,
           type => worker,
           modules => [patchbay_registry]}],
    {ok, {SupFlags, Children}}.
