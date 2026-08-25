-module(patchbay_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

%% Root supervisor for the `patchbay` core application. Starts the
%% registry as its sole permanent child -- everything else (contexts,
%% services) is mounted at runtime by the harness on top of this.

start_link() ->
    supervisor:start_link({local, patchbay_sup}, ?MODULE, []).

init([]) ->
    %% The registry's backup table is owned HERE, not by the registry
    %% process: a one_for_one restart of the registry must not destroy
    %% the very state the fresh instance is about to restore. The table
    %% dies with this supervisor, i.e. with the whole application -- the
    %% correct durability scope, since pids recorded in it are
    %% meaningless across an application or VM restart.
    _ = ets:new(patchbay_registry_backup,
                [named_table, public, set, {read_concurrency, true}]),
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    Children =
        [#{id => patchbay_registry,
           start => {patchbay_registry, start_link, []},
           restart => permanent,
           shutdown => 5000,
           type => worker,
           modules => [patchbay_registry]}],
    {ok, {SupFlags, Children}}.
