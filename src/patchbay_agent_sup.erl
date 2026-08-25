-module(patchbay_agent_sup).

-behaviour(supervisor).

-export([start_link/1, delegate/5, stop/2, init/1]).

%% The sub-agent supervisor: a simple_one_for_one dynamic supervisor --
%% one patchbay_agent child per delegated sub-agent, started on demand via
%% delegate/5 and stopped via stop/2. It is a plain supervisor (not an
%% patchbay_context) because it has nothing to compose: the harness mounts
%% it onto a context like any other child.
%%
%% Children are `temporary`: a crashing sub-agent is NOT restarted and
%% never takes this supervisor down. That is the free crash isolation --
%% a parent that wants failure notification gets it from the MonRef
%% delegate/5 returns (a plain `DOWN` message), alongside the tagged
%% done message a well-behaved sub-agent sends on its own. Restart
%% policy can be revisited once something (e.g. the vault) wants
%% durable redelivery.

start_link(Name) ->
    supervisor:start_link({local, Name}, ?MODULE, []).

%% Start one sub-agent, delegated from the calling process (the
%% parent). Mod is the callback module; cbargs go to its init/1; ref
%% tags the {patchbay_agent, done, Ref, Pid, Result} message back to
%% parent; opts is a map of start options -- currently just an optional
%% `name` key, which opts the agent into registry registration under
%% that name.
%%
%% Monitors the new child from the parent process before returning, so
%% a crash is visible as an ordinary `DOWN` message with no separate
%% step to remember. Returns {ok, Pid, MonRef} or {error, Reason}.
delegate(Sup, Mod, CbArgs, Ref, Opts) ->
    case supervisor:start_child(Sup, [{Mod, CbArgs, Ref, self(), Opts}]) of
        {ok, Pid} ->
            Mon = erlang:monitor(process, Pid),
            {ok, Pid, Mon};
        {error, Reason} ->
            {error, Reason}
    end.

%% Stop a specific sub-agent by pid. terminate_child/2 accepts a pid
%% for a simple_one_for_one supervisor, blocks until the child is
%% actually down, and -- since patchbay_agent traps exits and the child
%% spec's shutdown is a timeout rather than brutal_kill -- runs
%% terminate/2 (the disposer) before returning.
stop(Sup, Pid) ->
    supervisor:terminate_child(Sup, Pid).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one, intensity => 5, period => 10},
    ChildSpec = #{id => patchbay_agent,
                  start => {patchbay_agent, start_link, []},
                  restart => temporary,
                  shutdown => 5000,
                  type => worker,
                  modules => [patchbay_agent]},
    {ok, {SupFlags, [ChildSpec]}}.
