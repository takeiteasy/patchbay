-module(patchbay_context).

-behaviour(supervisor).

-export([start_link/2, mount/2, unmount/2, children/1, init/1]).

%% A context is a supervisor -- one per composition boundary (see
%% docs/architecture.md). A context registers *itself* in
%% patchbay_registry under `name` during init/1, before start_link/2
%% returns to its caller, so nested contexts are discoverable exactly like
%% any other service: mounting a child spec whose module is another
%% patchbay_context is all a nested context is. No separate mechanism
%% needed.
%%
%% Restart strategy is one_for_one; individual child specs decide their
%% own restart type (permanent/transient/temporary) -- `transient` is the
%% recommended default (restart only on abnormal exit), overridable per
%% plugin via the child spec, not hardcoded here.

start_link(Name, Opts) ->
    supervisor:start_link(?MODULE, {Name, Opts}).

init({Name, Opts}) ->
    patchbay_registry:register(Name, self(), Opts),
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    {ok, {SupFlags, []}}.

mount(Ctx, ChildSpec) ->
    supervisor:start_child(Ctx, ChildSpec).

unmount(Ctx, Id) ->
    case supervisor:terminate_child(Ctx, Id) of
        ok -> supervisor:delete_child(Ctx, Id);
        Other -> Other
    end.

children(Ctx) ->
    supervisor:which_children(Ctx).
