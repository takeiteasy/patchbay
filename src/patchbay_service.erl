-module(patchbay_service).

-behaviour(gen_server).

%% Client API
-export([start_link/2, call_service/2, cast/2]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Wraps a callback module in a gen_server that handles registry
%% registration and dependency-waiting once, so individual plugins
%% don't reimplement it. See docs/plugins.md for the full callback
%% contract; the short version:
%%
%%   service_name()               -> atom                    [required]
%%   dependencies()               -> [atom()]                [required]
%%   init(Args)                   -> {ok, State}             [required]
%%   ready(Deps, State)           -> {ok, State}              [optional]
%%   dep_down(Name, Reason, State)-> {ok, State}              [optional]
%%   handle_message(Msg, State)   -> {ok, State} | {reply, R, State}
%%                                                            [optional]
%%   terminate(Reason, State)     -> ok                       [optional]
%%
%% Missing optional callbacks default to no-ops, checked with
%% erlang:function_exported/3 so a trivial plugin only writes
%% service_name, dependencies, and init.
%%
%% Dependency waiting: init/1 subscribes to every declared dependency
%% via patchbay_registry:subscribe/1 and does NOT call lookup first --
%% the registry's subscribe replays an existing registration
%% immediately (see patchbay_registry), so this is race-free by
%% construction and never blocks the supervisor start.
%%
%% terminate/2 is the plugin's disposer: the callback's own terminate/2
%% runs first, then this module unregisters the service from
%% patchbay_registry (see docs/plugins.md).

-record(service,
        {mod :: module(),
         name :: atom(),
         deps :: [atom()],
         ready :: #{atom() => pid()},
         status :: waiting | ready,
         cbstate :: term()}).

-callback service_name() -> atom().
-callback dependencies() -> [atom()].
-callback init(Args :: term()) -> {ok, State :: term()}.
-callback ready(Deps :: #{atom() => pid()}, State :: term()) ->
    {ok, State :: term()}.
-callback dep_down(Name :: atom(), Reason :: term(), State :: term()) ->
    {ok, State :: term()}.
-callback handle_message(Msg :: term(), State :: term()) ->
    {ok, State :: term()} | {reply, Reply :: term(), State :: term()}.
-callback terminate(Reason :: term(), State :: term()) -> ok.

-optional_callbacks([ready/2, dep_down/3, handle_message/2, terminate/2]).

%% ------------------------------------------------------------------
%% Client API
%% ------------------------------------------------------------------

start_link(Mod, Args) ->
    gen_server:start_link(?MODULE, {Mod, Args}, []).

call_service(Name, Msg) ->
    case patchbay_registry:lookup(Name) of
        {ok, {Pid, _Props}} -> gen_server:call(Pid, {msg, Msg});
        {error, not_found} -> {error, not_found}
    end.

cast(Name, Msg) ->
    case patchbay_registry:lookup(Name) of
        {ok, {Pid, _Props}} -> gen_server:cast(Pid, {msg, Msg});
        {error, not_found} -> {error, not_found}
    end.

%% ------------------------------------------------------------------
%% gen_server callbacks
%% ------------------------------------------------------------------

init({Mod, Args}) ->
    %% A plain gen_server does not trap exits, so a supervisor's ordinary
    %% shutdown -- exit(Pid, shutdown) via supervisor:terminate_child --
    %% would kill this process outright without ever calling terminate/2,
    %% silently skipping the callback's disposer and the registry
    %% unregister below. Trapping exits is what makes terminate/2 (and
    %% therefore the plugin's disposer) actually fire on
    %% patchbay_context:unmount.
    process_flag(trap_exit, true),
    Name = Mod:service_name(),
    Deps = Mod:dependencies(),
    {ok, CbState0} = Mod:init(Args),
    State0 = #service{mod = Mod, name = Name, deps = Deps,
                      ready = #{}, status = waiting, cbstate = CbState0},
    patchbay_registry:register(Name, self(), #{}),
    %% Subscribing (not looking up) is what makes this race-free: a
    %% dependency already registered is replayed to us immediately by
    %% the registry, so the zero-deps and already-satisfied-deps cases
    %% both fall out of the same maybe_transition_ready call below,
    %% whether or not any registered notification ever needs to arrive.
    lists:foreach(fun(Dep) -> patchbay_registry:subscribe(Dep) end, Deps),
    {ok, maybe_transition_ready(State0)}.

handle_call({msg, Msg}, _From, #service{mod = Mod, cbstate = CbState} = State) ->
    case call_handle_message(Mod, Msg, CbState) of
        {reply, R, CbState2} -> {reply, R, set_cbstate(State, CbState2)};
        {ok, CbState2} -> {reply, ok, set_cbstate(State, CbState2)}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({patchbay_registry, registered, DepName, Pid}, State) ->
    handle_dep_registered(DepName, Pid, State);
handle_info({patchbay_registry, unregistered, DepName, Reason}, State) ->
    handle_dep_unregistered(DepName, Reason, State);
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(Reason, State) ->
    call_terminate(State#service.mod, Reason, State#service.cbstate),
    %% `catch': during shutdown ordering the registry may already be
    %% gone (noproc) -- nothing left to unregister with, and crashing
    %% inside terminate would only turn an orderly stop into an error
    %% report.
    catch patchbay_registry:unregister(State#service.name),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% dependency lifecycle
%% ------------------------------------------------------------------

handle_dep_registered(DepName, Pid, State) ->
    case lists:member(DepName, State#service.deps) of
        true ->
            Ready = maps:put(DepName, Pid, State#service.ready),
            State1 = State#service{ready = Ready},
            {noreply, maybe_transition_ready(State1)};
        false ->
            {noreply, State}
    end.

handle_dep_unregistered(DepName, Reason, State) ->
    case lists:member(DepName, State#service.deps) of
        true ->
            WasReady = State#service.status =:= ready,
            Ready = maps:remove(DepName, State#service.ready),
            State1 = State#service{ready = Ready, status = waiting},
            case WasReady of
                true ->
                    {noreply, transition_dep_down(DepName, Reason, State1)};
                false ->
                    {noreply, State1}
            end;
        false ->
            {noreply, State}
    end.

all_deps_ready(Deps, Ready) ->
    lists:all(fun(D) -> maps:is_key(D, Ready) end, Deps).

%% Called both from init/1 (covers the zero-dependency case, which no
%% registered notification would ever trigger) and from
%% handle_dep_registered (covers the normal case). Idempotent: a no-op
%% once status is already ready.
maybe_transition_ready(#service{deps = Deps, ready = Ready} = State) ->
    case all_deps_ready(Deps, Ready)
         andalso State#service.status =/= ready of
        true ->
            {ok, CbState2} = call_ready(State#service.mod, Ready,
                                        State#service.cbstate),
            State1 = State#service{status = ready},
            State1#service{cbstate = CbState2};
        false ->
            State
    end.

transition_dep_down(DepName, Reason, State) ->
    {ok, CbState2} = call_dep_down(State#service.mod, DepName, Reason,
                                   State#service.cbstate),
    State#service{cbstate = CbState2}.

%% ------------------------------------------------------------------
%% optional-callback dispatch
%% ------------------------------------------------------------------

call_ready(Mod, Deps, CbState) ->
    case erlang:function_exported(Mod, ready, 2) of
        true -> Mod:ready(Deps, CbState);
        false -> {ok, CbState}
    end.

call_dep_down(Mod, DepName, Reason, CbState) ->
    case erlang:function_exported(Mod, dep_down, 3) of
        true -> Mod:dep_down(DepName, Reason, CbState);
        false -> {ok, CbState}
    end.

call_handle_message(Mod, Msg, CbState) ->
    case erlang:function_exported(Mod, handle_message, 2) of
        true -> Mod:handle_message(Msg, CbState);
        false -> {ok, CbState}
    end.

call_terminate(Mod, Reason, CbState) ->
    case erlang:function_exported(Mod, terminate, 2) of
        true ->
            _ = Mod:terminate(Reason, CbState),
            ok;
        false ->
            ok
    end.

set_cbstate(State, CbState) ->
    State#service{cbstate = CbState}.
