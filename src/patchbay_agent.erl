-module(patchbay_agent).

-behaviour(gen_server).

%% Client API
-export([start_link/1, prompt/2, prompt_wait/2]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% One delegated sub-agent. Wraps a callback module in a gen_server --
%% the same optional-callback style as patchbay_service, minus the
%% dependency machinery. The callback contract:
%%
%%   init(Args)                  -> {ok, State}              [required]
%%   handle_message(Msg, State)  -> {ok, State} | {reply, R, State}
%%                               | {done, Result, State}     [optional]
%%   terminate(Reason, State)    -> ok                        [optional]
%%
%% See docs/delegation.md for the full contract, including why a cast
%% drops {reply, R, State} on the floor (there is no caller to reply
%% to -- a sub-agent that wants to talk back during a cast sends a
%% message itself).
%%
%% Returning {done, Result, State2} from handle_message is how a
%% sub-agent "decides it is done": patchbay_agent sends the tagged
%% message
%%
%%   {patchbay_agent, done, Ref, Pid, Result}
%%
%% to its parent and stops normally, on both the cast and call paths.
%% The ref was supplied by the parent at delegate time so concurrent
%% delegations are matchable.
%%
%% Crash isolation is structural: this process runs under its
%% patchbay_agent_sup as a `temporary` child, so a crashing sub-agent is
%% simply gone -- it never takes the parent down and is not
%% restarted. patchbay_agent_sup:delegate/5 monitors the child from the
%% parent process, so a crash also arrives as an ordinary `DOWN`.
%%
%% Registration in patchbay_registry is opt-in: pass #{name => Atom} as
%% opts to be discoverable under that name for the agent's lifetime;
%% omit `name` (or pass #{}) to stay unregistered, which is the
%% default for what is fundamentally an ephemeral task, not a
%% service. A duplicate name fails the start loudly (registry returns
%% {error, ...}, which init/1 turns into {stop, Reason}) rather than
%% silently colliding with a sibling agent.
%%
%% The single start argument is:
%%   {Mod, CbArgs, Ref, Parent, Opts}

-record(agent,
        {mod :: module(),
         name :: atom() | undefined,
         ref :: term(),
         parent :: pid(),
         cbstate :: term()}).

%% ------------------------------------------------------------------
%% Client API
%% ------------------------------------------------------------------

start_link(ChildArgs) ->
    gen_server:start_link(?MODULE, ChildArgs, []).

%% Fire-and-forget prompt.
prompt(Pid, Msg) ->
    gen_server:cast(Pid, {msg, Msg}).

%% Blocking prompt; the reply comes from {reply, R, State}, or `ok` on
%% {ok, State2} / the done path.
prompt_wait(Pid, Msg) ->
    gen_server:call(Pid, {msg, Msg}).

%% ------------------------------------------------------------------
%% gen_server callbacks
%% ------------------------------------------------------------------

init({Mod, CbArgs, Ref, Parent, Opts}) ->
    %% Trap exits so supervisor shutdown reaches terminate/2 (the
    %% disposer) instead of killing us outright -- same reason as
    %% patchbay_service's init/1.
    process_flag(trap_exit, true),
    {ok, CbState0} = Mod:init(CbArgs),
    State0 = #agent{mod = Mod, name = undefined, ref = Ref,
                    parent = Parent, cbstate = CbState0},
    case maps:find(name, Opts) of
        {ok, Name} ->
            case patchbay_registry:register(Name, self(), #{}) of
                ok -> {ok, State0#agent{name = Name}};
                {error, Reason} -> {stop, Reason}
            end;
        error ->
            {ok, State0}
    end.

handle_call({msg, Msg}, _From, State) ->
    #agent{mod = Mod, cbstate = CbState} = State,
    match_result(call_handle_message(Mod, Msg, CbState), State).

handle_cast({msg, Msg}, #agent{mod = Mod, cbstate = CbState} = State) ->
    %% Fire-and-forget done: send the tagged message now; the
    %% normal stop below still runs terminate/2 (the disposer).
    case call_handle_message(Mod, Msg, CbState) of
        {done, Result, CbState2} ->
            {stop, normal, send_done(set_cbstate(State, CbState2), Result)};
        {reply, _R, CbState2} ->
            {noreply, set_cbstate(State, CbState2)};
        {ok, CbState2} ->
            {noreply, set_cbstate(State, CbState2)}
    end.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(Reason, #agent{mod = Mod, name = Name, cbstate = CbState}) ->
    %% Unregister last, after the disposer has run -- same order as
    %% patchbay_service, so a dying agent can notify anyone as its last
    %% act. Only unregistered if a name was actually claimed at init
    %% time.
    case erlang:function_exported(Mod, terminate, 2) of
        true -> _ = Mod:terminate(Reason, CbState), ok;
        false -> ok
    end,
    case Name of
        undefined -> ok;
        _ -> patchbay_registry:unregister(Name)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% internals
%% ------------------------------------------------------------------

call_handle_message(Mod, Msg, CbState) ->
    case erlang:function_exported(Mod, handle_message, 2) of
        true ->
            Mod:handle_message(Msg, CbState);
        false ->
            %% Optional-callback default: a no-op that keeps the agent alive.
            {ok, CbState}
    end.

%% Call-path return shaping: on {reply, R, State2} reply R; on
%% {ok, State2} reply ok; on {done, Result, State2} send the tagged
%% done message to the parent, reply ok, and stop normally.
match_result({reply, R, CbState2}, State) ->
    {reply, R, set_cbstate(State, CbState2)};
match_result({done, Result, CbState2}, State) ->
    {stop, normal, ok, send_done(set_cbstate(State, CbState2), Result)};
match_result({ok, CbState2}, State) ->
    {reply, ok, set_cbstate(State, CbState2)}.

set_cbstate(State, CbState2) ->
    State#agent{cbstate = CbState2}.

send_done(#agent{parent = Parent, ref = Ref} = State, Result) ->
    erlang:send(Parent, {patchbay_agent, done, Ref, self(), Result}),
    State.
