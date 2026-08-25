-module(patchbay_registry).

-behaviour(gen_server).

%% Client API
-export([start_link/0, register/3, unregister/1, lookup/1, await/2,
         subscribe/1, unsubscribe/1, names/0]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, patchbay_registry).
-define(BACKUP_TAB, patchbay_registry_backup).

%% The service registry: the piece that lets a plugin be mounted before
%% its dependency exists, and be told about it when it appears.
%%
%% This is a hand-rolled gen_server rather than a pull of gproc, so the
%% core stays at zero non-OTP dependencies -- consistent with the
%% project's "BEAM-native" thesis. Everything here is reached only
%% through the client API below, so a gproc-backed implementation could
%% be swapped in later without touching callers.
%%
%% Notification protocol sent to subscribers:
%%   {patchbay_registry, registered,   Name, Pid}
%%   {patchbay_registry, unregistered, Name, Reason}
%%
%% `subscribe` is atomic with respect to an existing registration: if
%% `Name` is already registered at the moment of subscribing, the
%% subscriber is sent a `registered` message immediately, before
%% `subscribe` returns. This removes the lookup-then-subscribe race by
%% construction -- callers (see patchbay_service) never need to call
%% `lookup` themselves.
%%
%% `await` owns its own timeout server-side via `erlang:start_timer/3`
%% rather than relying on the caller's `gen_server:call` timeout. If
%% the client timeout fired instead, a name that registers after the
%% client gave up would still find a stale `From` sitting in `waiters`
%% forever, and the server would try to reply into a caller that's no
%% longer listening. Client calls are made with `infinity` -- the
%% server-side timer is what actually bounds the wait. `await(Name,
%% infinity)` is supported and means exactly that: park until the name
%% registers, with no deadline.
%%
%% The state is deliberately a plain map (not a record) with one key per
%% index; the tests inspect it directly via sys:get_state/1.
%%
%% Crash recovery: the registry writes every registration and
%% subscription through to a public ETS table `patchbay_registry_backup`
%% that is owned by patchbay_sup, so it survives a one_for_one restart
%% of the registry process itself. On init the fresh instance rebuilds
%% its indexes from that table: entries whose pid died while the
%% registry was down are pruned and their subscribers are sent an
%% `unregistered' notification with reason `noproc', while surviving
%% pids get fresh monitors. Scope: this makes the registry resilient to
%% ITS OWN crashes only. The table dies with the supervisor (i.e. with
%% the application), which is correct -- pids recorded in it are
%% meaningless after an application or VM restart. In-flight `await'
%% waiters are likewise not persisted; their callers fail when the
%% registry process dies (standard gen_server call semantics). A bare
%% start_link/0 without the supervisor around creates a fallback table
%% owned by the registry process itself, degrading to no recovery.

%% ------------------------------------------------------------------
%% Client API
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register(Name, Pid, Props) ->
    gen_server:call(?SERVER, {register, Name, Pid, Props}).

unregister(Name) ->
    gen_server:call(?SERVER, {unregister, Name}).

lookup(Name) ->
    gen_server:call(?SERVER, {lookup, Name}).

await(Name, Timeout) when Timeout =:= infinity;
                          is_integer(Timeout), Timeout >= 0 ->
    gen_server:call(?SERVER, {await, Name, Timeout}, infinity);
await(_Name, Timeout) ->
    erlang:error(badarg, [Timeout]).

subscribe(Name) ->
    gen_server:call(?SERVER, {subscribe, Name, self()}).

unsubscribe(Name) ->
    gen_server:call(?SERVER, {unsubscribe, Name, self()}).

names() ->
    gen_server:call(?SERVER, names).

%% ------------------------------------------------------------------
%% gen_server callbacks
%% ------------------------------------------------------------------

init([]) ->
    ensure_backup_table(),
    {State1, DeadNames} = restore_registrations(new_state()),
    State2 = restore_subscriptions(State1),
    %% Subscribers are restored by now, so pruned registrations can
    %% reach everyone who still cares -- mirroring what the DOWN would
    %% have delivered had the registry been alive when the pid died.
    State3 = lists:foldl(
               fun(Name, Acc) ->
                       notify_subs(Name, {?SERVER, unregistered, Name,
                                          noproc},
                                   Acc)
               end, State2, DeadNames),
    {ok, State3}.

handle_call({register, Name, Pid, Props}, _From, State) ->
    do_register(Name, Pid, Props, State);
handle_call({unregister, Name}, _From, State) ->
    do_unregister(Name, unregistered, State);
handle_call({lookup, Name}, _From, State) ->
    {reply, do_lookup(Name, State), State};
handle_call({await, Name, Timeout}, From, State) ->
    do_await(Name, Timeout, From, State);
handle_call({subscribe, Name, Pid}, _From, State) ->
    {reply, ok, do_subscribe(Name, Pid, State)};
handle_call({unsubscribe, Name, Pid}, _From, State) ->
    {reply, ok, do_unsubscribe(Name, Pid, State)};
handle_call(names, _From, State) ->
    {reply, maps:keys(maps:get(regs, State)), State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Mon, process, _Pid, Reason}, State) ->
    handle_down(Mon, Reason, State);
handle_info({timeout, TimerRef, timeout}, State) ->
    handle_waiter_timeout(TimerRef, State);
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% state
%% ------------------------------------------------------------------
%%
%% regs             :: #{Name => {Pid, Props}}
%% reg_mon          :: #{Name => MonRef}          monitor on a registered pid
%% mon_reg          :: #{MonRef => Name}          reverse, for DOWN handling
%% waiters          :: #{Name => [{From, TimerRef, CallerMonRef}]}
%% timer_waiter     :: #{TimerRef => {Name, From}}
%% callermon_waiter :: #{CallerMonRef => {Name, From, TimerRef}}
%% subs             :: #{Name => sets:set(Pid)}
%% sub_mon          :: #{Pid => MonRef}           one monitor per subscriber
%% mon_sub          :: #{MonRef => Pid}           reverse, for DOWN handling

new_state() ->
    #{regs => #{},
      reg_mon => #{},
      mon_reg => #{},
      waiters => #{},
      timer_waiter => #{},
      callermon_waiter => #{},
      subs => #{},
      sub_mon => #{},
      mon_sub => #{}}.

%% Tiny map helpers -- keep the code below legible without a record.
sget(State, Key) -> maps:get(Key, State).
sput(State, Key, Val) -> maps:put(Key, Val, State).

%% ------------------------------------------------------------------
%% backup table -- write-through state for crash recovery
%% ------------------------------------------------------------------
%%
%% Row shapes mirror the two durable parts of the state:
%%   {{reg, Name}, {Pid, Props}}
%%   {{sub, Name}, sets:set(Pid)}
%% Everything else (monitors, waiters) is rebuilt or deliberately
%% dropped at init; see the module header.

ensure_backup_table() ->
    case ets:whereis(?BACKUP_TAB) of
        undefined ->
            %% Degraded bare-mode fallback: owned by this process, so it
            %% dies with us and a restart starts empty. patchbay_sup
            %% normally owns the table and outlives registry crashes.
            ets:new(?BACKUP_TAB, [named_table, public, set]);
        _Tab ->
            ok
    end.

backup_put_reg(Name, Pid, Props) ->
    true = ets:insert(?BACKUP_TAB, {{reg, Name}, {Pid, Props}}),
    ok.

backup_del_reg(Name) ->
    true = ets:delete(?BACKUP_TAB, {reg, Name}),
    ok.

backup_put_sub(Name, PidsSet) ->
    true = ets:insert(?BACKUP_TAB, {{sub, Name}, PidsSet}),
    ok.

backup_del_sub(Name) ->
    true = ets:delete(?BACKUP_TAB, {sub, Name}),
    ok.

restore_registrations(State) ->
    Rows = ets:match_object(?BACKUP_TAB, {{reg, '$1'}, '$2'}),
    lists:foldl(
      fun({{reg, Name}, {Pid, Props}}, {Acc, Dead}) ->
              case erlang:is_process_alive(Pid) of
                  true ->
                      Mon = erlang:monitor(process, Pid),
                      Acc1 = sput(sput(sput(Acc,
                                           regs,
                                           maps:put(Name, {Pid, Props},
                                                    sget(Acc, regs))),
                                      reg_mon,
                                      maps:put(Name, Mon,
                                               sget(Acc, reg_mon))),
                                 mon_reg,
                                 maps:put(Mon, Name, sget(Acc, mon_reg))),
                      {Acc1, Dead};
                  false ->
                      backup_del_reg(Name),
                      {Acc, [Name | Dead]}
              end
      end, {State, []}, Rows).

restore_subscriptions(State0) ->
    Rows = ets:match_object(?BACKUP_TAB, {{sub, '$1'}, '$2'}),
    lists:foldl(
      fun({{sub, Name}, PidsSet}, Acc) ->
              Live = sets:filter(fun erlang:is_process_alive/1, PidsSet),
              case sets:size(Live) of
                  0 ->
                      backup_del_sub(Name),
                      Acc;
                  _ ->
                      backup_put_sub(Name, Live),
                      Acc1 = sput(Acc, subs,
                                  maps:put(Name, Live, sget(Acc, subs))),
                      sets:fold(fun ensure_sub_monitor/2, Acc1, Live)
              end
      end, State0, Rows).

%% ------------------------------------------------------------------
%% register / unregister / lookup
%% ------------------------------------------------------------------

do_register(Name, Pid, Props, State) ->
    case maps:find(Name, sget(State, regs)) of
        {ok, {ExistingPid, _Props}} ->
            case erlang:is_process_alive(ExistingPid) of
                true ->
                    {reply, {error, {already_registered, ExistingPid}}, State};
                false ->
                    do_register_fresh(Name, Pid, Props, State)
            end;
        error ->
            do_register_fresh(Name, Pid, Props, State)
    end.

do_register_fresh(Name, Pid, Props, State0) ->
    %% drop_registration first: if `Name` is being re-registered (a dead
    %% pid's replacement raced ahead of that pid's own DOWN -- e.g. a
    %% supervisor restart completing before the registry got around to
    %% processing the old monitor), this clears the OLD monitor's
    %% reg_mon/mon_reg entries before the new ones go in. Without it,
    %% mon_reg[OldMonRef] survives pointing at `Name`; when the old DOWN
    %% eventually arrives, handle_down matches on that stale ref, looks up
    %% regs[Name] (now the live NEW registration), and tears it down --
    %% broadcasting `unregistered` for a process that's still alive.
    %% demonitor(_, [flush]) also drops an already-queued DOWN from this
    %% process's own mailbox, so this is correct regardless of which
    %% arrives first, not just correct when DOWN happens to win the race.
    State1 = drop_registration(Name, State0),
    Mon = erlang:monitor(process, Pid),
    State2 = sput(sput(sput(State1,
                           regs, maps:put(Name, {Pid, Props}, sget(State1, regs))),
                       reg_mon, maps:put(Name, Mon, sget(State1, reg_mon))),
                  mon_reg, maps:put(Mon, Name, sget(State1, mon_reg))),
    State3 = reply_waiters(Name, Pid, State2),
    State4 = notify_subs(Name, {?SERVER, registered, Name, Pid}, State3),
    ok = backup_put_reg(Name, Pid, Props),
    {reply, ok, State4}.

do_unregister(Name, Reason, State) ->
    case maps:find(Name, sget(State, regs)) of
        error ->
            {reply, ok, State};
        {ok, _Entry} ->
            State1 = drop_registration(Name, State),
            State2 = notify_subs(Name,
                                 {?SERVER, unregistered, Name, Reason},
                                 State1),
            {reply, ok, State2}
    end.

drop_registration(Name, State0) ->
    Mon = maps:get(Name, sget(State0, reg_mon), undefined),
    undefined =/= Mon andalso erlang:demonitor(Mon, [flush]),
    ok = backup_del_reg(Name),
    State1 = sput(State0, regs, maps:remove(Name, sget(State0, regs))),
    State2 = sput(State1, reg_mon, maps:remove(Name, sget(State1, reg_mon))),
    sput(State2, mon_reg,
         case Mon of
             undefined -> sget(State2, mon_reg);
             _ -> maps:remove(Mon, sget(State2, mon_reg))
         end).

do_lookup(Name, State) ->
    case maps:find(Name, sget(State, regs)) of
        {ok, Entry} -> {ok, Entry};
        error -> {error, not_found}
    end.

%% ------------------------------------------------------------------
%% await (server-owned timeout)
%% ------------------------------------------------------------------

do_await(Name, Timeout, From, State) ->
    case maps:find(Name, sget(State, regs)) of
        {ok, {Pid, _Props}} ->
            {reply, {ok, Pid}, State};
        error ->
            %% `infinity' means "no server-side deadline": no timer is
            %% started, so a caller waiting on a name that never
            %% registers waits forever. Non-negative integers go to
            %% start_timer; anything else is rejected by the guard on
            %% the client-side await/2, so it raises badarg in the
            %% CALLER and never reaches (or kills) this server.
            TimerRef = case Timeout of
                           infinity -> undefined;
                           T -> erlang:start_timer(T, self(), timeout)
                       end,
            {CallerPid, _Tag} = From,
            CallerMon = erlang:monitor(process, CallerPid),
            Entry = {From, TimerRef, CallerMon},
            Existing = maps:get(Name, sget(State, waiters), []),
            {noreply,
             sput(sput(sput(State,
                            waiters,
                            maps:put(Name, [Entry | Existing],
                                     sget(State, waiters))),
                       timer_waiter,
                       maps:put(TimerRef, {Name, From},
                                sget(State, timer_waiter))),
                  callermon_waiter,
                  maps:put(CallerMon, {Name, From, TimerRef},
                           sget(State, callermon_waiter)))}
    end.

reply_waiters(Name, Pid, State0) ->
    Entries = maps:get(Name, sget(State0, waiters), []),
    lists:foldl(
      fun({From, TimerRef, CallerMon}, Acc) ->
              cancel_waiter_timer(TimerRef),
              erlang:demonitor(CallerMon, [flush]),
              gen_server:reply(From, {ok, Pid}),
              sput(sput(Acc,
                        timer_waiter,
                        maps:remove(TimerRef, sget(Acc, timer_waiter))),
                   callermon_waiter,
                   maps:remove(CallerMon, sget(Acc, callermon_waiter)))
      end,
      sput(State0, waiters, maps:remove(Name, sget(State0, waiters))),
      Entries).

handle_waiter_timeout(TimerRef, State) ->
    case maps:find(TimerRef, sget(State, timer_waiter)) of
        error ->
            {noreply, State};
        {ok, {Name, From}} ->
            gen_server:reply(From, {error, timeout}),
            {noreply, drop_waiter(Name, From, TimerRef, State)}
    end.

cancel_waiter_timer(undefined) ->
    ok;
cancel_waiter_timer(TimerRef) ->
    erlang:cancel_timer(TimerRef).

drop_waiter(Name, From, TimerRef, State0) ->
    Waiters = maps:get(Name, sget(State0, waiters), []),
    Remaining = [E || E = {F, _, _} <- Waiters, F =/= From],
    Removed = [E || E = {F, _, _} <- Waiters, F =:= From],
    State1 =
        sput(State0, waiters,
             case Remaining of
                 [] -> maps:remove(Name, sget(State0, waiters));
                 [_|_] -> maps:put(Name, Remaining, sget(State0, waiters))
             end),
    State2 = sput(State1, timer_waiter,
                  maps:remove(TimerRef, sget(State1, timer_waiter))),
    lists:foldl(
      fun({_F, TRef, CallerMon}, Acc) ->
              cancel_waiter_timer(TRef),
              erlang:demonitor(CallerMon, [flush]),
              sput(Acc, callermon_waiter,
                   maps:remove(CallerMon, sget(Acc, callermon_waiter)))
      end, State2, Removed).

%% ------------------------------------------------------------------
%% subscribe / unsubscribe
%% ------------------------------------------------------------------

do_subscribe(Name, Pid, State0) ->
    Current = maps:get(Name, sget(State0, subs), sets:new()),
    State1 =
        case sets:is_element(Pid, Current) of
            true ->
                State0;
            false ->
                Updated = sets:add_element(Pid, Current),
                ok = backup_put_sub(Name, Updated),
                StateA = sput(State0, subs,
                              maps:put(Name, Updated, sget(State0, subs))),
                ensure_sub_monitor(Pid, StateA)
        end,
    %% Replay an existing registration immediately, whether or not this
    %% subscribe call added a new subscriber -- that replay is what makes
    %% lookup-then-subscribe unnecessary (see module header).
    case maps:find(Name, sget(State1, regs)) of
        {ok, {RegPid, _Props}} ->
            erlang:send(Pid, {?SERVER, registered, Name, RegPid}),
            State1;
        error ->
            State1
    end.

ensure_sub_monitor(Pid, State) ->
    case maps:is_key(Pid, sget(State, sub_mon)) of
        true ->
            State;
        false ->
            Mon = erlang:monitor(process, Pid),
            sput(sput(State,
                      sub_mon, maps:put(Pid, Mon, sget(State, sub_mon))),
                 mon_sub, maps:put(Mon, Pid, sget(State, mon_sub)))
    end.

do_unsubscribe(Name, Pid, State) ->
    case maps:get(Name, sget(State, subs), undefined) of
        undefined ->
            State;
        Current ->
            Updated = sets:del_element(Pid, Current),
            State1 =
                case sets:size(Updated) of
                    0 -> ok = backup_del_sub(Name),
                         sput(State, subs, maps:remove(Name, sget(State, subs)));
                    _ -> ok = backup_put_sub(Name, Updated),
                         sput(State, subs, maps:put(Name, Updated,
                                                    sget(State, subs)))
                end,
            maybe_drop_sub_monitor(Pid, State1)
    end.

still_subscribed(Pid, State) ->
    maps:fold(fun(_Name, _Pids, true) -> true;
                 (_Name, Pids, false) -> sets:is_element(Pid, Pids)
              end, false, sget(State, subs)).

maybe_drop_sub_monitor(Pid, State) ->
    case still_subscribed(Pid, State) of
        true ->
            State;
        false ->
            case maps:find(Pid, sget(State, sub_mon)) of
                error ->
                    State;
                {ok, Mon} ->
                    erlang:demonitor(Mon, [flush]),
                    sput(sput(State,
                              sub_mon, maps:remove(Pid, sget(State, sub_mon))),
                         mon_sub, maps:remove(Mon, sget(State, mon_sub)))
            end
    end.

notify_subs(Name, Msg, State) ->
    Pids = sets:to_list(maps:get(Name, sget(State, subs), sets:new())),
    lists:foreach(fun(P) -> erlang:send(P, Msg) end, Pids),
    State.

drop_all_subs_for(Pid, State0) ->
    Names = maps:keys(sget(State0, subs)),
    State1 =
        lists:foldl(
          fun(Name, Acc) ->
                  case maps:get(Name, sget(Acc, subs), undefined) of
                      undefined ->
                          Acc;
                      Current ->
                          Updated = sets:del_element(Pid, Current),
                          case sets:size(Updated) of
                              0 -> ok = backup_del_sub(Name),
                                   sput(Acc, subs,
                                        maps:remove(Name, sget(Acc, subs)));
                              _ -> ok = backup_put_sub(Name, Updated),
                                   sput(Acc, subs,
                                        maps:put(Name, Updated,
                                                 sget(Acc, subs)))
                          end
                  end
          end, State0, Names),
    sput(State1, sub_mon, maps:remove(Pid, sget(State1, sub_mon))).

%% ------------------------------------------------------------------
%% DOWN handling -- registered pids, waiting callers, and subscribers
%% all share the same monitor-message stream, disambiguated by which
%% reverse-index map the ref shows up in.
%% ------------------------------------------------------------------

handle_down(Mon, Reason, State)
  when is_map_key(Mon, map_get(mon_reg, State)) ->
    Name = maps:get(Mon, sget(State, mon_reg)),
    {ok, {_Pid, _Props}} = maps:find(Name, sget(State, regs)),
    %% Same teardown path as an explicit unregister (including the
    %% backup-table row removal); the demonitor of our own just-fired
    %% monitor with [flush] also swallows the DOWN we are handling.
    State1 = drop_registration(Name, State),
    State2 = notify_subs(Name, {?SERVER, unregistered, Name, Reason}, State1),
    {noreply, State2};
handle_down(Mon, _Reason, State)
  when is_map_key(Mon, map_get(mon_sub, State)) ->
    Pid = maps:get(Mon, sget(State, mon_sub)),
    State1 = sput(State, mon_sub, maps:remove(Mon, sget(State, mon_sub))),
    {noreply, drop_all_subs_for(Pid, State1)};
handle_down(Mon, _Reason, State)
  when is_map_key(Mon, map_get(callermon_waiter, State)) ->
    {Name, From, TimerRef} = maps:get(Mon, sget(State, callermon_waiter)),
    cancel_waiter_timer(TimerRef),
    State1 = sput(sput(State,
                       timer_waiter,
                       maps:remove(TimerRef, sget(State, timer_waiter))),
                  callermon_waiter,
                  maps:remove(Mon, sget(State, callermon_waiter))),
    Waiters = maps:get(Name, sget(State1, waiters), []),
    Remaining = [E || E = {F, _, _} <- Waiters, F =/= From],
    State2 =
        sput(State1, waiters,
             case Remaining of
                 [] -> maps:remove(Name, sget(State1, waiters));
                 [_|_] -> maps:put(Name, Remaining, sget(State1, waiters))
             end),
    {noreply, State2};
handle_down(_Mon, _Reason, State) ->
    {noreply, State}.
