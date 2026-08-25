-module(patchbay_registry_tests).

-include_lib("eunit/include/eunit.hrl").

%% Tests for the registry's concurrency-critical behaviour, ported from
%% the original LFE ltest suite when patchbay became a standalone,
%% pure-Erlang library.
%%
%% The registry is a singleton registered under the fixed local name
%% 'patchbay_registry' (its client API hardcodes that name), so tests
%% share one instance rather than running isolated. with_registry/1
%% gives each test a fresh instance and guarantees teardown via
%% try/after, so a crashing test doesn't leave a stale registration
%% wedging every test that runs after it. It also drains the calling
%% process's mailbox after teardown: eunit runs each test function in
%% its own process, but a notification still in flight when a test
%% finishes would otherwise be delivered to whatever process reuses the
%% name next -- cheap to prevent.

with_registry(F) ->
    case whereis('patchbay_registry') of
        undefined -> ok;
        Pid -> catch gen_server:stop(Pid)
    end,
    {ok, _} = 'patchbay_registry':start_link(),
    try F()
    after
        catch gen_server:stop('patchbay_registry'),
        drain()
    end.

drain() ->
    receive _ -> drain()
    after 50 -> ok
    end.

%% ------------------------------------------------------------------
%% register / lookup / unregister
%% ------------------------------------------------------------------

register_lookup_unregister_roundtrip_test() ->
    with_registry(fun() ->
        ?assertEqual(ok, 'patchbay_registry':register('svc', self(), #{k => v})),
        ?assertEqual({ok, {self(), #{k => v}}},
                     'patchbay_registry':lookup('svc')),
        ?assertEqual(ok, 'patchbay_registry':unregister('svc')),
        ?assertEqual({error, not_found}, 'patchbay_registry':lookup('svc'))
    end).

duplicate_registration_of_live_pid_is_an_error_test() ->
    with_registry(fun() ->
        'patchbay_registry':register('svc', self(), #{}),
        ?assertMatch({error, {already_registered, _}},
                     'patchbay_registry':register('svc', self(), #{}))
    end).

%% ------------------------------------------------------------------
%% await
%% ------------------------------------------------------------------

await_returns_immediately_when_already_present_test() ->
    with_registry(fun() ->
        'patchbay_registry':register('svc', self(), #{}),
        ?assertEqual({ok, self()}, 'patchbay_registry':await('svc', 1000))
    end).

await_timeout_leaves_no_leak_test() ->
    with_registry(fun() ->
        ?assertEqual({error, timeout},
                     'patchbay_registry':await('nope', 100)),
        State = sys:get_state('patchbay_registry'),
        ?assertEqual(#{}, maps:get(waiters, State)),
        ?assertEqual(#{}, maps:get(timer_waiter, State)),
        ?assertEqual(#{}, maps:get(callermon_waiter, State))
    end).

await_caller_death_is_reaped_test() ->
    with_registry(fun() ->
        Caller = spawn(fun() -> 'patchbay_registry':await('nope_yet', 5000) end),
        timer:sleep(50),
        exit(Caller, kill),
        timer:sleep(50),
        State = sys:get_state('patchbay_registry'),
        ?assertEqual(#{}, maps:get(waiters, State)),
        ?assertEqual(#{}, maps:get(callermon_waiter, State)),
        ?assertEqual(#{}, maps:get(timer_waiter, State))
    end).

%% ------------------------------------------------------------------
%% subscribe / notify
%% ------------------------------------------------------------------

subscribe_replays_existing_registration_immediately_test() ->
    with_registry(fun() ->
        'patchbay_registry':register('svc', self(), #{}),
        'patchbay_registry':subscribe('svc'),
        receive
            {'patchbay_registry', registered, 'svc', Pid} ->
                ?assertEqual(self(), Pid)
        after 200 ->
            ?assert(false)
        end
    end).

subscribe_to_absent_name_sends_nothing_until_registered_test() ->
    with_registry(fun() ->
        'patchbay_registry':subscribe('svc'),
        receive
            _ -> ?assert(false)
        after 100 ->
            ok
        end,
        'patchbay_registry':register('svc', self(), #{}),
        receive
            {'patchbay_registry', registered, 'svc', Pid} ->
                ?assertEqual(self(), Pid)
        after 200 ->
            ?assert(false)
        end
    end).

subscriber_death_drops_its_subscriptions_test() ->
    with_registry(fun() ->
        Sub = spawn(fun() ->
                            'patchbay_registry':subscribe('svc'),
                            timer:sleep(5000)
                    end),
        timer:sleep(50),
        exit(Sub, kill),
        timer:sleep(50),
        State = sys:get_state('patchbay_registry'),
        ?assertEqual(#{}, maps:get(subs, State)),
        ?assertEqual(#{}, maps:get(sub_mon, State)),
        ?assertEqual(#{}, maps:get(mon_sub, State))
    end).

%% ------------------------------------------------------------------
%% registered-pid death cleans up the registration too
%% ------------------------------------------------------------------

registered_pid_death_deregisters_and_notifies_subs_test() ->
    with_registry(fun() ->
        %% Subscribe before registering, so the only replay we get is
        %% the real registration -- not subscribe's immediate-replay
        %% path (already covered above), which would otherwise be the
        %% first message in the mailbox and mask the one this test is
        %% about.
        'patchbay_registry':subscribe('svc'),
        SvcPid = spawn(fun() -> receive _ -> ok end end),
        'patchbay_registry':register('svc', SvcPid, #{}),
        receive
            {'patchbay_registry', registered, 'svc', Pid} ->
                ?assertEqual(SvcPid, Pid)
        after 200 ->
            ?assert(false)
        end,
        exit(SvcPid, kill),
        receive
            {'patchbay_registry', unregistered, 'svc', Reason} ->
                ?assertEqual(killed, Reason)
        after 500 ->
            ?assert(false)
        end,
        ?assertEqual({error, not_found}, 'patchbay_registry':lookup('svc'))
    end).

%% ------------------------------------------------------------------
%% re-registration racing the old pid's DOWN
%%
%% A supervisor restarting a dead child can complete -- and the new
%% child can call register/3 -- before the registry has processed its
%% own monitor DOWN for the old pid (both are independent, async signal
%% paths triggered by the same kill). do_register_fresh must handle
%% this regardless of which message wins the race, or the eventual
%% stale DOWN tears down the live new registration and wrongly
%% broadcasts `unregistered` for a process that's still alive. This is
%% forced deterministically with sys:suspend/resume rather than relying
%% on the ordinary timing (which happened, in practice, to almost
%% always favor DOWN-first and mask the bug during manual testing).
%% ------------------------------------------------------------------

re_registration_survives_a_losing_race_against_old_down_test() ->
    with_registry(fun() ->
        OldPid = spawn(fun() -> receive _ -> ok end end),
        'patchbay_registry':register('svc', OldPid, #{}),
        'patchbay_registry':subscribe('svc'),
        receive
            {'patchbay_registry', registered, 'svc', _} -> ok
        after 200 ->
            ?assert(false)
        end,
        NewPid = spawn(fun() -> receive _ -> ok end end),
        sys:suspend('patchbay_registry'),
        %% Both now queue up behind the suspend, in this order: the new
        %% registration lands in the mailbox first, the old pid's DOWN
        %% second -- the losing order for the naive implementation.
        spawn(fun() -> 'patchbay_registry':register('svc', NewPid, #{}) end),
        timer:sleep(50),
        exit(OldPid, kill),
        timer:sleep(50),
        sys:resume('patchbay_registry'),
        timer:sleep(100),
        ?assertEqual({ok, {NewPid, #{}}},
                     'patchbay_registry':lookup('svc')),
        State = sys:get_state('patchbay_registry'),
        ?assertEqual(1, maps:size(maps:get(mon_reg, State))),
        ?assertEqual(1, maps:size(maps:get(reg_mon, State))),
        %% No spurious `unregistered` for the still-live new pid.
        receive
            {'patchbay_registry', unregistered, 'svc', _} -> ?assert(false)
        after 200 ->
            ok
        end
    end).
