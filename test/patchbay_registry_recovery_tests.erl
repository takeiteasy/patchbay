-module(patchbay_registry_recovery_tests).

-include_lib("eunit/include/eunit.hrl").

%% Crash-recovery behaviour: kill the registry process under a real
%% patchbay_sup tree and prove that registrations, subscriptions and
%% monitors are rebuilt from the supervisor-owned backup table -- and
%% that entries whose pid died while the registry was down are pruned
%% with an honest `unregistered'/noproc broadcast instead of lingering
%% as ghosts.

with_tree(F) ->
    catch supervisor:stop('patchbay_sup'),
    {ok, _} = patchbay_sup:start_link(),
    try F()
    after
        catch supervisor:stop('patchbay_sup'),
        drain()
    end.

drain() ->
    receive _ -> drain()
    after 50 -> ok
    end.

await_restart(OldPid) ->
    await_restart(OldPid, 40).

await_restart(_OldPid, 0) ->
    erlang:error(registry_did_not_restart);
await_restart(OldPid, N) ->
    case whereis('patchbay_registry') of
        Pid when is_pid(Pid), Pid =/= OldPid ->
            Pid;
        _ ->
            timer:sleep(25),
            await_restart(OldPid, N - 1)
    end.

drain_registered(Name, Pid) ->
    receive
        {'patchbay_registry', registered, Name, Pid} -> ok
    after 500 ->
        erlang:error({no_replay, Name})
    end.

registrations_survive_registry_crash_test() ->
    with_tree(fun() ->
        ?assertEqual(ok,
                     'patchbay_registry':register('svc', self(), #{k => v})),
        Old = whereis('patchbay_registry'),
        exit(Old, kill),
        await_restart(Old),
        ?assertEqual({ok, {self(), #{k => v}}},
                     'patchbay_registry':lookup('svc')),
        ?assert(lists:member('svc', 'patchbay_registry':names()))
    end).

subscriptions_survive_and_notify_after_restart_test() ->
    with_tree(fun() ->
        ?assertEqual(ok, 'patchbay_registry':subscribe('y')),
        Old = whereis('patchbay_registry'),
        exit(Old, kill),
        await_restart(Old),
        %% The fresh instance still knows this process subscribes to
        %% 'y', so the new registration is delivered to it.
        ?assertEqual(ok, 'patchbay_registry':register('y', self(), #{})),
        drain_registered('y', self()),
        ?assertEqual({ok, {self(), #{}}},
                     'patchbay_registry':lookup('y'))
    end).

dead_entries_pruned_with_noproc_broadcast_test() ->
    with_tree(fun() ->
        Victim = spawn(fun() -> receive after infinity -> ok end end),
        ?assertEqual(ok, 'patchbay_registry':register('v', Victim, #{})),
        ?assertEqual(ok, 'patchbay_registry':subscribe('v')),
        drain_registered('v', Victim),
        Old = whereis('patchbay_registry'),
        %% Suspend so the victim's DOWN is queued but never processed,
        %% then destroy the registry itself: the backup table now holds
        %% an entry for a dead pid, exactly as if the pid had died
        %% during downtime.
        sys:suspend(Old),
        exit(Victim, kill),
        exit(Old, kill),
        await_restart(Old),
        receive
            {'patchbay_registry', unregistered, 'v', noproc} -> ok
        after 1000 ->
            erlang:error(expected_noproc_broadcast)
        end,
        ?assertEqual({error, not_found}, 'patchbay_registry':lookup('v'))
    end).

monitors_rebuilt_after_restart_test() ->
    with_tree(fun() ->
        V = spawn(fun() -> receive after infinity -> ok end end),
        ?assertEqual(ok, 'patchbay_registry':register('m', V, #{})),
        ?assertEqual(ok, 'patchbay_registry':subscribe('m')),
        drain_registered('m', V),
        Old = whereis('patchbay_registry'),
        exit(Old, kill),
        await_restart(Old),
        %% The fresh instance must be monitoring V on its own behalf:
        %% V's death after the restart has to reach subscribers.
        exit(V, kill),
        receive
            {'patchbay_registry', unregistered, 'm', killed} -> ok
        after 1000 ->
            erlang:error(monitor_not_restored)
        end
    end).

await_infinity_is_supported_test() ->
    with_tree(fun() ->
        Self = self(),
        Caller = spawn(fun() ->
            Self ! {result,
                    'patchbay_registry':await('later', infinity)}
        end),
        timer:sleep(50),
        Mon = erlang:monitor(process, Caller),
        ?assertEqual(ok, 'patchbay_registry':register('later', Self, #{})),
        receive
            {result, {ok, Self}} -> ok;
            {'DOWN', Mon, _, _, _} -> erlang:error(caller_died)
        after 1000 ->
            erlang:error(await_infinity_never_replied)
        end,
        %% A junk timeout raises badarg in the caller; the registry
        %% must not only survive it but keep serving calls.
        BadCaller = spawn(fun() ->
            Self ! {bad_result,
                    (catch 'patchbay_registry':await('x', not_a_timeout))}
        end),
        timer:sleep(50),
        receive
            {bad_result, {'EXIT', {badarg, [_|_]}}} -> ok
        after 500 ->
            erlang:error(expected_badarg_in_caller)
        end,
        ?assertMatch([_|_], 'patchbay_registry':names())
    end).
