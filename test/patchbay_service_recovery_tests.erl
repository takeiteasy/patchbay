-module(patchbay_service_recovery_tests).

-include_lib("eunit/include/eunit.hrl").

%% Service-level recovery: with a real supervision tree (context +
%% patchbay_service wrappers), kill the registry and prove that
%% dependency relationships self-heal -- the service layer itself needs
%% no recovery code, because the registry rebuilds its own bookkeeping.
%%
%% Also locks in unmount semantics: supervisor shutdown must be honored
%% promptly, running the plugin's terminate/2 disposer, not skipped via
%% brutal-kill timeout.

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

child_spec(Mod, Args) ->
    #{id => Mod,
      start => {patchbay_service, start_link, [Mod, Args]},
      restart => transient,
      type => worker}.

relationships_self_heal_after_registry_crash_test() ->
    with_tree(fun() ->
        {ok, Ctx} = patchbay_context:start_link('recovery_ctx', #{}),
        {ok, _Echo} =
            patchbay_context:mount(Ctx, child_spec(pb_test_echo_service, [])),
        {ok, _Consumer} =
            patchbay_context:mount(
              Ctx, child_spec(pb_test_consumer_service,
                              #{reporter => self()})),
        %% consumer became ready once echo registered:
        receive {pb_test_consumer, ready, _Deps} -> ok
        after 1000 -> erlang:error(never_ready) end,

        {ok, {EchoPid, _}} = 'patchbay_registry':lookup('pb_test_echo'),
        Old = whereis('patchbay_registry'),
        exit(Old, kill),
        await_restart(Old),

        %% The service processes themselves never died; the registry
        %% restored their registration from the backup table:
        ?assert(is_process_alive(EchoPid)),
        ?assertEqual({ok, {EchoPid, #{}}},
                     'patchbay_registry':lookup('pb_test_echo')),

        %% Both directions of dependency monitoring survived: killing
        %% echo reaches the consumer as dep_down, echo restarts under
        %% its context, re-registers, and the consumer goes ready again.
        exit(EchoPid, kill),
        receive {pb_test_consumer, dep_down, 'pb_test_echo', killed} -> ok
        after 1000 -> erlang:error(dep_not_observed) end,
        receive {pb_test_consumer, ready, _Deps2} -> ok
        after 1000 -> erlang:error(no_re_ready_after_restart) end,

        %% And the whole stack still serves calls end to end.
        ?assertEqual({ok, pong},
                     patchbay_service:call_service('pb_test_consumer', ping))
    end).

unmount_is_prompt_and_runs_disposer_test() ->
    with_tree(fun() ->
        {ok, Ctx} = patchbay_context:start_link('unmount_ctx', #{}),
        {ok, EchoPid} =
            patchbay_context:mount(
              Ctx, child_spec(pb_test_echo_service, [self()])),
        T0 = erlang:monotonic_time(millisecond),
        ?assertEqual(ok, supervisor:terminate_child(Ctx, pb_test_echo_service)),
        Elapsed = erlang:monotonic_time(millisecond) - T0,
        ?assert(Elapsed < 2000),
        receive
            {pb_test_echo_disposed, EchoPid} -> ok
        after 500 ->
            erlang:error(disposer_never_ran)
        end,
        ?assertEqual({error, not_found},
                     'patchbay_registry':lookup('pb_test_echo'))
    end).
