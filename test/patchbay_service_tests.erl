-module(patchbay_service_tests).

-include_lib("eunit/include/eunit.hrl").

%% call_service/3: timeout control for services whose handle_message can
%% legitimately run longer than gen_server's 5s default (shell tools,
%% long evals). On timeout the caller gets {error, timeout} instead of a
%% caught exit; the service process itself is untouched and keeps
%% serving afterwards.
%%
%% Also covers metadata/0 propagation: the wrapper publishes an optional
%% callback module's metadata/0 as the registration props -- the
%% mechanism tool plugins use to advertise themselves for discovery.

with_tree(F) ->
    catch supervisor:stop('patchbay_sup'),
    {ok, Sup} = patchbay_sup:start_link(),
    try F(Sup)
    after
        catch supervisor:stop(Sup),
        receive _ -> ok after 50 -> ok end
    end.

mount(Mod, Args, Id, CtxName) ->
    {ok, Ctx} = patchbay_context:start_link(CtxName, #{}),
    {ok, Pid} = patchbay_context:mount(
                  Ctx, #{id => Id,
                         start => {patchbay_service, start_link, [Mod, Args]},
                         restart => transient,
                         type => worker}),
    Pid.

call_service_timeout_test_() ->
    {foreach,
     fun() -> catch supervisor:stop('patchbay_sup'),
              {ok, _} = patchbay_sup:start_link(), ok
     end,
     fun(_) -> catch supervisor:stop('patchbay_sup'),
               receive _ -> ok after 50 -> ok end
     end,
     [?_test(with_tree(fun(_) ->
                           mount(pb_test_echo_service, [], echo,
                                 'svc_timeout_ctx'),
                           ?assertEqual({ok, x},
                                        patchbay_service:call_service(
                                          'pb_test_echo', {echo, x})),
                           ?assertEqual({ok, slept},
                                        patchbay_service:call_service(
                                          'pb_test_echo', {sleep, 10}, 5000)),
                           ?assertEqual({error, timeout},
                                        patchbay_service:call_service(
                                          'pb_test_echo', {sleep, 2000}, 50)),
                           %% The timed-out call did not disturb the
                           %% service -- it finishes its sleep and
                           %% still answers:
                           timer:sleep(2100),
                           ?assertEqual({ok, x},
                                        patchbay_service:call_service(
                                          'pb_test_echo', {echo, x}))
                   end)),
      ?_test(with_tree(fun(_) ->
                           mount(pb_test_meta_service, [], meta,
                                 'svc_meta_ctx'),
                           ?assertMatch({ok, {_Pid, #{kind := tool}}},
                                        patchbay_registry:lookup(
                                          'pb_test_meta'))
                   end))]}.
