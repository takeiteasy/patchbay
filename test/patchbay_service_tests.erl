-module(patchbay_service_tests).

-include_lib("eunit/include/eunit.hrl").

%% call_service/3: timeout control for services whose handle_message can
%% legitimately run longer than gen_server's 5s default (shell tools,
%% long evals). On timeout the caller gets {error, timeout} instead of a
%% caught exit; the service process itself is untouched and keeps
%% serving afterwards.

call_service_timeout_test_() ->
    {foreach,
     fun() ->
             catch supervisor:stop('patchbay_sup'),
             {ok, Sup} = patchbay_sup:start_link(),
             {ok, Ctx} = patchbay_context:start_link('svc_test_ctx', #{}),
             {ok, _Pid} = patchbay_context:mount(
                            Ctx, #{id => echo,
                                   start => {patchbay_service, start_link,
                                             [pb_test_echo_service, []]},
                                   restart => transient,
                                   type => worker}),
             Sup
     end,
     fun(Sup) ->
             catch supervisor:stop(Sup),
             receive _ -> ok after 50 -> ok end
     end,
     [fun(_Sup) ->
              [?_test(begin
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
                          %% service -- it finishes its sleep and still
                          %% answers:
                          timer:sleep(2100),
                          ?assertEqual({ok, x},
                                       patchbay_service:call_service(
                                         'pb_test_echo', {echo, x}))
                      end)]
     end]}.
