-module(pb_test_echo_service).

-behaviour(patchbay_service).

-export([service_name/0, dependencies/0, init/1, handle_message/2,
         terminate/2]).

%% Minimal dependency-free service used by recovery tests. When started
%% with Args = [ReporterPid], terminate reports back so tests can prove
%% the dispose path actually ran.

service_name() -> 'pb_test_echo'.
dependencies() -> [].

init([Reporter]) when is_pid(Reporter) ->
    {ok, #{reporter => Reporter}};
init([]) ->
    {ok, #{reporter => undefined}}.

handle_message({echo, Msg}, State) ->
    {reply, {ok, Msg}, State};
handle_message(_Msg, State) ->
    {reply, {error, bad_message}, State}.

terminate(_Reason, #{reporter := Reporter}) ->
    case is_pid(Reporter) of
        true -> Reporter ! {pb_test_echo_disposed, self()};
        false -> ok
    end,
    ok.
