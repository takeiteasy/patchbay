-module(pb_test_consumer_service).

-behaviour(patchbay_service).

-export([service_name/0, dependencies/0, init/1, ready/2, dep_down/3,
         handle_message/2]).

%% Consumer fixture depending on pb_test_echo. Reports lifecycle
%% transitions to a Reporter pid passed in Args so tests can observe
%% exactly what the service layer did across a registry restart --
%% including whether relationships survive it.

service_name() -> 'pb_test_consumer'.
dependencies() -> ['pb_test_echo'].

init(#{reporter := Reporter}) ->
    {ok, #{reporter => Reporter}}.

ready(Deps, #{reporter := Reporter} = State) ->
    Reporter ! {pb_test_consumer, ready, Deps},
    {ok, State}.

dep_down(Name, Reason, #{reporter := Reporter} = State) ->
    Reporter ! {pb_test_consumer, dep_down, Name, Reason},
    {ok, State}.

handle_message(ping, State) ->
    {reply, {ok, pong}, State};
handle_message(_Msg, State) ->
    {reply, {error, bad_message}, State}.
