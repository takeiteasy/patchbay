-module(pb_test_meta_service).

-behaviour(patchbay_service).

-export([service_name/0, dependencies/0, init/1, metadata/0,
         handle_message/2]).

%% Minimal service that publishes metadata/0 -- proves the wrapper
%% forwards it to the registry as registration props (the mechanism
%% tool plugins use to advertise themselves for discovery).

service_name() -> 'pb_test_meta'.
dependencies() -> [].

init([]) ->
    {ok, #{}}.

metadata() ->
    #{kind => tool,
      summary => <<"test fixture metadata">>}.

handle_message(_Msg, State) ->
    {reply, ok, State}.
