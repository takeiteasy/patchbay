-module(patchbay_app).

-behaviour(application).

-export([start/2, stop/1]).

%% OTP application callback for the `patchbay` core. Starts the root
%% supervisor, which in turn starts the registry (see patchbay_sup).

start(_Type, _Args) ->
    patchbay_sup:start_link().

stop(_State) ->
    ok.
