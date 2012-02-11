-module(bucket_store_ets_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    bucket_store_ets_sup:start_link().

stop(_State) ->
    ok.
