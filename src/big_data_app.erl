%%%-------------------------------------------------------------------
%% @doc big_data_nif public API
%% @end
%%%-------------------------------------------------------------------

-module(big_data_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = persistent_term:put('$bd_logger', logger),
    ok = bd_ets:new(),
    logger:set_application_level(big_data, debug),
    {ok, Ref} = big_data_nif:new(),
    ok = persistent_term:put(big_data, Ref),
    {ok, _} = bd_store_redis:start_link("127.0.0.1", 6379, 0, "123456", 5000),

    big_data_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
