%%%-------------------------------------------------------------------
%% @doc big_data_nif public API
%% @end
%%%-------------------------------------------------------------------

-module(big_data_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, _} = bd_store_redis:start_link("127.0.0.1", 6379, 0, "123456", 5000),
    big_data_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
