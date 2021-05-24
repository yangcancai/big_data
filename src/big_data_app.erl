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
    bd_counter:new(),
    Dir = application:get_env(big_data, dir, "data"),
    logger:set_application_level(big_data, debug),
    {ok, Ref} = big_data_nif:new(),
    ok = persistent_term:put(big_data, Ref),
    bd_backend_store:set_backend_store(bd_store_redis),
    {ok, _} = bd_store_redis:start_link("127.0.0.1", 6379, 0, "123456", 5000),
    {ok, _} = bd_checkpoint:start_link(#{dir => Dir}),
    {ok, P} = big_data_sup:start_link(),
    {ok, _} = bd_log_wal_sup:start_child(),
    {ok, P}.

stop(_State) ->
    bd_log_wal_sup:stop_child(),
    ok.

%% internal functions
