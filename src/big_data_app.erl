%%%-------------------------------------------------------------------
%% @doc big_data_nif public API
%% @end
%%%-------------------------------------------------------------------

-module(big_data_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    big_data_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
