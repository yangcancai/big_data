%%%-------------------------------------------------------------------
%%% @author yangcancai

%%% Copyright (c) 2021 by yangcancai(yangcancai0112@gmail.com), All Rights Reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%       https://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

%%% @doc
%%%
%%% @end
%%% Created : 2021-05-17T02:55:43+00:00
%%%-------------------------------------------------------------------
-module(big_data).

-author("yangcancai").

-include("big_data.hrl").

-export([start/0,stop/0,command/1, backend/0]).

-callback command(#bd_wal{}) -> term().
start() ->
    Backend = application:get_env(big_data, big_data_backend, redis),
    cool_tools_backend:create(?MODULE, Backend),
    ok.
stop() ->
    ok.
command(#bd_wal{action = Action, args = _Args} = Wal)
    when Action == insert;
         Action == update_counter;
         Action == update_elem;
         Action == remove_row ->
    case persistent_term:get('$bd_log_wal_started') of
        true ->
            bd_log_wal:write_sync(Wal#bd_wal{module = big_data_backend:backend()}),
            big_data_backend:command(Wal);
        false ->
            {error, bd_log_wal_not_started}
    end;
command(#bd_wal{action = _Action, args = _Args} = Wal) ->
            big_data_backend:command(Wal).

backend() ->
    big_data_backend:backend().