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
%%% Created : 2021-05-17T03:57:05+00:00
%%%-------------------------------------------------------------------
-module(bd_store_redis).

-behaviour(bd_backend_store).

-author("yangcancai").

-include("big_data.hrl").

-export([start_link/5, handle_get/1, pid/0, handle_put/2, handle_put/1, handle_del/1,
         cmd/1, term_cmd/2]).

start_link(Ip, Port, Pwd, Db, ReconnSleep) ->
    {ok, Pid} = eredis:start_link(Ip, Port, Pwd, Db, ReconnSleep),
    ok = persistent_term:put(?MODULE, Pid),
    {ok, Pid}.

-spec handle_get(BigKey :: big_key()) -> row_data_list().
handle_get(BigKey) ->
    case eredis:q(pid(), ["GET", BigKey]) of
        {ok, undefined} ->
            [];
        {ok, Result} ->
            erlang:binary_to_term(Result)
    end.

-spec handle_del(BigKey :: big_key()) -> ok.
handle_del(BigKey) ->
    {ok, _} = eredis:q(pid(), ["DEL", BigKey]),
    ok.

-spec handle_put(BigKey :: big_key(), RowDataList :: row_data_list()) -> ok.
handle_put(BigKey, RowDataList) ->
    {ok, <<"OK">>} = eredis:q(pid(), ["SET", BigKey, erlang:term_to_binary(RowDataList)]),
    ok.

handle_put(Chunk) ->
    CmdList =
        lists:flatten([[BigKey, erlang:term_to_binary(RowDataList)]
                       || {BigKey, RowDataList} <- Chunk]),
    {ok, <<"OK">>} = eredis:q(pid(), ["MSET" | CmdList], 60000),
    ok.

pid() ->
    persistent_term:get(?MODULE).

cmd(L) ->
    eredis:q(pid(), L).

term_cmd(P, L) ->
    case eredis:q(P, L) of
        {ok, <<"OK">>} ->
            ok;
        {ok, Binary} when is_binary(Binary) ->
            erlang:binary_to_term(Binary);
        V ->
            V
    end.
