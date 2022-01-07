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
%%% Created : 2021-12-27T10:09:59+00:00
%%%-------------------------------------------------------------------
-module(big_data_redis).

-author("yangcancai").

-include("big_data.hrl").

-export([new/0, insert/3, insert/5, get/2, get_row/3, get_range/4, get_range_row_ids/4,
         get_row_ids/3, get_time_index/3, update_elem/4, update_counter/4, lookup_elem/4, remove/2,
         remove_row/3, remove_row_ids/4, row_id/1]).
-export([command/1]).

command(#bd_wal{action = Action, args = Args}) ->
    apply(?MODULE, Action, [?BD_BIG_DATA_REF | Args]).

new() ->
    {ok, bd_store_redis:pid()}.

insert(Ref, BigKey, R) ->
    bd_store_redis:term_cmd(Ref, ["big_data.set", BigKey, erlang:term_to_binary(R)]).

-spec insert(Ref :: big_data(),
             BigKey :: big_key(),
             RowID :: row_id(),
             Time :: t(),
             Term :: term()) ->
                ok.
insert(Ref, BigKey, RowID, Time, Term)
    when is_binary(BigKey), is_binary(RowID), is_integer(Time) ->
    bd_store_redis:term_cmd(Ref,
                            ["big_data.set",
                             BigKey,
                             erlang:term_to_binary(#row_data{row_id = RowID,
                                                             time = Time,
                                                             term = Term})]).

-spec update_elem(Ref :: big_data(),
                  BigKey :: big_key(),
                  RowID :: row_id(),
                  ElemSpecs :: elem_specs()) ->
                     [boolean()] | ?BD_NOTFOUND.
update_elem(Ref, BigKey, RowID, ElemSpecs) when is_binary(BigKey), is_binary(RowID) ->
    bd_store_redis:term_cmd(Ref,
                            ["big_data.update_elem",
                             BigKey,
                             RowID,
                             erlang:term_to_binary(ElemSpecs)]).

-spec update_counter(Ref :: big_data(),
                     BigKey :: big_key(),
                     RowID :: row_id(),
                     ElemSpecs :: elem_specs()) ->
                        [boolean()] | ?BD_NOTFOUND.
update_counter(Ref, BigKey, RowID, ElemSpecs) when is_binary(BigKey), is_binary(RowID) ->
    bd_store_redis:term_cmd(Ref,
                            ["big_data.update_counter",
                             BigKey,
                             RowID,
                             erlang:term_to_binary(ElemSpecs)]).

-spec get(Ref :: big_data(), BigKey :: big_key()) -> row_data_list() | ?BD_NOTFOUND.
get(Ref, BigKey) when is_binary(BigKey) ->
    bd_store_redis:term_cmd(Ref, ["big_data.get", BigKey]).

-spec get_row(Ref :: big_data(), BigKey :: big_key(), RowID :: row_id()) ->
                 row_data_list() | ?BD_NOTFOUND | nil.
get_row(Ref, BigKey, RowID) when is_binary(BigKey), is_binary(RowID) ->
    bd_store_redis:term_cmd(Ref, ["big_data.get_row", BigKey, RowID]).

-spec get_range(Ref :: big_data(),
                BigKey :: binary(),
                StartTime :: t(),
                EndTime :: t()) ->
                   row_data_list() | ?BD_NOTFOUND.
get_range(Ref, BigKey, StartTime, EndTime)
    when is_binary(BigKey), is_integer(StartTime), is_integer(EndTime) ->
    bd_store_redis:term_cmd(Ref, ["big_data.get_range", BigKey, StartTime, EndTime]).

-spec get_range_row_ids(Ref :: big_data(),
                        BigKey :: big_key(),
                        StartTime :: t(),
                        EndTime :: t()) ->
                           row_id_list() | ?BD_NOTFOUND.
get_range_row_ids(Ref, BigKey, StartTime, EndTime)
    when is_binary(BigKey), is_integer(StartTime), is_integer(EndTime) ->
    bd_store_redis:term_cmd(Ref, ["big_data.get_range_row_ids", BigKey, StartTime, EndTime]).

-spec get_row_ids(Ref :: big_data(), BigKey :: big_key(), Time :: t()) ->
                     row_id_list() | ?BD_NOTFOUND.
get_row_ids(Ref, BigKey, Time) when is_binary(BigKey), is_integer(Time) ->
    bd_store_redis:term_cmd(Ref, ["big_data.get_row_ids", BigKey, Time]).

-spec get_time_index(Ref :: big_data(), BigKey :: big_key(), RowID :: row_id()) ->
                        t() | ?BD_NOTFOUND.
get_time_index(Ref, BigKey, RowID) when is_binary(BigKey), is_binary(RowID) ->
    bd_store_redis:term_cmd(Ref, ["big_data.get_time_index", BigKey, RowID]).

-spec lookup_elem(Ref :: big_data(),
                  BigKey :: big_key(),
                  RowID :: row_id(),
                  ElemSpec :: elem_specs()) ->
                     tuple() | ?BD_NOTFOUND.
lookup_elem(Ref, BigKey, RowID, ElemSpecs) when is_binary(BigKey), is_binary(RowID) ->
    bd_store_redis:term_cmd(Ref,
                            ["big_data.lookup_elem",
                             BigKey,
                             RowID,
                             erlang:term_to_binary(ElemSpecs)]).

-spec remove(Ref :: big_data(), BigKey :: binary()) -> ok.
remove(Ref, BigKey) when is_binary(BigKey) ->
    bd_store_redis:term_cmd(Ref, ["big_data.remove", BigKey]).

-spec remove_row(Ref :: big_data(), BigKey :: binary(), RowID :: binary()) -> ok.
remove_row(Ref, BigKey, RowID) when is_binary(BigKey), is_binary(RowID) ->
    bd_store_redis:term_cmd(Ref, ["big_data.remove_row", BigKey, RowID]).

-spec remove_row_ids(Ref :: big_data(),
                     BigKey :: big_key(),
                     StartTime :: t(),
                     EndTime :: t()) ->
                        ok.
remove_row_ids(Ref, BigKey, StartTime, EndTime)
    when is_binary(BigKey), is_integer(StartTime), is_integer(EndTime) ->
    bd_store_redis:term_cmd(Ref, ["big_data.remove_row_ids", BigKey, StartTime, EndTime]).

row_id(RowID) when is_integer(RowID) ->
    erlang:integer_to_binary(RowID);
row_id(RowID) when is_list(RowID) ->
    erlang:list_to_binary(RowID);
row_id(RowID) ->
    RowID.
