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

-export([insert/5, get/2, get_row/3, get_range/4, get_range_row_ids/4, get_row_ids/3,
         get_time_index/3, update_elem/4, update_counter/4, lookup_elem/4, remove/2, remove_row/3,
         remove_row_ids/4, clear/1, reload/2, command/1]).

command(#bd_wal{action = Action, args = Args} = Wal)
    when Action == insert;
         Action == update_counter;
         Action == update_elem;
         Action == remove_row ->
    bd_log_wal:write(Wal),
    apply(Action, Args);
command(#bd_wal{action = Action, args = Args}) ->
    apply(Action, Args).

-spec insert(Ref :: big_data(),
             BigKey :: big_key(),
             RowID :: row_id(),
             Time :: t(),
             Term :: term()) ->
                ok.
insert(Ref, BigKey, RowID, Time, Term) ->
    big_data_nif:insert(Ref,
                        BigKey,
                        #row_data{row_id = RowID,
                                  time = Time,
                                  term = Term}).

-spec update_elem(Ref :: big_data(),
                  BigKey :: big_key(),
                  RowID :: row_id(),
                  ElemSpecs :: elem_specs()) ->
                     [boolean()] | ?BD_NOTFOUND.
update_elem(Ref, BigKey, RowID, ElemSpecs) ->
    case big_data_nif:update_elem(Ref, BigKey, RowID, ElemSpecs) of
        ?BD_NOTFOUND ->
            reload(Ref,
                   BigKey,
                   fun(_) -> big_data_nif:update_elem(Ref, BigKey, RowID, ElemSpecs) end);
        BoolL ->
            BoolL
    end.

-spec update_counter(Ref :: big_data(),
                     BigKey :: big_key(),
                     RowID :: row_id(),
                     ElemSpecs :: elem_specs()) ->
                        [boolean()] | ?BD_NOTFOUND.
update_counter(Ref, BigKey, RowID, ElemSpecs) ->
    case big_data_nif:update_counter(Ref, BigKey, RowID, ElemSpecs) of
        ?BD_NOTFOUND ->
            %% import from db
            reload(Ref,
                   BigKey,
                   fun(_) -> big_data_nif:update_counter(Ref, BigKey, RowID, ElemSpecs) end);
        BoolL ->
            BoolL
    end.

-spec get(Ref :: big_data(), BigKey :: big_key()) -> row_data_list() | ?BD_NOTFOUND.
get(Ref, BigKey) ->
    case big_data_nif:get(Ref, BigKey) of
        ?BD_NOTFOUND ->
            reload(Ref, BigKey, fun(RowDataList) -> RowDataList end);
        Result ->
            Result
    end.

-spec get_row(Ref :: big_data(), BigKey :: big_key(), RowID :: row_id()) ->
                 row_data() | ?BD_NOTFOUND | nil.
get_row(Ref, BigKey, RowID) ->
    case big_data_nif:get_row(Ref, BigKey, RowID) of
        ?BD_NOTFOUND ->
            reload(Ref, BigKey, fun(_RowDataList) -> big_data_nif:get_row(Ref, BigKey, RowID) end);
        nil ->
            nil;
        RowData ->
            RowData
    end.

-spec get_range(Ref :: big_data(),
                BigKey :: binary(),
                StartTime :: t(),
                EndTime :: t()) ->
                   row_data_list() | ?BD_NOTFOUND.
get_range(Ref, BigKey, StartTime, EndTime) ->
    case big_data_nif:get_range(Ref, BigKey, StartTime, EndTime) of
        ?BD_NOTFOUND ->
            reload(Ref,
                   BigKey,
                   fun(_) -> big_data_nif:get_range(Ref, BigKey, StartTime, EndTime) end);
        RowDataList ->
            RowDataList
    end.

-spec get_range_row_ids(Ref :: big_data(),
                        BigKey :: big_key(),
                        StartTime :: t(),
                        EndTime :: t()) ->
                           row_id_list() | ?BD_NOTFOUND.
get_range_row_ids(Ref, BigKey, StartTime, EndTime) ->
    case big_data_nif:get_range_row_ids(Ref, BigKey, StartTime, EndTime) of
        ?BD_NOTFOUND ->
            reload(Ref,
                   BigKey,
                   fun(_) -> big_data_nif:get_range_row_ids(Ref, BigKey, StartTime, EndTime) end);
        RowIDList ->
            RowIDList
    end.

-spec get_row_ids(Ref :: big_data(), BigKey :: big_key(), Time :: t()) ->
                     row_id_list() | ?BD_NOTFOUND.
get_row_ids(Ref, BigKey, Time) ->
    case big_data_nif:get_row_ids(Ref, BigKey, Time) of
        ?BD_NOTFOUND ->
            reload(Ref, BigKey, fun(_) -> big_data_nif:get_row_ids(Ref, BigKey, Time) end);
        RowIDList ->
            RowIDList
    end.

-spec get_time_index(Ref :: big_data(), BigKey :: big_key(), RowID :: row_id()) ->
                        t() | ?BD_NOTFOUND.
get_time_index(Ref, BigKey, RowID) ->
    case big_data_nif:get_time_index(Ref, BigKey, RowID) of
        ?BD_NOTFOUND ->
            reload(Ref, BigKey, fun(_) -> big_data_nif:get_time_index(Ref, BigKey, RowID) end);
        T ->
            T
    end.

-spec lookup_elem(Ref :: big_data(),
                  BigKey :: big_key(),
                  RowID :: row_id(),
                  ElemSpec :: elem_specs()) ->
                     tuple() | ?BD_NOTFOUND.
lookup_elem(Ref, BigKey, RowID, ElemSpecs) ->
    case big_data_nif:lookup_elem(Ref, BigKey, RowID, ElemSpecs) of
        ?BD_NOTFOUND ->
            reload(Ref,
                   BigKey,
                   fun(_) -> big_data_nif:lookup_elem(Ref, BigKey, RowID, ElemSpecs) end);
        Tuple ->
            Tuple
    end.

%% clear all buckets
-spec clear(Ref :: big_data()) -> ok.
clear(_Ref) ->
    ok.

-spec remove(Ref :: big_data(), BigKey :: binary()) -> ok.
remove(Ref, BigKey) ->
    ok = bd_backend_store:handle_del(BigKey),
    big_data_nif:remove(Ref, BigKey).

-spec remove_row(Ref :: big_data(), BigKey :: binary(), RowID :: binary()) -> ok.
remove_row(Ref, BigKey, RowID) ->
    case big_data_nif:remove_row(Ref, BigKey, RowID) of
        ?BD_NOTFOUND ->
            reload(Ref, BigKey, fun(_) -> big_data_nif:remove_row(Ref, BigKey, RowID) end);
        ok ->
            ok
    end.

-spec remove_row_ids(Ref :: big_data(),
                     BigKey :: big_key(),
                     StartTime :: t(),
                     EndTime :: t()) ->
                        ok.
remove_row_ids(Ref, BigKey, StartTime, EndTime) ->
    case big_data_nif:remove_row_ids(Ref, BigKey, StartTime, EndTime) of
        ?BD_NOTFOUND ->
            reload(Ref,
                   BigKey,
                   fun(_) -> big_data_nif:remove_row_ids(Ref, BigKey, StartTime, EndTime) end);
        ok ->
            ok
    end.

reload(Ref, BigKey) ->
    case bd_backend_store:handle_get(BigKey) of
        [] ->
            ?BD_NOTFOUND;
        RowDataList ->
            ok = big_data_nif:insert_new(Ref, BigKey, RowDataList)
    end.

reload(Ref, BigKey, Callback) ->
    case reload(Ref, BigKey) of
        ?BD_NOTFOUND ->
            ?BD_NOTFOUND;
        RowDataList ->
            Callback(RowDataList)
    end.