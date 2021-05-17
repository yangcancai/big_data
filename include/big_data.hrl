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
%%% Created : 2021-05-07T04:06:26+00:00
%%%-------------------------------------------------------------------

-author("yangcancai").

-ifndef(H_big_data_nif).

-define(H_big_data_nif, true).

-record(row_data,
        {row_id = <<"">> :: binary(), term = {} :: term(), time = 0 :: integer()}).

-type ation() :: insert | update_elem | remove.
-type maybe(T) :: undefined | T.
-type wal_write_strategy() :: default | o_sync.

                         % writes all pending in one write(2) call then calls fsync(1)

-type big_data() :: reference().
-type big_key() :: binary().
-type row_id() :: binary().
-type row_id_list() :: [row_id()].
-type row_data() :: #row_data{}.
-type row_data_list() :: [row_data()].
-type t() :: integer().
-type pos() :: integer().
%%  pos from 0
-type elem_spec() :: {pos(), term()} | pos().
-type elem_specs() :: [elem_spec()].

% like delay writes but tries to open the file using synchronous io
% (O_SYNC) rather than a write(2) followed by an fsync.
-define(BD_BIG_DATA_REF, persistent_term:get(big_data)).

-record(bd_wal,
        {id = 0 :: pos_integer(),
         action = write :: ation(),
         args = [] :: list(),
         time = erlang:system_time(1000) :: pos_integer()}).

-define(BD_NOTFOUND, notfound).

-endif.
