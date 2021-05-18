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

%% logging shim
-define(DEBUG(Fmt, Args), ?DISPATCH_LOG(debug, Fmt, Args)).
-define(INFO(Fmt, Args), ?DISPATCH_LOG(info, Fmt, Args)).
-define(NOTICE(Fmt, Args), ?DISPATCH_LOG(notice, Fmt, Args)).
-define(WARN(Fmt, Args), ?DISPATCH_LOG(warning, Fmt, Args)).
-define(WARNING(Fmt, Args), ?DISPATCH_LOG(warning, Fmt, Args)).
-define(ERR(Fmt, Args), ?DISPATCH_LOG(error, Fmt, Args)).
-define(ERROR(Fmt, Args), ?DISPATCH_LOG(error, Fmt, Args)).
-define(DISPATCH_LOG(Level, Fmt, Args),
        %% same as OTP logger does when using the macro
        (persistent_term:get('$bd_logger')):log(Level,
                                                Fmt,
                                                Args,
                                                #{mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY},
                                                  file => ?FILE,
                                                  line => ?LINE}),
        ok).
-define(H_big_data_nif, true).
-define(SYNC_INTERVAL, 5000).
-define(BD_LOG_META, bd_log_meta).
-define(BD_WAL_CHECKPOINT_SIZE, 1000).
-define(BD_WAL_CHECKPOINT_TIMEOUT, 60000).
-define(BD_WAL_MAX_SIZE_BYTES, 128 * 1024 * 1024).

-record(row_data,
        {row_id = <<"">> :: binary(), term = {} :: term(), time = 0 :: integer()}).

-type write() :: insert | update_elem | update_counter | remove_row.
-type read() :: get | get_row | get_range.
-type ation() :: write() | read().
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
-record(bd_log_wal_state,
        {fd :: maybe(file:io_device()),
         data_dir = <<"/tmp/big_data">> :: string(),
         file_name :: maybe(file:filename()),
         file_num = 1 :: pos_integer(),
         file_size = 0 :: pos_integer(),
         log_meta_ref :: reference(),
         wal_buffer_tid :: reference(),
         checkpoint_seq = 0 :: pos_integer(),
         last_checkpoint_time = 0 :: pos_integer(),
         log_seq = 0 :: pos_integer(),
         max_size_bytes = ?BD_WAL_MAX_SIZE_BYTES :: pos_integer(),
         write_strategy = default :: wal_write_strategy(),
         file_modes = [raw, write, read, binary] :: list()}).

-define(BD_NOTFOUND, notfound).

-endif.