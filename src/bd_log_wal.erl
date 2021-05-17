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
%%%  wal file format:
%%%  Wal Header: 5bytes
%%%  Wal Header
%%%  Magic: 4bytes
%%%  File format version: 1bytes
%%%
%%%  Frame =
%%%  <<Checksum:32/integer,
%%%  FrameDataLen:32/unsigned,
%%%  Idx:64/unsigned,
%%%  FrameData:FrameDataLen/binary>>
%%%  CheckSum = erlang:adler32(<<Idx:64/unsigned, FrameData/binary>>)
%%% @end
%%% Created : 2021-05-14T04:30:10+00:00
%%%-------------------------------------------------------------------
-module(bd_log_wal).

-author("yangcancai").

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([write/1, prepare_file/2, make_tmp/1]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(HEADER_SIZE, 5).
-define(MAGIC, "BDWA").
-define(VERSION, 1).
%% 32m， file:read(fd, ?WAL_RECOVERY_CHUNK_SIZE)
-define(WAL_RECOVERY_CHUNK_SIZE, 33554432).
-define(BD_WAL_BUFFER, bd_wal_buffer).

-include("big_data.hrl").

-record(bd_log_wal_state,
        {fd :: maybe(file:io_device()),
         filename :: maybe(file:filename()),
         file_num = 1 :: pos_integer(),
         file_size = 0 :: pos_integer(),
         log_meta_ref :: reference(),
         checkpoint_seq = 0 :: pos_integer(),
         last_checkpoint_time = 0 :: pos_integer(),
         log_seq = 0 :: pos_integer(),
         max_size_bytes = ?BD_WAL_MAX_SIZE_BYTES :: pos_integer(),
         write_strategy = default :: wal_write_strategy(),
         file_modes = [raw, write, read, binary] :: list()}).

%%%===================================================================
%%% API
%%%===================================================================
-spec write(Wal :: #bd_wal{}) -> ok.
write(Wal) ->
    gen_server:cast(?MODULE, Wal).

%% @doc Spawns the server and registers the local name (unique)
-spec start_link() -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec init(Args :: term()) ->
              {ok, State :: #bd_log_wal_state{}} |
              {ok, State :: #bd_log_wal_state{}, timeout() | hibernate} |
              {stop, Reason :: term()} |
              ignore.
init([]) ->
    %% recover from *.wal
    ?BD_WAL_BUFFER =
        ets:new(?BD_WAL_BUFFER,
                [named_table, public, set, {keypos, #bd_wal.id}, {write_concurrency, true}]),
    erlang:send_after(1000, self(), tick),
    {ok, recover_wal()}.

%% @private
%% @doc Handling call messages
-spec handle_call(Request :: term(),
                  From :: {pid(), Tag :: term()},
                  State :: #bd_log_wal_state{}) ->
                     {reply, Reply :: term(), NewState :: #bd_log_wal_state{}} |
                     {reply,
                      Reply :: term(),
                      NewState :: #bd_log_wal_state{},
                      timeout() | hibernate} |
                     {noreply, NewState :: #bd_log_wal_state{}} |
                     {noreply, NewState :: #bd_log_wal_state{}, timeout() | hibernate} |
                     {stop, Reason :: term(), Reply :: term(), NewState :: #bd_log_wal_state{}} |
                     {stop, Reason :: term(), NewState :: #bd_log_wal_state{}}.
handle_call(_Request, _From, State = #bd_log_wal_state{}) ->
    {reply, ok, State}.

%% @private
%% @doc Handling cast messages
-spec handle_cast(Request :: term(), State :: #bd_log_wal_state{}) ->
                     {noreply, NewState :: #bd_log_wal_state{}} |
                     {noreply, NewState :: #bd_log_wal_state{}, timeout() | hibernate} |
                     {stop, Reason :: term(), NewState :: #bd_log_wal_state{}}.
handle_cast(#bd_wal{} = Wal, State) ->
    %% First: write ahead log
    write_wal(State, Wal),
    %% Second: update wal_buffer
    insert_wal_buffer(Wal),
    {noreply, State};
handle_cast(_Request, State = #bd_log_wal_state{}) ->
    {noreply, State}.

%% @private
%% @doc Handling all non call/cast messages
-spec handle_info(Info :: timeout() | term(), State :: #bd_log_wal_state{}) ->
                     {noreply, NewState :: #bd_log_wal_state{}} |
                     {noreply, NewState :: #bd_log_wal_state{}, timeout() | hibernate} |
                     {stop, Reason :: term(), NewState :: #bd_log_wal_state{}}.
handle_info(tick,
            State =
                #bd_log_wal_state{log_meta_ref = Ref,
                                  log_seq = LogSeq,
                                  last_checkpoint_time = LastTime,
                                  checkpoint_seq = CheckpointSeq}) ->
    erlang:send_after(1000, self(), tick),
    Now = erlang:system_time(1000),
    case LogSeq - CheckpointSeq >= ?BD_WAL_CHECKPOINT_SIZE orelse
             Now - LastTime >= ?BD_WAL_CHECKPOINT_TIMEOUT
    of
        true ->
            NewSeq = checkpoint(Ref, CheckpointSeq),
            %% delete already commit data
            delete_wal_buffer(NewSeq),
            {noreply, State#bd_log_wal_state{checkpoint_seq = NewSeq, last_checkpoint_time = Now}};
        _ ->
            {noreply, State}
    end;
handle_info(_Info, State = #bd_log_wal_state{}) ->
    {noreply, State}.

%% @private
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
-spec terminate(Reason :: normal | shutdown | {shutdown, term()} | term(),
                State :: #bd_log_wal_state{}) ->
                   term().
terminate(_Reason, _State = #bd_log_wal_state{log_meta_ref = Ref}) ->
    ok = dets:sync(?BD_LOG_META),
    _ = dets:close(Ref),
    ok.

%% @private
%% @doc Convert process state when code is changed
-spec code_change(OldVsn :: term() | {down, term()},
                  State :: #bd_log_wal_state{},
                  Extra :: term()) ->
                     {ok, NewState :: #bd_log_wal_state{}} | {error, Reason :: term()}.
code_change(_OldVsn, State = #bd_log_wal_state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
recover_wal() ->
    Dir = "data/wal",
    ok = make_dir(Dir),
    MetaFile = "data/meta.dets",
    {ok, Ref} = dets:open_file(?BD_LOG_META, [{file, MetaFile}, {auto_save, ?SYNC_INTERVAL}]),
    CheckpointSeq = lookup_checkpoint_seq(Ref),
    WalFiles =
        lists:sort(
            filelib:wildcard(
                filename:join(Dir, "*.wal"))),
    %% read chunk to wal_buffer
    [begin
         Fd = open_and_read_header(File),
         recover_wal_chunk(CheckpointSeq, Fd, ?WAL_RECOVERY_CHUNK_SIZE)
     end
     || File <- WalFiles],
    %% proccess all action
    process_all_action(),
    %% checkpoint
    NewCheckpointSeq = checkpoint(Ref, CheckpointSeq),
    #bd_log_wal_state{log_meta_ref = Ref, checkpoint_seq = NewCheckpointSeq}.

prepare_file(File, Modes) ->
    Tmp = make_tmp(File),
    %% rename is atomic-ish so we will never accidentally write an empty wal file
    %% using prim_file here as file:rename/2 uses the file server
    ok = prim_file:rename(Tmp, File),
    case bd_file_handle:open(File, Modes) of
        {ok, Fd2} ->
            {ok, ?HEADER_SIZE} = file:position(Fd2, ?HEADER_SIZE),
            {ok, Fd2};
        {error, _} = Err ->
            Err
    end.

make_tmp(File) ->
    Tmp = filename:rootname(File) ++ ".tmp",
    {ok, Fd} = file:open(Tmp, [write, binary, raw]),
    ok = file:write(Fd, <<?MAGIC, ?VERSION:8/unsigned>>),
    ok = file:sync(Fd),
    ok = file:close(Fd),
    Tmp.

-spec make_dir(file:name_all()) -> ok | {error, file:posix() | badarg}.
make_dir(Dir) ->
    handle_ensure_dir(filelib:ensure_dir(Dir), Dir).

handle_ensure_dir(ok, Dir) ->
    handle_make_dir(file:make_dir(Dir));
handle_ensure_dir(Error, _Dir) ->
    Error.

handle_make_dir(ok) ->
    ok;
handle_make_dir({error, eexist}) ->
    ok;
handle_make_dir(Error) ->
    Error.

open_and_read_header(File) ->
    {ok, Fd} = file:open(File, [read, binary, raw]),
    case file:read(Fd, ?HEADER_SIZE) of
        {ok, <<?MAGIC, ?VERSION:8/unsigned>>} ->
            %% the only version currently supported
            Fd;
        {ok, <<Magic:4/binary, UnknownVersion:8/unsigned>>} ->
            exit({unknown_wal_file_format, Magic, UnknownVersion})
    end.

%% recover wal chunck to ets table
recover_wal_chunk(CheckpointSeq, Fd, ChunkSz) ->
    Chunk = read_from_wal_file(Fd, ChunkSz),
    recover_frames(CheckpointSeq, Fd, Chunk, ChunkSz).

recover_frames(_CheckpointSeq,
               _Fd,
               <<0:32/unsigned,
                 FrameDataLen:32/unsigned,
                 0:64/integer,
                 _:FrameDataLen/unsigned,
                 _/binary>>,
               _ChunkSize) ->
    ok;
recover_frames(CheckpointSeq,
               Fd,
               <<Checksum:32/integer,
                 FrameDataLen:32/unsigned,
                 Idx:64/unsigned,
                 FrameData:FrameDataLen/binary,
                 Rest/binary>>,
               ChunkSize) ->
    case Idx > CheckpointSeq of
        true ->
            true = validate_and_update(Checksum, Idx, FrameData);
        _ ->
            ignore
    end,
    recover_frames(CheckpointSeq, Fd, Rest, ChunkSize);
recover_frames(CheckpointSeq, Fd, Chunk, ChunkSize) ->
    NextChunk = read_from_wal_file(Fd, ChunkSize),
    case NextChunk of
        <<>> ->
            ok;
        _ ->
            Chunk0 = <<Chunk/binary, NextChunk/binary>>,
            recover_frames(CheckpointSeq, Fd, Chunk0, ChunkSize)
    end.

validate_and_update(Checksum, Idx, FrameData) ->
    ok = validate_checksum(Checksum, Idx, FrameData),
    %% write to memtable
    true = insert_wal_buffer(erlang:binary_to_term(FrameData)),
    ok.

validate_checksum(Checksum, Idx, FrameData) ->
    % building a binary just for the checksum may feel a bit wasteful
    % but this is only called during recovery which should be a rare event
    case erlang:adler32(<<Idx:64/unsigned, FrameData/binary>>) of
        Checksum ->
            ok;
        _ ->
            exit(wal_checksum_validation_failure)
    end.

read_from_wal_file(Fd, Sz) ->
    case bd_file_handle:read(Fd, Sz) of
        {ok, <<Data/binary>>} ->
            Data;
        eof ->
            <<>>;
        {error, Reason} ->
            exit({could_not_read_wal_chunk, Reason})
    end.

process_all_action() ->
    List = ets:tab2list(?BD_WAL_BUFFER),
    process_all_action(?BD_BIG_DATA_REF, List).

process_all_action(_, []) ->
    ok;
process_all_action(Ref, [#bd_wal{action = Action, args = Args} | Rest]) ->
    apply(big_data, Action, [Ref] ++ Args),
    process_all_action(Ref, Rest).

checkpoint(Ref, CheckpointSeq) when is_integer(CheckpointSeq) ->
    {NewSeq, List} =
        ets:foldl(fun (#bd_wal{id = Id} = Row, {Seq, Acc}) when Id > CheckpointSeq ->
                          {erlang:max(Id, Seq), [Row | Acc]};
                      (_, Acc) ->
                          Acc
                  end,
                  {CheckpointSeq, []},
                  ?BD_WAL_BUFFER),

    ok = checkpoint(Ref, lists:reverse(List)),
    NewSeq;
checkpoint(_, []) ->
    ok;
checkpoint(Ref,
           [#bd_wal{id = ID,
                    action = Action,
                    args = [BigKey | _]}
            | Rest])
    when Action == insert;
         Action == remove_row;
         Action == update_counter;
         Action == update_elem ->
    case big_data_nif:get(?BD_BIG_DATA_REF, BigKey) of
        ?BD_NOTFOUND ->
            ok;
        RowDataList ->
            ok = bd_backend_store:handle_put(BigKey, RowDataList),
            update_checkpoint_seq(Ref, ID)
    end,
    checkpoint(Ref, Rest).

lookup_checkpoint_seq(Ref) ->
    case dets:lookup(Ref, checkpoint_seq) of
        [] ->
            0;
        [Seq] ->
            Seq
    end.

update_checkpoint_seq(Ref, Seq) ->
    ok = dets:insert(Ref, {checkpoint_seq, Seq}).

delete_wal_buffer(CheckpointSeq) ->
    true =
        ets:match_delete(?BD_WAL_BUFFER,
                         ets:fun2ms(fun(#bd_wal{id = Id}) when Id < CheckpointSeq -> true end)).

insert_wal_buffer(#bd_wal{} = Log) ->
    true = ets:insert(?BD_WAL_BUFFER, Log).

write_wal(#bd_log_wal_state{fd = Fd}, #bd_wal{id = Idx} = Wal) ->
    FrameData = erlang:term_to_binary(Wal),
    FrameDataLen = erlang:size(FrameData),
    Checksum = erlang:adler32(<<Idx:64/unsigned, FrameData/binary>>),
    WalFrame =
        <<Checksum:32/integer,
          FrameDataLen:32/unsigned,
          Idx:64/unsigned,
          FrameData:FrameDataLen/binary>>,
    bd_file_handle:write(Fd, WalFrame).
