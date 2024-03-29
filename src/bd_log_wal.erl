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

-include_lib("stdlib/include/ms_transform.hrl").

%% API
-export([start_link/1, i/0, stop/0, make_dir/1, wal_buffer/0, id_seq/0]).
-export([write/1, write_sync/1, delete_wal_buffer/3, process_all_action/2]).
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

%%%===================================================================
%%% API
%%%===================================================================
id_seq() ->
    bd_counter:get(?BD_COUNTER_ID_SEQ).

-spec wal_buffer() -> list().
wal_buffer() ->
    S = i(),
    ets:tab2list(S#bd_log_wal_state.wal_buffer_tid).

-spec i() -> #bd_log_wal_state{}.
i() ->
    sys:get_state(?MODULE).

-spec write(Wal :: #bd_wal{}) -> ok.
write(Wal) ->
    gen_server:cast(?MODULE, Wal).

write_sync(Wal) ->
    gen_server:call(?MODULE, Wal).

%% @doc Spawns the server and registers the local name (unique)
-spec start_link(Config :: map()) ->
                    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Config], []).

stop() ->
    gen_server:call(?MODULE,
                    stop).%%%===================================================================
                          %%% gen_server callbacks
                          %%%===================================================================

%% @private
%% @doc Initializes the server
-spec init(Args :: term()) ->
              {ok, State :: #bd_log_wal_state{}} |
              {ok, State :: #bd_log_wal_state{}, timeout() | hibernate} |
              {stop, Reason :: term()} |
              ignore.
init([Config]) ->
    %% recover from *.wal
    erlang:send_after(1000, self(), tick),
    {ok, recover_wal(Config)}.

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
handle_call(#bd_wal{} = Wal, _, State) ->
    {noreply, NewState} = handle_cast(Wal, State),
    {reply, ok, NewState};
handle_call(stop, From, State) ->
    {stop, normal, State#bd_log_wal_state{stop_from = From}};
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
    NewState = write_wal(State, Wal),
    bd_counter:add(?BD_COUNTER_ID_SEQ, 1),
    %% Second: update wal_buffer
    insert_wal_buffer(NewState#bd_log_wal_state.wal_buffer_tid,
                      Wal#bd_wal{id = NewState#bd_log_wal_state.id_seq}),
    {noreply, NewState};
handle_cast(_Request, State = #bd_log_wal_state{}) ->
    {noreply, State}.

%% @private
%% @doc Handling all non call/cast messages
-spec handle_info(Info :: timeout() | term(), State :: #bd_log_wal_state{}) ->
                     {noreply, NewState :: #bd_log_wal_state{}} |
                     {noreply, NewState :: #bd_log_wal_state{}, timeout() | hibernate} |
                     {stop, Reason :: term(), NewState :: #bd_log_wal_state{}}.
handle_info(tick, State = #bd_log_wal_state{}) ->
    {noreply, State};
handle_info({waiting_pid, Pid}, State = #bd_log_wal_state{}) ->
    ok = persistent_term:put('$bd_log_wal_started', true),
    notify_recover_finished(Pid),
    {noreply, State};
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
terminate(Reason, _State = #bd_log_wal_state{stop_from = From, fd = Fd}) ->
    ok = close_existing(Fd),
    ?DEBUG("Wal process terminate, Reason: ~p..", [Reason]),
    case From of
        undefined ->
            ignore;
        _ ->
            gen:reply(From, ok)
    end,
    ok = persistent_term:put('$bd_log_wal_started', false),
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
recover_wal(#{dir := Dir} = Config) ->
    Pid = maps:get(waiting_pid, Config, undefined),
    ?DEBUG("Recover: ..~p", [Config]),
    Tid = ets:new(?BD_WAL_BUFFER,
                  [public, set, {keypos, #bd_wal.id}, {write_concurrency, true}]),
    bd_checkpoint:set_tid(Tid),
    ok = make_dir(Dir),
    CheckpointSeq = bd_checkpoint:lookup_checkpoint_seq(),
    WalFiles =
        maybe_not_checkpoint_files(sort_file(filelib:wildcard(
                                                 filename:join(Dir, "*.wal"))),
                                   CheckpointSeq),
    %% read chunk to wal_buffer
    bd_counter:put(?BD_COUNTER_CHECKPOINT_SEQ, CheckpointSeq),
    NewIdSeq =
        lists:foldl(fun(File, _Acc) ->
                       Fd = open_and_read_header(File),
                       IdSeq = recover_wal_chunk(Tid, CheckpointSeq, Fd, ?WAL_RECOVERY_CHUNK_SIZE),
                       close_existing(Fd),
                       IdSeq
                    end,
                    CheckpointSeq,
                    WalFiles),
    ?DEBUG("CheckpointSeq = ~p, id_seq = ~p, walfiles = ~p",
           [CheckpointSeq, NewIdSeq, WalFiles]),
    bd_counter:put(?BD_COUNTER_ID_SEQ, NewIdSeq),
    CurFileNum = extract_file_num(lists:reverse(WalFiles)),
    %% proccess all action
    process_all_action(Tid),
    bd_checkpoint:sync(600000),
    State =
        #bd_log_wal_state{file_num = CurFileNum,
                          data_dir = Dir,
                          id_seq = NewIdSeq,
                          wal_buffer_tid = Tid},
    NewS = roll_over(State),
    self() ! {waiting_pid, Pid},
    NewS.

notify_recover_finished(undefined) ->
    ok;
notify_recover_finished(Pid) when is_pid(Pid) ->
    case erlang:is_process_alive(Pid) of
        true ->
            ?DEBUG("Notify recover finished, Pid = ~p", [Pid]),
            Pid ! {self(), ?BD_RECOVER_FINISHED};
        _ ->
            ok
    end.

prepare_file(File, Modes) ->
    Tmp = make_tmp(File),
    %% rename is atomic-ish so we will never accidentally write an empty wal file
    %% using prim_file here as file:rename/2 uses the file server
    ok = prim_file:rename(Tmp, File),
    case bd_file_handle:open(File, Modes) of
        {ok, Fd2} ->
            {ok, ?HEADER_SIZE} = bd_file_handle:position(Fd2, ?HEADER_SIZE),
            {ok, Fd2};
        {error, _} = Err ->
            Err
    end.

make_tmp(File) ->
    Tmp = filename:rootname(File) ++ ".tmp",
    {ok, Fd} = bd_file_handle:open(Tmp, [write, binary, raw]),
    ok = bd_file_handle:write(Fd, <<?MAGIC, ?VERSION:8/unsigned>>),
    ok = bd_file_handle:sync(Fd),
    ok = bd_file_handle:close(Fd),
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
    {ok, Fd} = bd_file_handle:open(File, [read, binary, raw]),
    case bd_file_handle:read(Fd, ?HEADER_SIZE) of
        {ok, <<?MAGIC, ?VERSION:8/unsigned>>} ->
            %% the only version currently supported
            Fd;
        {ok, <<Magic:4/binary, UnknownVersion:8/unsigned>>} ->
            exit({unknown_wal_file_format, Magic, UnknownVersion})
    end.

%% recover wal chunck to ets table
recover_wal_chunk(Tid, CheckpointSeq, Fd, ChunkSz) ->
    Chunk = read_from_wal_file(Fd, ChunkSz),
    recover_frames(Tid, CheckpointSeq, Fd, Chunk, ChunkSz, CheckpointSeq).

recover_frames(_,
               _CheckpointSeq,
               _Fd,
               <<0:32/unsigned,
                 FrameDataLen:32/unsigned,
                 0:64/integer,
                 _:FrameDataLen/unsigned,
                 _/binary>>,
               _ChunkSize,
               IdSeq) ->
    IdSeq;
recover_frames(Tid,
               CheckpointSeq,
               Fd,
               <<Checksum:32/integer,
                 FrameDataLen:32/unsigned,
                 Idx:64/unsigned,
                 FrameData:FrameDataLen/binary,
                 Rest/binary>>,
               ChunkSize,
               _IdSeq) ->
    case Idx > CheckpointSeq of
        true ->
            ok = validate_and_update(Tid, Checksum, Idx, FrameData);
        _ ->
            ignore
    end,
    recover_frames(Tid, CheckpointSeq, Fd, Rest, ChunkSize, Idx);
recover_frames(Tid, CheckpointSeq, Fd, Chunk, ChunkSize, IdSeq) ->
    NextChunk = read_from_wal_file(Fd, ChunkSize),
    case NextChunk of
        <<>> ->
            IdSeq;
        _ ->
            Chunk0 = <<Chunk/binary, NextChunk/binary>>,
            recover_frames(Tid, CheckpointSeq, Fd, Chunk0, ChunkSize, IdSeq)
    end.

validate_and_update(Tid, Checksum, Idx, FrameData) ->
    ok = validate_checksum(Checksum, Idx, FrameData),
    %% write to memtable
    true = insert_wal_buffer(Tid, erlang:binary_to_term(FrameData)),
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

process_all_action(_, []) ->
    ok;
process_all_action(Ref, [#bd_wal{ args = Args, action = Action} = Wal | Rest]) ->
    case big_data:backend() of
      big_data_redis ->
        apply(big_data_redis, command, [Wal]);
      _->
        apply(big_data_local, Action, [ Ref | Args])
    end,
    process_all_action(Ref, Rest).

process_all_action(Tid) ->
  List = ets:tab2list(Tid),
      process_all_action(?BD_BIG_DATA_REF, List).

delete_wal_buffer(Tid, CheckpointSeq, NewSeq) ->
    [ets:delete(Tid, Id) || Id <- lists:seq(CheckpointSeq, NewSeq)].

insert_wal_buffer(Tid, #bd_wal{id = _ID} = Log) ->
    true = ets:insert(Tid, Log).

write_wal(#bd_log_wal_state{fd = Fd,
                            id_seq = IdSeq,
                            file_size = FileSize,
                            write_strategy = WriteStrategy,
                            max_size_bytes = MaxSizeBytes} =
              State,
          #bd_wal{} = Wal) ->
    Idx = IdSeq + 1,
    FrameData = erlang:term_to_binary(Wal#bd_wal{id = Idx}),
    FrameDataLen = erlang:size(FrameData),
    Checksum = erlang:adler32(<<Idx:64/unsigned, FrameData/binary>>),
    WalFrame =
        <<Checksum:32/integer,
          FrameDataLen:32/unsigned,
          Idx:64/unsigned,
          FrameData:FrameDataLen/binary>>,
    FrameLen = FrameDataLen + 8 + 4 + 4,
    case FileSize + FrameLen > MaxSizeBytes of
        true ->
            NewState = roll_over(State),
            flush_wal(NewState#bd_log_wal_state.fd,
                      NewState#bd_log_wal_state.write_strategy,
                      WalFrame),
            NewState#bd_log_wal_state{file_size = FrameLen, id_seq = Idx};
        false ->
            flush_wal(Fd, WriteStrategy, WalFrame),
            State#bd_log_wal_state{file_size = FileSize + FrameLen, id_seq = Idx}
    end.

flush_wal(Fd, WriteStrategy, Bytes) ->
    case WriteStrategy of
        default ->
            ok = bd_file_handle:write(Fd, Bytes);
        %% flush os buffer to disk
        %% only data, difference from sync
        %% sync will be sync data and meta info such as size, time ..
        % ok = bd_file_handle:datasync(Fd);
        o_sync ->
            ok = bd_file_handle:write(Fd, Bytes)
    end.

close_existing(undefined) ->
    ok;
close_existing(Fd) ->
    case bd_file_handle:close(Fd) of
        ok ->
            ok;
        {error, Reason} ->
            exit({could_not_close, Reason})
    end.

roll_over(#bd_log_wal_state{fd = Fd,
                            data_dir = DataDir,
                            id_seq = IdSeq,
                            file_num = FileNum} =
              State) ->
    close_existing(Fd),
    Num = FileNum + 1,
    FileName =
        filename:join(DataDir, zpad_filename(erlang:integer_to_list(IdSeq + 1), "wal", Num)),
    S = State#bd_log_wal_state{file_num = Num, file_name = FileName},
    open_wal(S).

open_wal(#bd_log_wal_state{file_name = File,
                           write_strategy = WriteStrategy,
                           file_modes = FileModes} =
             State) ->
    {ok, Fd} = prepare_file(File, reset_file_modes(WriteStrategy, FileModes)),
    State#bd_log_wal_state{fd = Fd, file_size = 0}.

reset_file_modes(default, FileModes) ->
    FileModes;
reset_file_modes(o_sync, FileModes) ->
    [sync] ++ FileModes.

extract_file_num([]) ->
    0;
extract_file_num([F | _]) ->
    zpad_extract_num(filename:basename(F)).

zpad_extract_num(Fn) ->
    {match, [_, NumStr, _]} =
        re:run(Fn, "(.*)([0-9]{8})(.*)", [{capture, all_but_first, list}]),
    list_to_integer(NumStr).

zpad_filename("", Ext, Num) ->
    lists:flatten(
        io_lib:format("~8..0B.~s", [Num, Ext]));
zpad_filename(Prefix, Ext, Num) ->
    lists:flatten(
        io_lib:format("~s_~8..0B.~s", [Prefix, Num, Ext])).

sort_file(Files) ->
    lists:sort(fun sort_file/2, Files).

sort_file(A, B) ->
    case string:split(A, "_") of
        [_, A1] ->
            [_, B1] = string:split(B, "_"),
            A1 < B1;
        [A1] ->
            [B1] = string:split(B, "_"),
            A1 < B1
    end.

maybe_not_checkpoint_files([], _) ->
    [];
maybe_not_checkpoint_files(Files, CheckpointSeq) ->
    N = maybe_not_checkpoint_files(Files, CheckpointSeq, 1),
    lists:sublist(Files, N, erlang:length(Files)).

maybe_not_checkpoint_files([], _, N) ->
    N - 1;
maybe_not_checkpoint_files([File | Rest], CheckpointSeq, N) ->
    {Eq, B} =
        case string:split(
                 filename:basename(File), "_")
        of
            [_] ->
                {false, true};
            [First, _] ->
                {erlang:list_to_integer(First) == CheckpointSeq,
                 erlang:list_to_integer(First) >= CheckpointSeq}
        end,
    case B of
        true ->
            case N > 1 andalso Eq == false of
                true ->
                    N - 1;
                _ ->
                    N
            end;
        false ->
            maybe_not_checkpoint_files(Rest, CheckpointSeq, N + 1)
    end.
