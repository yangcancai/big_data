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
%%% Created : 2021-05-24T07:29:44+00:00
%%%-------------------------------------------------------------------
-module(bd_checkpoint).

-author("yangcancai").

-behaviour(gen_server).

-include("big_data.hrl").

%% API
-export([start_link/1, i/0, sync/0, sync/1, async/0, set_tid/1, seq/0,
         lookup_checkpoint_seq/0]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record(bd_checkpoint_state,
        {tid :: reference(),
         ref :: reference(),
         checkpoint_seq = 0 :: pos_integer(),
         last_checkpoint_time = 0 :: pos_integer()}).

%%%===================================================================
%%% API
%%%===================================================================
sync() ->
    gen_server:call(?MODULE, checkpoint).

sync(Timeout) ->
    ?DEBUG("Checkpoint sync timeout:~p", [Timeout]),
    gen_server:call(?MODULE, checkpoint, Timeout).

async() ->
    gen_server:cast(?MODULE, checkpoint).

i() ->
    sys:get_state(?MODULE).

seq() ->
    bd_counter:get(?BD_COUNTER_CHECKPOINT_SEQ).

lookup_checkpoint_seq() ->
    #bd_checkpoint_state{ref = Ref} = i(),
    lookup_checkpoint_seq(Ref).

set_tid(Tid) ->
    gen_server:call(?MODULE, {set_tid, Tid}).

%% @doc Spawns the server and registers the local name (unique)
-spec start_link(Config :: map()) ->
                    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Config], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec init(Args :: term()) ->
              {ok, State :: #bd_checkpoint_state{}} |
              {ok, State :: #bd_checkpoint_state{}, timeout() | hibernate} |
              {stop, Reason :: term()} |
              ignore.
init([#{dir := Dir}]) ->
    erlang:send_after(1000, self(), tick),
    bd_log_wal:make_dir(Dir),
    MetaFile = filename:join(Dir, "meta.dets"),
    {ok, Ref} = dets:open_file(?BD_LOG_META, [{file, MetaFile}, {auto_save, ?SYNC_INTERVAL}]),
    CheckpointSeq = lookup_checkpoint_seq(Ref),
    bd_counter:put(?BD_COUNTER_CHECKPOINT_SEQ, CheckpointSeq),
    {ok, #bd_checkpoint_state{ref = Ref, checkpoint_seq = CheckpointSeq}}.

%% @private
%% @doc Handling call messages
-spec handle_call(Request :: term(),
                  From :: {pid(), Tag :: term()},
                  State :: #bd_checkpoint_state{}) ->
                     {reply, Reply :: term(), NewState :: #bd_checkpoint_state{}} |
                     {reply,
                      Reply :: term(),
                      NewState :: #bd_checkpoint_state{},
                      timeout() | hibernate} |
                     {noreply, NewState :: #bd_checkpoint_state{}} |
                     {noreply, NewState :: #bd_checkpoint_state{}, timeout() | hibernate} |
                     {stop, Reason :: term(), Reply :: term(), NewState :: #bd_checkpoint_state{}} |
                     {stop, Reason :: term(), NewState :: #bd_checkpoint_state{}}.
handle_call(checkpoint, _From, #bd_checkpoint_state{} = State) ->
    {noreply, NewState} = handle_cast(checkpoint, State),
    {reply, NewState#bd_checkpoint_state.checkpoint_seq, NewState};
handle_call({set_tid, Tid}, _, State) ->
    {reply, ok, State#bd_checkpoint_state{tid = Tid}};
handle_call(_Request, _From, State = #bd_checkpoint_state{}) ->
    {reply, ok, State}.

%% @private
%% @doc Handling cast messages
-spec handle_cast(Request :: term(), State :: #bd_checkpoint_state{}) ->
                     {noreply, NewState :: #bd_checkpoint_state{}} |
                     {noreply, NewState :: #bd_checkpoint_state{}, timeout() | hibernate} |
                     {stop, Reason :: term(), NewState :: #bd_checkpoint_state{}}.
handle_cast(checkpoint,
            #bd_checkpoint_state{ref = Ref,
                                 tid = Tid,
                                 checkpoint_seq = CheckpointSeq} =
                State) ->
    IdSeq = bd_log_wal:id_seq(),
    case IdSeq =< CheckpointSeq of
        true ->
            ?DEBUG("Finish checkpoint id_seq = ~p, checkpointseq = ~p", [IdSeq, CheckpointSeq]),
            {noreply, State#bd_checkpoint_state{last_checkpoint_time = erlang:system_time(1000)}};
        false ->
            NewSeq = checkpoint(Tid, CheckpointSeq, IdSeq),
            bd_counter:add(?BD_COUNTER_CHECKPOINT_SEQ, NewSeq - CheckpointSeq),
            update_checkpoint_seq(Ref, NewSeq),
            NewS =
                State#bd_checkpoint_state{checkpoint_seq = NewSeq,
                                          last_checkpoint_time = erlang:system_time(1000)},
            handle_cast(checkpoint, NewS)
    end;
handle_cast(_Request, State = #bd_checkpoint_state{}) ->
    {noreply, State}.

%% @private
%% @doc Handling all non call/cast messages
-spec handle_info(Info :: timeout() | term(), State :: #bd_checkpoint_state{}) ->
                     {noreply, NewState :: #bd_checkpoint_state{}} |
                     {noreply, NewState :: #bd_checkpoint_state{}, timeout() | hibernate} |
                     {stop, Reason :: term(), NewState :: #bd_checkpoint_state{}}.
handle_info(tick,
            #bd_checkpoint_state{checkpoint_seq = CheckpointSeq, last_checkpoint_time = LastTime} =
                State) ->
    erlang:send_after(1000, self(), tick),
    Now = erlang:system_time(1000),
    LogSeq = bd_counter:get(?BD_COUNTER_ID_SEQ),
    case LogSeq - CheckpointSeq >= ?BD_WAL_CHECKPOINT_SIZE
         orelse Now - LastTime >= ?BD_WAL_CHECKPOINT_TIMEOUT
    of
        true ->
            ?DEBUG("Time to checkpoint id_seq = ~p, checkpoint_seq = ~p", [LogSeq, CheckpointSeq]),
            handle_cast(checkpoint, State);
        _ ->
            {noreply, State}
    end;
handle_info(_Info, State = #bd_checkpoint_state{}) ->
    {noreply, State}.

%% @private
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
-spec terminate(Reason :: normal | shutdown | {shutdown, term()} | term(),
                State :: #bd_checkpoint_state{}) ->
                   term().
terminate(_Reason, _State = #bd_checkpoint_state{ref = Ref}) ->
    ok = dets:sync(?BD_LOG_META),
    _ = dets:close(Ref),
    ok.

%% @private
%% @doc Convert process state when code is changed
-spec code_change(OldVsn :: term() | {down, term()},
                  State :: #bd_checkpoint_state{},
                  Extra :: term()) ->
                     {ok, NewState :: #bd_checkpoint_state{}} | {error, Reason :: term()}.
code_change(_OldVsn, State = #bd_checkpoint_state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
lookup_checkpoint_seq(Ref) ->
    case dets:lookup(Ref, checkpoint_seq) of
        [] ->
            0;
        [{_, Seq}] ->
            Seq
    end.

update_checkpoint_seq(Ref, Seq) ->
    ok = dets:insert(Ref, {checkpoint_seq, Seq}).

checkpoint(undefined, CheckpointSeq, _) ->
    CheckpointSeq;
checkpoint(Tid, CheckpointSeq, IdSeq) when is_integer(CheckpointSeq) ->
    {NewSeq, List} = get_chunk(Tid, CheckpointSeq, IdSeq),
    %% memory change
    bd_log_wal:process_all_action(?BD_BIG_DATA_CHECKPOINT_REF, List),
    checkpoint_chunk(?BD_BIG_DATA_CHECKPOINT_REF, List),
    %% delete already commit data
    bd_log_wal:delete_wal_buffer(Tid, CheckpointSeq, NewSeq),
    NewSeq.

checkpoint_chunk(Ref, List) ->
    %% merge
    BigKeyList =
        sets:to_list(
            sets:from_list([BigKey || #bd_wal{args = [BigKey | _]} <- List])),
    Chunk =
        lists:foldl(fun(BigKey, Acc) ->
                       case big_data_nif:get(Ref, BigKey) of
                           ?BD_NOTFOUND ->
                               Acc;
                           RowDataList ->
                               [{BigKey, RowDataList} | Acc]
                       end
                    end,
                    [],
                    BigKeyList),
    bd_backend_store:handle_put(
        lists:reverse(Chunk)).

get_chunk(Tid, CheckpointSeq, IdSeq) ->
    Max = max_chunk_id(CheckpointSeq, IdSeq),
    List =
        [begin
             [#bd_wal{} = R] = ets:lookup(Tid, Id),
             R
         end
         || Id <- lists:seq(CheckpointSeq + 1, Max)],
    {Max, List}.

max_chunk_id(CheckpointSeq, IdSeq) ->
    erlang:min(CheckpointSeq + 1000, IdSeq).
