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
-export([start_link/1, i/0, sync/0, sync/1, async/0, set_tid/1, checkpoint_seq/0,
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
    gen_server:call(?MODULE, checkpoint, Timeout).

async() ->
    gen_server:cast(?MODULE, checkpoint).

i() ->
    sys:get_state(?MODULE).

checkpoint_seq() ->
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
    NewSeq = checkpoint(Tid, CheckpointSeq),
    bd_counter:add(?BD_COUNTER_CHECKPOINT_SEQ, NewSeq - CheckpointSeq),
    update_checkpoint_seq(Ref, NewSeq),
    {noreply,
     State#bd_checkpoint_state{checkpoint_seq = NewSeq,
                               last_checkpoint_time = erlang:system_time(1000)}};
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
    case LogSeq - CheckpointSeq >= ?BD_WAL_CHECKPOINT_SIZE orelse
             Now - LastTime >= ?BD_WAL_CHECKPOINT_TIMEOUT
    of
        true ->
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
    ?DEBUG("Update checkpoint seq: ~p", [Seq]),
    ok = dets:insert(Ref, {checkpoint_seq, Seq}).

checkpoint(undefined, CheckpointSeq) ->
    CheckpointSeq;
checkpoint(Tid, CheckpointSeq) when is_integer(CheckpointSeq) ->
    {NewSeq, List} =
        ets:foldl(fun (#bd_wal{id = Id} = Row, {Seq, Acc}) when Id > CheckpointSeq ->
                          {erlang:max(Id, Seq), [Row | Acc]};
                      (_, Acc) ->
                          Acc
                  end,
                  {CheckpointSeq, []},
                  Tid),

    ok = checkpoint(lists:sort(List)),
    %% delete already commit data
    bd_log_wal:delete_wal_buffer(Tid, NewSeq),
    NewSeq.

checkpoint([]) ->
    ok;
checkpoint([#bd_wal{id = _ID,
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
            ok = bd_backend_store:handle_put(BigKey, RowDataList)
    end,
    %% warning: directly update disk, so slowly
    checkpoint(Rest).
