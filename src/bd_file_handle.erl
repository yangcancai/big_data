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
%%% Created : 2021-05-14T06:10:45+00:00
%%%-------------------------------------------------------------------
-module(bd_file_handle).

-author("yangcancai").

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([open/2, close/1, sync/1, datasync/1, write/2, read/2, position/2]).
-export([pwrite/2, pwrite/3, pread/2, pread/3]).

-define(SERVER, ?MODULE).
-define(TABLE, bd_io_metrics).
-define(COUNT_TIME, [io_sync, io_seek, io_file_handle_open_attempt]).
-define(COUNT_TIME_BYTES, [io_read, io_write]).

-record(state, {monitors = #{} :: #{pid() => reference()}}).

open(File, Modes) ->
    gen_server:cast(?MODULE, {open, self()}),
    update(io_file_handle_open_attempt, fun() -> file:open(File, Modes) end).

close(Fd) ->
    gen_server:cast(?MODULE, {close, self()}),
    file:close(Fd).

sync(Fd) ->
    update(io_sync, fun() -> file:sync(Fd) end).

datasync(Fd) ->
    update(io_sync, fun() -> file:datasync(Fd) end).

write(Fd, Bytes) ->
    update(io_write, iolist_size(Bytes), fun() -> file:write(Fd, Bytes) end).

read(Fd, Bytes) ->
    update(io_read, Bytes, fun() -> file:read(Fd, Bytes) end).

position(Fd, Location) ->
    update(io_seek, fun() -> file:position(Fd, Location) end).

pwrite(Fd, LocBytes) ->
    update(io_write, fun() -> file:pwrite(Fd, LocBytes) end).

pwrite(Fd, Location, Bytes) ->
    update(io_write, iolist_size(Bytes), fun() -> file:pwrite(Fd, Location, Bytes) end).

pread(Fd, LocBytes) ->
    update(io_read, fun() -> file:pread(Fd, LocBytes) end).

pread(Fd, Location, Number) ->
    update(io_read, Number, fun() -> file:pread(Fd, Location, Number) end).

update(Op, Bytes, Thunk) ->
    {Time, Res} = timer_tc(Thunk),
    _ = ets:update_counter(?TABLE, {Op, count}, 1),
    _ = ets:update_counter(?TABLE, {Op, bytes}, Bytes),
    _ = ets:update_counter(?TABLE, {Op, time}, Time),
    Res.

update(Op, Thunk) ->
    {Time, Res} = timer_tc(Thunk),
    _ = ets:update_counter(?TABLE, {Op, count}, 1),
    _ = ets:update_counter(?TABLE, {Op, time}, Time),
    Res.

timer_tc(Thunk) ->
    T1 = erlang:monotonic_time(),
    Res = Thunk(),
    T2 = erlang:monotonic_time(),
    Diff = erlang:convert_time_unit(T2 - T1, native, micro_seconds),
    {Diff, Res}.

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    [ets:insert(?TABLE, {{Op, Counter}, 0})
     || Op <- ?COUNT_TIME_BYTES, Counter <- [count, bytes, time]],
    [ets:insert(?TABLE, {{Op, Counter}, 0}) || Op <- ?COUNT_TIME, Counter <- [count, time]],
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({open, Pid}, #state{monitors = Monitors} = State) ->
    ets:update_counter(bd_open_file_metrics, Pid, 1, {Pid, 0}),
    case Monitors of
        #{Pid := _MRef} ->
            {noreply, State};
        _ ->
            MRef = erlang:monitor(process, Pid),
            {noreply, State#state{monitors = Monitors#{Pid => MRef}}}
    end;
handle_cast({close, Pid}, State) ->
    ets:update_counter(bd_open_file_metrics, Pid, -1, {Pid, 0}),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, Pid, _}, #state{monitors = Monitors0} = State) ->
    case maps:take(Pid, Monitors0) of
        {MRef, Monitors} ->
            ets:delete(bd_open_file_metrics, Pid),
            {noreply, State#state{monitors = Monitors}};
        error ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
