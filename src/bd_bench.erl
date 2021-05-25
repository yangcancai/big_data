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
%%% Created : 2021-05-19T07:19:22+00:00
%%%-------------------------------------------------------------------
-module(bd_bench).

-author("yangcancai").

-include("big_data.hrl").

-export([run/0, run/2, overview/0]).

-define(TotalCmdIx, 1).
-define(TotalTime, 2).
-define(FIELDS, [total_command, sum_time]).
-define(SIZE, erlang:length(?FIELDS)).

run() ->
    run(100, 100).

run(N, Degree) when is_integer(N) ->
    run(#{bucket_counter => N,
          degree => Degree,
          min_row => 1,
          max_row => 300,
          mix_row_bytes => 100,
          max_row_bytes => 500}).

run(#{bucket_counter := BucketCounter, degree := Degree} = Config) ->
    spawn_link(fun() ->
                  Each = BucketCounter div Degree,
                  Pid = self(),
                  Size = ?SIZE,
                  Ref = counters:new(Size, []),
                  persistent_term:put(bd_bench_s, erlang:system_time(1000)),
                  persistent_term:put(counters, Ref),
                  Pids =
                      [spawn_link(fun() -> loop(Pid, Each, Config) end)
                       || _ <- lists:seq(1, Degree)],
                  wait_loop(Pids),
                  Rs = overview(),
                  io:format("Overview = ~p~n", [Rs])
               end).

overview() ->
    Ref = persistent_term:get(counters),
    L = [counters:get(Ref, I) || I <- lists:seq(1, ?SIZE)],
    R = #{total_command := Cmd, sum_time := Time} =
            maps:from_list(
                lists:zip(?FIELDS, L)),
    Tps = Time div Cmd,
    TotalTime = persistent_term:get(bd_bench_e) - persistent_term:get(bd_bench_s),
    S = TotalTime div 1000,
    Sec = case S of
              0 ->
                  1;
              S ->
                  S
          end,
    R#{aver => Tps div 1000,
       tps => Cmd div Sec,
       total_time => TotalTime}.

wait_loop([]) ->
    persistent_term:put(bd_bench_e, erlang:system_time(1000)),
    ok;
wait_loop(Pids) ->
    receive
        {Pid, done} ->
            ?DEBUG("Bech done pid = ~p", [Pid]),
            wait_loop(lists:delete(Pid, Pids));
        {'EXIT', Pid, _} ->
            ?DEBUG("Bech exit pid = ~p", [Pid]),
            wait_loop(lists:delete(Pid, Pids))
    end.

loop(Parent,
     Each,
     #{min_row := MixRow,
       max_row := MaxRow,
       mix_row_bytes := MixRowBytes,
       max_row_bytes := MaxRowBytes}) ->
    Row = gen_row(MixRow, MaxRow),
    Ref = persistent_term:get(counters),
    %% insert
    [begin
         Bucket = gen_bucket(),
         [begin
              RowBytes = gen_data(MixRowBytes, MaxRowBytes),
              counters:add(Ref, ?TotalCmdIx, 1),
              {T, _Rs} =
                  timer:tc(fun() ->
                              big_data:command(#bd_wal{action = insert,
                                                       args =
                                                           [Bucket,
                                                            erlang:integer_to_binary(RowID),
                                                            erlang:system_time(1000),
                                                            RowBytes]})
                           end),
              counters:add(Ref, ?TotalTime, T)
          end
          || RowID <- lists:seq(1, Row)]
     end
     || _ <- lists:seq(1, Each)],
    Parent ! {self(), done}.

gen_row(MixRow, MaxRow) ->
    rand(MixRow, MaxRow).

gen_bucket() ->
    rand_bytes(32).

gen_data(MixRowBytes, MaxRowBytes) ->
    RowBytes = rand(MixRowBytes, MaxRowBytes),
    rand_bytes(RowBytes).

rand(Mix, Max) ->
    rand:uniform(Max - Mix) + Mix.

rand_bytes(N) ->
    ?BD_RAND_BYTES(N).
