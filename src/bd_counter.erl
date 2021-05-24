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
%%% Created : 2021-05-24T08:33:53+00:00
%%%-------------------------------------------------------------------
-module(bd_counter).

-author("yangcancai").

-include("big_data.hrl").

-export([new/0, ref/0, overview/0, get/1, put/2, add/2, update_counter/1]).

-define(SIZE, erlang:length(?BD_COUNTER_LIST)).

new() ->
    Rs = counters:new(?SIZE, []),
    persistent_term:put(?MODULE, Rs),
    Rs.

ref() ->
    persistent_term:get(?MODULE).

add(I, Incr) ->
    counters:add(ref(), I, Incr).

update_counter(I) ->
    add(I, 1).

put(I, Value) ->
    counters:put(ref(), I, Value).

get(I) ->
    counters:get(ref(), I).

overview() ->
    lists:zip(?BD_COUNTER_LIST, [?MODULE:get(I) || I <- lists:seq(1, ?SIZE)]).
