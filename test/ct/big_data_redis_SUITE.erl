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
%%% Created : 2021-12-28T03:26:39+00:00
%%%-------------------------------------------------------------------
-module(big_data_redis_SUITE).

backend() ->
    big_data_redis.

-include("base.hrl").
pid(_) ->
    do_insert_check(?LINE, self()),
    ok.
func(_) ->
    do_insert_check(?LINE, fun() -> ok end),
    ok.
ref(_) ->
    do_insert_check(?LINE, erlang:make_ref()),
    ok.