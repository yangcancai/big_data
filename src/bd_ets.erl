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
%%% Created : 2021-05-18T06:16:10+00:00
%%%-------------------------------------------------------------------
-module(bd_ets).

-author("yangcancai").

-define(TABLES, [bd_metrics, bd_open_file_metrics, bd_io_metrics]).

-export([new/0]).

new() ->
    _ = [ets:new(Table, [named_table, public, {write_concurrency, true}])
         || Table <- ?TABLES],
    ok.
