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
%%% Created : 2021-05-17T03:57:22+00:00
%%%-------------------------------------------------------------------
-module(bd_backend_store).

-include("big_data.hrl").

-author("yangcancai").

-callback handle_get(big_key()) -> row_data_list().
-callback handle_put(big_key(), row_data_list()) -> ok.
-callback handle_del(big_key()) -> ok.

-export([handle_get/1, handle_put/2, handle_del/1, set_backend_store/1]).

-spec set_backend_store(Module :: atom()) -> ok.
set_backend_store(Module) ->
    persistent_term:put(?MODULE, Module).

-spec handle_get(BigKey :: big_key()) -> row_data_list().
handle_get(BigKey) ->
    Mod = m(),
    Mod:handle_get(BigKey).

-spec handle_put(BigKey :: big_key(), RowDataList :: row_data_list()) -> ok.
handle_put(BigKey, RowDataList) ->
    Mod = m(),
    Mod:handle_put(BigKey, RowDataList).

-spec handle_del(BigKey :: big_key()) -> ok.
handle_del(BigKey) ->
    Mod = m(),
    Mod:handle_del(BigKey).

m() ->
    persistent_term:get(?MODULE).
