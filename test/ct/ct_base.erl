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
%%% Created : 2022-01-10T13:35:08+00:00
%%%-------------------------------------------------------------------
-module(ct_base).

-author("yangcancai").

-export([new_meck/0, new_meck/2, bd_store_expect/0, bd_store_expect/2, del_meck/0]).
new_meck() ->
 new_meck(bd_store_redis,[passthrough, non_strict, no_link]).
new_meck(Mod, L) ->
  ok = meck:new(Mod, L),
  ok.
bd_store_expect() ->
  ok = bd_store_expect(bd_store_redis, backend_store).
bd_store_expect(Mod, Tab) ->
  backend_store = ets:new(backend_store, [named_table, set, public]),
  ok =
    meck:expect(Mod,
      handle_get,
      fun(BigKey) ->
        case ets:lookup(Tab, BigKey) of
          [{_, V}] ->
            V;
          [] ->
            []
        end
      end),
  ok =
    meck:expect(Mod,
      handle_put,
      fun(BigKey, RowDataList) ->
        true = ets:insert(Tab, {BigKey, RowDataList}),
        ok
      end),
  ok =
    meck:expect(Mod,
      handle_put,
      fun(List) ->
        [true = ets:insert(Tab, {BigKey, RowDataList})
          || {BigKey, RowDataList} <- List],
        ok
      end),

  ok =
    meck:expect(Mod,
      handle_del,
      fun(BigKey) ->
        ets:delete(Tab, BigKey),
        ok
      end),
  ok.

del_meck() ->
  meck:unload().
