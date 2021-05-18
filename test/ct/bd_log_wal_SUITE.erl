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
%%% Created : 2021-05-18T03:19:49+00:00
%%%-------------------------------------------------------------------

-module(bd_log_wal_SUITE).

-author("yangcancai").

-include("big_data_ct.hrl").

-compile(export_all).

-define(APP, big_data).

all() ->
    [recover, write, checkpoint].

init_per_suite(Config) ->
    ok = application:load(?APP),
    new_meck(),
    Dir = "/tmp/big_data/test",
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    del_meck(),
    Config.

init_per_testcase(_Case, Config) ->
    bd_ets:new(),
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    {ok, Ref} = big_data_nif:new(),
    ok = persistent_term:put(big_data, Ref),
    ok = persistent_term:put('$bd_logger', logger),
    {ok, FileHandler} = bd_file_handle:start_link(),
    _ = ets:new(backend_store, [named_table, set, public]),
    ok = bd_store_expect(bd_store_redis, backend_store),
    bd_backend_store:set_backend_store(bd_store_redis),
    [{backend_store, backend_store}, {file_handler, FileHandler} | Config].

end_per_testcase(_Case, Config) ->
    _FileHandler = ?config(file_handler, Config),
    ok = bd_file_handle:stop(),
    ok.

new_meck() ->
    ok = meck:new(bd_store_redis, [non_strict, no_link]),
    ok.

bd_store_expect(Mod, Tab) ->
    ok =
        meck:expect(Mod,
                    handle_get,
                    fun(BigKey) ->
                       [{_, V}] = ets:lookup(Tab, BigKey),
                       V
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
                    handle_del,
                    fun(BigKey) ->
                       ets:delete(Tab, BigKey),
                       ok
                    end),
    ok.

del_meck() ->
    meck:unload().

recover(Config) ->
    Dir = ?config(dir, Config),
    bd_log_wal:make_dir(Dir),
    %% write frame to wal
    State =
        bd_log_wal:open_wal(#bd_log_wal_state{file_num = 1,
                                              file_name = filename:join(Dir, "00000001.wal"),
                                              data_dir = Dir}),
    Wal = #bd_wal{id = 1,
                  action = insert,
                  args = [<<"player">>, <<"1">>, 1, {a, 1}]},
    NewState = bd_log_wal:write_wal(State, Wal),
    ok = bd_log_wal:close_existing(NewState#bd_log_wal_state.fd),
    S = bd_log_wal:recover_wal(#{dir => Dir}),
    BigData = persistent_term:get(big_data),
    ?assertEqual(1, S#bd_log_wal_state.checkpoint_seq),
    ?assertEqual([], ets:tab2list(S#bd_log_wal_state.wal_buffer_tid)),
    ?assertEqual(1, bd_log_wal:lookup_checkpoint_seq(S#bd_log_wal_state.log_meta_ref)),
    ?assertEqual(filename:join(Dir, "00000002.wal"), S#bd_log_wal_state.file_name),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            time = 1,
                            term = {a, 1}}],
                 big_data:get(BigData, <<"player">>)),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            time = 1,
                            term = {a, 1}}],
                 bd_backend_store:handle_get(<<"player">>)),
    bd_log_wal:terminate(normal, S),
    ok.

write(Config) ->
    {ok, _} = bd_log_wal:start_link(#{dir => ?config(dir, Config)}),
    Wal = #bd_wal{id = 1,
                  action = insert,
                  args = [<<"player">>, <<"1">>, 1, {a, 1}]},
    ok = bd_log_wal:write_sync(Wal),
    ?assertEqual([Wal], bd_log_wal:wal_buffer()),
    ok = bd_log_wal:stop(),
    {ok, _} = bd_log_wal:start_link(#{dir => ?config(dir, Config), waiting_pid => self()}),
    receive
        {_, ?BD_RECOVER_FINISHED} ->
            ok
    end,
    ?assertEqual([#row_data{row_id = <<"1">>,
                            time = 1,
                            term = {a, 1}}],
                 bd_backend_store:handle_get(<<"player">>)),
    BigData = persistent_term:get(big_data),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            time = 1,
                            term = {a, 1}}],
                 big_data:get(BigData, <<"player">>)),

    ok.

checkpoint(_) ->
    ok.
