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
    ok = application:set_env(big_data, big_data_backend, local),
    big_data:start(),
    ct_base:new_meck(),
    Dir = "/tmp/big_data/test",
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    ct_base:del_meck(),
    Config.

init_per_testcase(_Case, Config) ->
    bd_ets:new(),
    bd_counter:new(),
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    {ok, Ref} = big_data_nif:new(),
    {ok, Ref1} = big_data_nif:new(),
    ok = persistent_term:put(big_data, Ref),
    ok = persistent_term:put(big_data_checkpoint, Ref1),

    ok = persistent_term:put('$bd_logger', logger),
    {ok, FileHandler} = bd_file_handle:start_link(),
    {ok, _} = bd_checkpoint:start_link(#{dir => ?config(dir, Config)}),
    ok = ct_base:bd_store_expect(bd_store_redis, backend_store),
    bd_backend_store:set_backend_store(bd_store_redis),
    [{backend_store, backend_store}, {file_handler, FileHandler} | Config].

end_per_testcase(_Case, Config) ->
    _FileHandler = ?config(file_handler, Config),
    ok = bd_file_handle:stop(),
    ok.


recover(Config) ->
    Dir = ?config(dir, Config),
    bd_log_wal:make_dir(Dir),
    %% write frame to wal
    State =
        bd_log_wal:open_wal(#bd_log_wal_state{file_num = 1,
                                              file_name = filename:join(Dir, "1_00000001.wal"),
                                              data_dir = Dir}),
    Wal = #bd_wal{action = insert, args = [<<"player">>, <<"1">>, 1, {a, 1}]},
    NewState = bd_log_wal:write_wal(State, Wal),
    ok = bd_log_wal:close_existing(NewState#bd_log_wal_state.fd),
    S = bd_log_wal:recover_wal(#{dir => Dir}),
    receive
        {waiting_pid, _} ->
            ok = persistent_term:put('$bd_log_wal_started', true),
            ok
    end,
    BigData = persistent_term:get(big_data),
    ?assertEqual(1, bd_checkpoint:seq()),
    ?assertEqual([], ets:tab2list(S#bd_log_wal_state.wal_buffer_tid)),
    ?assertEqual(1, bd_checkpoint:lookup_checkpoint_seq()),
    ?assertEqual(filename:join(Dir, "2_00000002.wal"), S#bd_log_wal_state.file_name),
    M = mod(),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            time = 1,
                            term = {a, 1}}],
                 M:get(BigData, <<"player">>)),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            time = 1,
                            term = {a, 1}}],
                 bd_backend_store:handle_get(<<"player">>)),
    bd_log_wal:terminate(normal, S),
    ok.

write(Config) ->
    {ok, _} = bd_log_wal:start_link(#{dir => ?config(dir, Config), waiting_pid => self()}),
    receive
        {_, ?BD_RECOVER_FINISHED} ->
            ok
    end,
    Wal = #bd_wal{action = insert, args = [<<"player">>, <<"1">>, 1, {a, 1}]},
    ok = bd_log_wal:write_sync(Wal),
    Wal1 = #bd_wal{action = insert, args = [<<"player">>, <<"2">>, 1, {a, 1}]},
    ok = bd_log_wal:write_sync(Wal1),
    ?assertEqual([Wal#bd_wal{id = 1}, Wal1#bd_wal{id = 2}], bd_log_wal:wal_buffer()),
    ok = bd_log_wal:stop(),
    timer:sleep(10),
    {ok, _} = bd_log_wal:start_link(#{dir => ?config(dir, Config), waiting_pid => self()}),
    receive
        {_, ?BD_RECOVER_FINISHED} ->
            ok
    end,
    ?assertEqual([#row_data{row_id = <<"1">>,
                            time = 1,
                            term = {a, 1}},
                  #row_data{row_id = <<"2">>,
                            time = 1,
                            term = {a, 1}}],
                 bd_backend_store:handle_get(<<"player">>)),
    BigData = persistent_term:get(big_data),
    M = mod(),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            time = 1,
                            term = {a, 1}},
                  #row_data{row_id = <<"2">>,
                            time = 1,
                            term = {a, 1}}],
                 M:get(BigData, <<"player">>)),

    UpdateElemWal = #bd_wal{action = update_elem, args = [<<"player">>, <<"1">>, [{1, 2}]]},
    UpdateCounterWal =
        #bd_wal{action = update_counter, args = [<<"player">>, <<"2">>, [{1, 2}]]},

    [true] = big_data:command(UpdateElemWal),
    [true] = big_data:command(UpdateCounterWal),
    M = mod(),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            time = 1,
                            term = {a, 2}},
                  #row_data{row_id = <<"2">>,
                            time = 1,
                            term = {a, 3}}],
                 M:get(BigData, <<"player">>)),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            time = 1,
                            term = {a, 1}},
                  #row_data{row_id = <<"2">>,
                            time = 1,
                            term = {a, 1}}],
                 bd_backend_store:handle_get(<<"player">>)),
    4 = bd_checkpoint:sync(),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            time = 1,
                            term = {a, 2}},
                  #row_data{row_id = <<"2">>,
                            time = 1,
                            term = {a, 3}}],
                 bd_backend_store:handle_get(<<"player">>)),
    RemoveRowWal = #bd_wal{action = remove_row, args = [<<"player">>, <<"1">>]},
    ok = big_data:command(RemoveRowWal),
    M = mod(),
    ?assertEqual([#row_data{row_id = <<"2">>,
                            time = 1,
                            term = {a, 3}}],
                 M:get(BigData, <<"player">>)),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            time = 1,
                            term = {a, 2}},
                  #row_data{row_id = <<"2">>,
                            time = 1,
                            term = {a, 3}}],
                 bd_backend_store:handle_get(<<"player">>)),
    5 = bd_checkpoint:sync(),
    ?assertEqual([#row_data{row_id = <<"2">>,
                            time = 1,
                            term = {a, 3}}],
                 bd_backend_store:handle_get(<<"player">>)),

    ok.

checkpoint(Config) ->
    M = mod(),
    {ok, _} = bd_log_wal:start_link(#{dir => ?config(dir, Config)}),
    Wal = #bd_wal{action = insert, args = [<<"player">>, <<"1">>, 1, {a, 1}]},
    ok = big_data:command(Wal),
    Wal1 = #bd_wal{action = insert, args = [<<"player">>, <<"2">>, 1, {a, 1}]},
    ok = big_data:command(Wal1),
    ?assertEqual([], bd_backend_store:handle_get(<<"player">>)),
    bd_checkpoint:sync(),
    S = bd_log_wal:i(),
    ?assertEqual(2, bd_checkpoint:seq()),
    ?assertEqual([], ets:tab2list(S#bd_log_wal_state.wal_buffer_tid)),
    ?assertEqual(2, bd_checkpoint:lookup_checkpoint_seq()),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            time = 1,
                            term = {a, 1}},
                  #row_data{row_id = <<"2">>,
                            time = 1,
                            term = {a, 1}}],
                 bd_backend_store:handle_get(<<"player">>)),
    BigData = persistent_term:get(big_data),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            time = 1,
                            term = {a, 1}},
                  #row_data{row_id = <<"2">>,
                            time = 1,
                            term = {a, 1}}],
                 M:get(BigData, <<"player">>)),

    ok.

mod() ->
    big_data:backend().
