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
%%% Created : 2021-05-25T03:12:27+00:00
%%%-------------------------------------------------------------------
-module(big_log_wal_test).

-include_lib("eunit/include/eunit.hrl").

sort_file_test() ->
    Files = bd_log_wal:sort_file(["data/1_00000001.wal", "data/1111_00000002.wal"]),
    ?assertEqual(Files, ["data/1_00000001.wal", "data/1111_00000002.wal"]),
    Files = bd_log_wal:sort_file(["data/1111_00000002.wal", "data/1_00000001.wal"]),
    ?assertEqual(Files, ["data/1_00000001.wal", "data/1111_00000002.wal"]),
    Fiels1 = bd_log_wal:sort_file(["data/00000001.wal", "data/00000002.wal"]),
    ?assertEqual(Fiels1, ["data/00000001.wal", "data/00000002.wal"]),
    Fiels1 = bd_log_wal:sort_file(["data/00000002.wal", "data/00000001.wal"]),
    ok.

zpad_extract_num_test() ->
    ?assertEqual(1, bd_log_wal:zpad_extract_num(["data/1_00000001.wal"])),
    ?assertEqual(1, bd_log_wal:zpad_extract_num(["data/00000001.wal"])),
    ok.

maybe_not_checkpoint_files_test() ->
    Files = bd_log_wal:maybe_not_checkpoint_files(["data/1_00000001.wal"], 0),
    ?assertEqual(Files, ["data/1_00000001.wal"]),
    ?assertEqual(["data/1_00000001.wal"],
                 bd_log_wal:maybe_not_checkpoint_files(["data/1_00000001.wal"], 1)),
    ?assertEqual(["data/1_00000001.wal"],
                 bd_log_wal:maybe_not_checkpoint_files(["data/1_00000001.wal"], 2)),

    ?assertEqual(["data/1_00000001.wal", "data/2_00000002.wal"],
                 bd_log_wal:maybe_not_checkpoint_files(["data/1_00000001.wal",
                                                        "data/2_00000002.wal"],
                                                       0)),

    ?assertEqual(["data/1_00000001.wal", "data/2_00000002.wal"],
                 bd_log_wal:maybe_not_checkpoint_files(["data/1_00000001.wal",
                                                        "data/2_00000002.wal"],
                                                       1)),
    ?assertEqual(["data/2_00000002.wal"],
                 bd_log_wal:maybe_not_checkpoint_files(["data/1_00000001.wal",
                                                        "data/2_00000002.wal"],
                                                       2)),

    ?assertEqual(["data/2_00000002.wal"],
                 bd_log_wal:maybe_not_checkpoint_files(["data/1_00000001.wal",
                                                        "data/2_00000002.wal"],
                                                       3)),
    ?assertEqual(["data/1_00000001.wal", "data/2_00000002.wal", "data/4_00000004.wal"],
                 bd_log_wal:maybe_not_checkpoint_files(["data/1_00000001.wal",
                                                        "data/2_00000002.wal",
                                                        "data/4_00000004.wal"],
                                                       0)),
    ?assertEqual(["data/1_00000001.wal", "data/2_00000002.wal", "data/4_00000004.wal"],
                 bd_log_wal:maybe_not_checkpoint_files(["data/1_00000001.wal",
                                                        "data/2_00000002.wal",
                                                        "data/4_00000004.wal"],
                                                       1)),
    ?assertEqual(["data/2_00000002.wal", "data/4_00000004.wal"],
                 bd_log_wal:maybe_not_checkpoint_files(["data/1_00000001.wal",
                                                        "data/2_00000002.wal",
                                                        "data/4_00000004.wal"],
                                                       2)),

    ?assertEqual(["data/2_00000002.wal", "data/5_00000004.wal"],
                 bd_log_wal:maybe_not_checkpoint_files(["data/1_00000001.wal",
                                                        "data/2_00000002.wal",
                                                        "data/5_00000004.wal"],
                                                       3)),
    ?assertEqual(["data/4_00000004.wal"],
                 bd_log_wal:maybe_not_checkpoint_files(["data/1_00000001.wal",
                                                        "data/2_00000002.wal",
                                                        "data/4_00000004.wal"],
                                                       4)),

    ?assertEqual(["data/4_00000004.wal"],
                 bd_log_wal:maybe_not_checkpoint_files(["data/1_00000001.wal",
                                                        "data/2_00000002.wal",
                                                        "data/4_00000004.wal"],
                                                       5)),
    ?assertEqual(["data/974509_00000027.wal", "data/1297037_00000028.wal"],
                 bd_log_wal:maybe_not_checkpoint_files(["data/974509_00000027.wal",
                                                        "data/1297037_00000028.wal"],
                                                       1028680)),

    ok.
