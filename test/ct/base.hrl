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

-include("big_data_ct.hrl").

-compile(export_all).

-define(APP, big_data).
-define(BACKEND, (backend())).

all() ->
    [insert_check,
     insert_new,
     not_found,
     remove,
     remove_row,
     remove_row_ids,
     append,
     clear,
     range,
     get_time_index,
     update_elem,
     update_counter,
     lookup_elem,
     insert_binary,
     all_term,
     integer,
     float,
     atom,
     string,
     tuple,
     list,
     binary,
     map,
     overflow,
     pid,
     func].

init_per_suite(Config) ->
    B = case ?BACKEND of
      big_data_redis ->
        redis;
      _->
        local
    end,
    ok = application:set_env(big_data, big_data_backend, B),
  ct_base:new_meck(),
  {ok, _} = application:ensure_all_started(?APP),
    Config.

end_per_suite(Config) ->
    ct_base:del_meck(),
    ok = application:stop(?APP),
    Config.

init_per_testcase(_Case, Config) ->
    ct_base:bd_store_expect(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

insert_check(Config) ->
    insert_check(Config, big_data_redis),
    insert_check(Config, big_data_local).

insert_check(_, _Module) ->
    {ok, Ref} = ?BACKEND:new(),
    BigKey = <<"a">>,
    BigKey = <<"a">>,
    ?BACKEND:remove(Ref, BigKey),
    ?BACKEND:insert(Ref,
                    BigKey,
                    #row_data{row_id = <<"1">>,
                              term = {a, 1},
                              time = 1}),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {a, 1},
                            time = 1}],
                 ?BACKEND:get(Ref, BigKey)),

    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {a, 1},
                            time = 1}],
                 ?BACKEND:get_row(Ref, BigKey, <<"1">>)),
    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = ?BD_RAND_BYTES(32),
                                  term = ?BD_RAND_BYTES(500),
                                  time = 1}),
    ok.

insert_new(_) ->
    {ok, Ref} = ?BACKEND:new(),
    BigKey = <<"a">>,
    ?BACKEND:remove(Ref, BigKey),
    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        [#row_data{row_id = <<"1">>,
                                   time = 1,
                                   term = 1},
                         #row_data{row_id = <<"2">>,
                                   time = 1,
                                   term = 2}]),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            time = 1,
                            term = 1},
                  #row_data{row_id = <<"2">>,
                            time = 1,
                            term = 2}],
                 ?BACKEND:get(Ref, BigKey)),
    ok.

not_found(_) ->
    {ok, Ref} = ?BACKEND:new(),
    BigKey = <<"a">>,
    RowID = <<"1">>,
    ?BACKEND:remove(Ref, BigKey),
    ?assertEqual([], ?BACKEND:get(Ref, BigKey)),
    ?assertEqual([], ?BACKEND:get_row(Ref, BigKey, RowID)),
    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"2">>,
                                  time = 1,
                                  term = 1}),
    ?assertEqual([], ?BACKEND:get_row(Ref, BigKey, RowID)),
    ok.

remove(_) ->
    {ok, Ref} = ?BACKEND:new(),
    BigKey = <<"a">>,
    RowID = <<"1">>,
    ?BACKEND:remove(Ref, BigKey),
    ?BACKEND:insert(Ref,
                    BigKey,
                    #row_data{row_id = RowID,
                              term = 1,
                              time = 1}),
    ?assertEqual([#row_data{row_id = RowID,
                            term = 1,
                            time = 1}],
                 ?BACKEND:get(Ref, BigKey)),
    ?BACKEND:remove(Ref, BigKey),
    ?assertEqual([], ?BACKEND:get(Ref, BigKey)),
    ?assertEqual([], ?BACKEND:get_row(Ref, BigKey, RowID)),
    ok.

remove_row(_) ->
    {ok, Ref} = ?BACKEND:new(),
    BigKey = <<"a">>,
    RowID = <<"1">>,
    ?BACKEND:remove(Ref, BigKey),
    ?BACKEND:insert(Ref,
                    BigKey,
                    #row_data{row_id = RowID,
                              term = 1,
                              time = 1}),
    ?BACKEND:insert(Ref,
                    BigKey,
                    #row_data{row_id = <<"2">>,
                              term = 1,
                              time = 2}),
    ?assertEqual([#row_data{row_id = RowID,
                            term = 1,
                            time = 1},
                  #row_data{row_id = <<"2">>,
                            term = 1,
                            time = 2}],
                 ?BACKEND:get(Ref, BigKey)),
    ?BACKEND:remove_row(Ref, BigKey, RowID),
    ?assertEqual([#row_data{row_id = <<"2">>,
                            term = 1,
                            time = 2}],
                 ?BACKEND:get(Ref, BigKey)),
    ?assertEqual([], ?BACKEND:get_row(Ref, BigKey, RowID)),
    ?assertEqual([#row_data{row_id = <<"2">>,
                            term = 1,
                            time = 2}],
                 ?BACKEND:get_row(Ref, BigKey, <<"2">>)),
    ok.

clear(_) ->
    {ok, Ref} = ?BACKEND:new(),
    BigKey = <<"a">>,
    RowID = <<"1">>,
    ?BACKEND:remove(Ref, BigKey),
    ?BACKEND:insert(Ref,
                    BigKey,
                    #row_data{row_id = RowID,
                              term = 1,
                              time = 1}),
    ?BACKEND:insert(Ref,
                    BigKey,
                    #row_data{row_id = <<"2">>,
                              term = 1,
                              time = 2}),
    ?BACKEND:insert(Ref,
                    <<"b">>,
                    #row_data{row_id = <<"2">>,
                              term = 1,
                              time = 2}),
    ?assertEqual([#row_data{row_id = <<"2">>,
                            term = 1,
                            time = 2}],
                 ?BACKEND:get(Ref, <<"b">>)),
    ?assertEqual([#row_data{row_id = RowID,
                            term = 1,
                            time = 1}],
                 ?BACKEND:get_row(Ref, BigKey, RowID)),
    ok.

range(_) ->
    {ok, Ref} = ?BACKEND:new(),
    BigKey = <<"a">>,
    ?BACKEND:remove(Ref, BigKey),
    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"1">>,
                                  term = 1,
                                  time = 10}),
    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"2">>,
                                  term = 2,
                                  time = 2}),
    ?assertEqual([#row_data{row_id = <<"2">>,
                            term = 2,
                            time = 2},
                  #row_data{row_id = <<"1">>,
                            term = 1,
                            time = 10}],
                 ?BACKEND:get_range(Ref, BigKey, 2, 10)),

    ?assertEqual([#row_data{row_id = <<"2">>,
                            term = 2,
                            time = 2}],
                 ?BACKEND:get_range(Ref, BigKey, 2, 2)),

    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"1">>,
                                  term = 1,
                                  time = 2}),
    ?assertEqual([#row_data{row_id = <<"2">>,
                            term = 2,
                            time = 2},
                  #row_data{row_id = <<"1">>,
                            term = 1,
                            time = 2}],
                 ?BACKEND:get_range(Ref, BigKey, 2, 2)),

    ?assertEqual([<<"2">>, <<"1">>], ?BACKEND:get_range_row_ids(Ref, BigKey, 2, 2)),
    ?assertEqual([<<"2">>, <<"1">>], ?BACKEND:get_row_ids(Ref, BigKey, 2)),
    ok.

get_time_index(_) ->
    {ok, Ref} = ?BACKEND:new(),
    BigKey = <<"a">>,
    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"1">>,
                                  term = 1,
                                  time = 10}),
    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"2">>,
                                  term = 2,
                                  time = 2}),

    ?assertEqual(2, ?BACKEND:get_time_index(Ref, BigKey, <<"2">>)),
    ok.

update_counter(_) ->
    {ok, Ref} = ?BACKEND:new(),
    BigKey = <<"a">>,
    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"1">>,
                                  term = 1,
                                  time = 10}),
    ?assertEqual(notfound, ?BACKEND:update_counter(Ref, <<"t">>, <<"1">>, {0, 2})),
    ?assertEqual([true], ?BACKEND:update_counter(Ref, BigKey, <<"1">>, {0, 2})),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = 3,
                            time = 10}],
                 ?BACKEND:get_row(Ref, BigKey, <<"1">>)),

    ?assertEqual([true], ?BACKEND:update_elem(Ref, BigKey, <<"1">>, {0, {1, 2}})),
    ?assertEqual([true, true],
                 ?BACKEND:update_counter(Ref, BigKey, <<"1">>, [{0, 2}, {1, 3}])),

    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {3, 5},
                            time = 10}],
                 ?BACKEND:get_row(Ref, BigKey, <<"1">>)),

    ?assertEqual([false, true, true, false],
                 ?BACKEND:update_counter(Ref, BigKey, <<"1">>, [{4, 0}, {0, 2}, {1, 3}, {2, 4}])),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {5, 8},
                            time = 10}],
                 ?BACKEND:get_row(Ref, BigKey, <<"1">>)),

    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"1">>,
                                  term = 1,
                                  time = 10}),
    ?assertEqual([true], ?BACKEND:update_counter(Ref, BigKey, <<"1">>, {0, 2.6})),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = 3.6,
                            time = 10}],
                 ?BACKEND:get_row(Ref, BigKey, <<"1">>)),
    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"1">>,
                                  term = {1, 1.0},
                                  time = 10}),
    ?assertEqual([true, true],
                 ?BACKEND:update_counter(Ref, BigKey, <<"1">>, [{1, 6}, {0, 2}])),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {3, 7.0},
                            time = 10}],
                 ?BACKEND:get_row(Ref, BigKey, <<"1">>)),

    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"1">>,
                                  term = <<"aaa">>,
                                  time = 10}),

    ?assertEqual([true], ?BACKEND:update_counter(Ref, BigKey, <<"1">>, {0, 2.6})),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = <<"aaa">>,
                            time = 10}],
                 ?BACKEND:get_row(Ref, BigKey, <<"1">>)),

    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"1">>,
                                  term = {a, 0.1},
                                  time = 10}),

    ?assertEqual([true], ?BACKEND:update_counter(Ref, BigKey, <<"1">>, {1, 2.6})),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {a, 2.7},
                            time = 10}],
                 ?BACKEND:get_row(Ref, BigKey, <<"1">>)),

    ?assertEqual([true],
                 ?BACKEND:update_counter(Ref, BigKey, <<"1">>, {1, 9223372036854775807})),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {a, 9.223372036854776e18},
                            time = 10}],
                 ?BACKEND:get_row(Ref, BigKey, <<"1">>)),

    ok.

update_elem(_) ->
    {ok, Ref} = ?BACKEND:new(),
    BigKey = <<"a">>,
    ?BACKEND:remove(Ref, BigKey),
    ?BACKEND:remove(Ref, <<"b">>),
    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"1">>,
                                  term = 1,
                                  time = 10}),

    ?assertEqual(notfound, ?BACKEND:update_elem(Ref, <<"b">>, <<"0">>, {0, 2})),
    ?assertEqual([true], ?BACKEND:update_elem(Ref, BigKey, <<"1">>, {0, 2})),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = 2,
                            time = 10}],
                 ?BACKEND:get_row(Ref, BigKey, <<"1">>)),

    ?assertEqual([false], ?BACKEND:update_elem(Ref, BigKey, <<"2">>, {0, 2})),
    ?assertEqual([false], ?BACKEND:update_elem(Ref, BigKey, <<"1">>, {1, 2})),

    ?assertEqual([true],
                 ?BACKEND:update_elem(Ref, BigKey, <<"1">>, {0, {a, 1, <<"hello">>}})),

    ?assertEqual([true, true, true],
                 ?BACKEND:update_elem(Ref, BigKey, <<"1">>, [{0, b}, {1, 2}, {2, <<"world">>}])),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {b, 2, <<"world">>},
                            time = 10}],
                 ?BACKEND:get_row(Ref, BigKey, <<"1">>)),

    ?assertEqual([true, true, false],
                 ?BACKEND:update_elem(Ref, BigKey, <<"1">>, [{0, c}, {1, 3}, {3, <<"world">>}])),

    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {c, 3, <<"world">>},
                            time = 10}],
                 ?BACKEND:get_row(Ref, BigKey, <<"1">>)),

    ok.

lookup_elem(_) ->
    {ok, Ref} = ?BACKEND:new(),
    BigKey = <<"a">>,
    ?BACKEND:remove(Ref, BigKey),
    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"1">>,
                                  term = 1,
                                  time = 10}),
    ?assertEqual({1}, ?BACKEND:lookup_elem(Ref, BigKey, <<"1">>, [0])),
    ?assertEqual({1}, ?BACKEND:lookup_elem(Ref, BigKey, <<"1">>, {0})),
    ?assertEqual({1}, ?BACKEND:lookup_elem(Ref, BigKey, <<"1">>, 0)),
    [true] = ?BACKEND:update_elem(Ref, BigKey, <<"1">>, {0, {a, 1}}),
    ?assertEqual({a, 1}, ?BACKEND:lookup_elem(Ref, BigKey, <<"1">>, {0, 1, 2})),
    ?assertEqual({1, a}, ?BACKEND:lookup_elem(Ref, BigKey, <<"1">>, {2, 1, 0})),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {a, 1},
                            time = 10}],
                 ?BACKEND:get_row(Ref, BigKey, <<"1">>)),

    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"1">>,
                                  term = 1,
                                  time = 10}),
    [true] =
        ?BACKEND:update_elem(Ref, BigKey, <<"1">>, {0, {{a, b}, [1, 2, 3], <<"hello">>, 8}}),

    ?assertEqual({{a, b}, [1, 2, 3], <<"hello">>},
                 ?BACKEND:lookup_elem(Ref, BigKey, <<"1">>, {0, 1, 2})),

    ?assertEqual({}, ?BACKEND:lookup_elem(Ref, BigKey, <<"1">>, {9})),

    ok.

remove_row_ids(_) ->
    {ok, Ref} = ?BACKEND:new(),
    BigKey = <<"a">>,
    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"1">>,
                                  term = 1,
                                  time = 1}),
    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"2">>,
                                  term = 1,
                                  time = 1}),
    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = <<"3">>,
                                  term = 1,
                                  time = 0}),

    ?assertEqual(ok, ?BACKEND:remove_row_ids(Ref, BigKey, 1, 1)),
    ?assertEqual([#row_data{row_id = <<"3">>,
                            term = 1,
                            time = 0}],
                 ?BACKEND:get(Ref, BigKey)),

    ?assertEqual(ok, ?BACKEND:remove_row_ids(Ref, BigKey, 0, 1)),
    ?assertEqual([], ?BACKEND:get(Ref, BigKey)),
    ok.

insert_binary(_) ->
    {ok, Ref} = ?BACKEND:new(),
    BigKey = <<"a">>,
    ?BACKEND:remove(Ref, BigKey),
    [begin
         Term = crypto:strong_rand_bytes(100),
         RowData =
             #row_data{row_id = <<"1">>,
                       term = Term,
                       time = 1},
         ok = ?BACKEND:insert(Ref, BigKey, RowData),
         ?assertEqual([RowData], ?BACKEND:get(Ref, BigKey))
     end
     || _ <- lists:seq(1, 100)],
    ok.

integer(_) ->
    %% integer
    %% FIXME
    %% do_insert_check(?LINE,-999999999999999999999999999999999),
    % do_insert_check(?LINE,-9223372036854775809),
    do_insert_check(?LINE, -9223372036854775808),
    %% neg_integer
    do_insert_check(?LINE, -1),
    %% zero
    do_insert_check(?LINE, 0),
    %% pos_integer
    do_insert_check(?LINE, 1),
    do_insert_check(?LINE, 9223372036854775807),
    %% FIXME
    %% do_insert_check(?LINE,9223372036854775808),
    ok.

float(_) ->
    %% float
    do_insert_check(?LINE, -0.0000000000000000001),
    do_insert_check(?LINE, -1.7976931348623157e308),
    do_insert_check(?LINE, 1.7976931348623158e308),
    do_insert_check(?LINE, 0.0),
    do_insert_check(?LINE, 1.0),
    ok.

atom(_) ->
    %% atom
    do_insert_check(?LINE, a),
    %% large atom
    do_insert_check(?LINE,
                    erlang:binary_to_atom(
                        erlang:list_to_binary(
                            string:copies("a", 100)),
                        utf8)),
    ok.

string(_) ->
    do_insert_check(?LINE, "a"),
    Str = "Error: RawTerm::from_bytes crash, Parsing Error: Error { input: [104, 4, 100, 0, 8, 114, 111, 119, 95,
    100, 97, 116, 97, 109, 0, 0, 0, 44, 72, 98, 108, 53, 67, 65, 109, 69, 83, 120, 110, 74, 117, 72, 83, 90, 71, 119, 76, 115, 106, 43, 53, 104, 81, 67, 78, 79, 88, 84, 85, 75, 47, 57, 88, 82, 121, 78, 56, 119, 70, 113, 69, 61, 112, 0, 0, 0, 88, 0, 44, 212, 15, 6, 239, 255, 39, 82, 172, 235, 1, 114, 225, 79, 108, 229, 0, 0, 0, 0, 0, 0, 0, 0, 100, 0, 20, 98, 105, 103, 95, 100, 97, 116, 97, 95, 114, 101, 100, 105, 115, 95, 83, 85, 73, 84, 69, 97, 0, 98, 1, 102, 160, 120, 88, 100, 0, 13, 110, 111, 110, 111, 100, 101, 64, 110, 111, 104, 111, 115, 116, 0, 0, 3, 68, 0, 0, 0, 0, 0, 0, 0, 0, 98, 97, 238, 125, 14], code: Eof }",
    do_insert_check(?LINE, Str),
    do_insert_check(?LINE, string:copies("a", 1000)),
    ok.

binary(_) ->
    do_insert_check(?LINE, <<"a">>),
    do_insert_check(?LINE, <<"你好我是,中国人"/utf8>>),
    do_insert_check(?LINE, erlang:term_to_binary({})),
    do_insert_check(?LINE,
                    erlang:list_to_binary(
                        string:copies("a", 1000))),
    do_insert_check(?LINE, {<<"a">>}),
    ok.

list(_) ->
    do_insert_check(?LINE, [N || N <- lists:seq(1, 10000)]),
    ok.

tuple(_) ->
    do_insert_check(?LINE, erlang:list_to_tuple([N || N <- lists:seq(1, 10000)])),
    ok.

map(_) ->
    do_insert_check(?LINE, #{a => 1}),
    do_insert_check(?LINE, #{a => <<"你好吗?"/utf8>>}),
    ok.

all_term(_) ->
    do_insert_check(?LINE, {}),
    do_insert_check(?LINE, {[]}),
    do_insert_check(?LINE, []),
    do_insert_check(?LINE, [[]]),
    do_insert_check(?LINE, {a}),
    do_insert_check(?LINE, {"a"}),
    do_insert_check(?LINE, {["a"]}),
    do_insert_check(?LINE, #{}),
    do_insert_check(?LINE, {m,undefined,undefined,0,1649659818,1649659818,0,undefined,
      [{d,1649659818,<<"123">>,2,<<"0">>,<<"0">>,<<"1">>,<<"0">>,<<>>,
        <<"50000">>,<<>>,<<>>,<<>>,
        [{xmlel,<<"text">>,[],[{xmlcdata,<<"hello">>}]}]}],
      undefined}),
    ok.

do_insert_check(Line, Term) ->
    {ok, Ref} = ?BACKEND:new(),
    BigKey = uid(),
    RowID = uid(),
    T = erlang:system_time(1),
    ok = ?BACKEND:insert(Ref, BigKey, RowID, T, Term),
    ?assertEqual({Line,
                  [#row_data{row_id = RowID,
                             term = Term,
                             time = T}]},
                 {Line, ?BACKEND:get(Ref, BigKey)}),
    ok = ?BACKEND:remove(Ref, BigKey),
    ok.

overflow(_) ->
    {ok, Ref} = ?BACKEND:new(),
    BigKey = uid(),
    RowID = uid(),
    T = erlang:system_time(1),

    ok =
        ?BACKEND:insert(Ref,
                        BigKey,
                        #row_data{row_id = RowID,
                                  term = {a, 1},
                                  time = T}),
    ?assertEqual([false],
                 ?BACKEND:update_counter(Ref, BigKey, RowID, {1, 9223372036854775807})),
    ?assertEqual([#row_data{row_id = RowID,
                            term = {a, 1},
                            time = T}],
                 ?BACKEND:get(Ref, BigKey)),
    ?assertEqual([true],
                 ?BACKEND:update_counter(Ref, BigKey, RowID, {1, 9223372036854775806})),
    ?assertEqual([#row_data{row_id = RowID,
                            term = {a, 9223372036854775807},
                            time = T}],
                 ?BACKEND:get(Ref, BigKey)),

    ?assertEqual([true],
                 ?BACKEND:update_counter(Ref, BigKey, RowID, {1, -9223372036854775808})),

    ?assertEqual([#row_data{row_id = RowID,
                            term = {a, -1},
                            time = T}],
                 ?BACKEND:get(Ref, BigKey)),
    ?assertEqual([false],
                 ?BACKEND:update_counter(Ref, BigKey, RowID, {1, -9223372036854775808})),

    ?assertEqual([#row_data{row_id = RowID,
                            term = {a, -1},
                            time = T}],
                 ?BACKEND:get(Ref, BigKey)),
    ok.

uid() ->
    base64:encode(
        crypto:strong_rand_bytes(32)).
append(_) ->
    do_append_check(?LINE, {0,[{1,a,b},{2,c,"你好"}]}, {1,[{2,c,"不好"},{3,e,f}], "new"},
     {1,[{2,c,"不好"},{3,e,f}], "new"}, [{0,[{update, gt}]}, {1, [{type, list},{max_len,2},{replace_cond,0}]}]),
  do_append_check(?LINE,
    {m,undefined,undefined,0,undefined,undefined,0,undefined,
      [{d,1649651451,<<"123">>,2,<<"0">>,<<"0">>,<<"1">>,
        <<"0">>,<<>>,<<"50000">>,<<>>,<<>>,<<>>,
        [{xmlel,<<"text">>,[],[{xmlcdata,<<"hello">>}]}]}],
      undefined},
    {m,undefined,undefined,0,undefined,undefined,0,undefined,
      [{d,1649651451,<<"123">>,2,<<"0">>,<<"0">>,<<"1">>,
        <<"0">>,<<>>,<<"50000">>,<<>>,<<>>,<<>>,
        [{xmlel,<<"text">>,[],[{xmlcdata,<<"hello">>}]}]}],
      undefined},
    {m,undefined,undefined,0,undefined,undefined,0,undefined,
      [{d,1649651451,<<"123">>,2,<<"0">>,<<"0">>,<<"1">>,
        <<"0">>,<<>>,<<"50000">>,<<>>,<<>>,<<>>,
        [{xmlel,<<"text">>,[],[{xmlcdata,<<"hello">>}]}]},
       {d,1649651451,<<"123">>,2,<<"0">>,<<"0">>,<<"1">>,
          <<"0">>,<<>>,<<"50000">>,<<>>,<<>>,<<>>,
          [{xmlel,<<"text">>,[],[{xmlcdata,<<"hello">>}]}]}],
      undefined},
  [{8,[{max_len,100},{type,list}]},{3,[{update,always}]},location]),
ok.
only_update_location(_) ->
    do_append_check(?LINE,
                    {0, [{1, a, b}, {2, c, "你好"}]},
                    {1, [{2, c, "不好"}, {3, e, f}], "new"},
                    {0, [{2, c, "不好"}, {3, e, f}], "new"},
                    [location, {1, [{type, list}, {max_len, 2}, {replace_cond, 0}]}]),
    do_append_check(?LINE,
                    {0, [{1, a, b}, {2, c, "你好"}]},
                    {1, [{2, c, "不好"}, {3, e, f}]},
                    {0, [{2, c, "不好"}, {3, e, f}]},
                    [location, {1, [{type, list}, {max_len, 2}, {replace_cond, 0}]}]),
    do_append_check(?LINE,
                    {0, [{1, a, b}, {2, c, "你好"}]},
                    {1, {2, c, "不好"}},
                    {0, [{1, a, b}, {2, c, "不好"}]},
                    [location, {1, [{type, list}, {max_len, 2}, {replace_cond, 0}]}]),

    do_append_check(?LINE,
                    {0, [{1, a, b}, {2, c, "你好"}]},
                    {1},
                    {0, [{1, a, b}, {2, c, "你好"}]},
                    [location, {1, [{type, list}, {max_len, 2}, {replace_cond, 0}]}]),

    ok.

do_append_check(Line, OldTerm, NewTerm, ExpectTerm, Option) ->
    {ok, Ref} = ?BACKEND:new(),
    BigKey = uid(),
    RowID = uid(),
    T = erlang:system_time(1),
    ok = ?BACKEND:append(Ref, BigKey, #row_data{row_id=RowID, time=T,term=OldTerm}, []),
    ?assertEqual({Line,
                  [#row_data{row_id = RowID,
                             term = OldTerm,
                             time = T}]},
                 {Line, ?BACKEND:get(Ref, BigKey)}),

    ok = ?BACKEND:append(Ref, BigKey, #row_data{row_id=RowID, time=T,term=NewTerm}, Option),
    ?assertEqual({Line,
                  [#row_data{row_id = RowID,
                             term = ExpectTerm,
                             time = T}]},
                 {Line, ?BACKEND:get(Ref, BigKey)}),

    ok = ?BACKEND:remove(Ref, BigKey),
    ok.

