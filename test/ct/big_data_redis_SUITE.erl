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

-author("yangcancai").

-include("big_data_ct.hrl").

-compile(export_all).

-define(APP, big_data).

all() ->
    [insert_check,
     insert_new,
     not_found,
     remove,
     remove_row,
     remove_row_ids,
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
     overflow].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(?APP),
    new_meck(),
    Config.

end_per_suite(Config) ->
    del_meck(),
    ok = application:stop(?APP),
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

new_meck() ->
    % ok = meck:new(big_data_redis, [non_strict, no_link]),
    ok.

expect() ->
    % ok = meck:expect(big_data_redis, test, fun() -> {ok, 1} end).
    ok.

del_meck() ->
    meck:unload().

insert_check(_Config) ->
    {ok, Ref} = big_data_redis:new(),
    BigKey = <<"a">>,
    BigKey = <<"a">>,
    big_data_redis:remove(Ref, BigKey),
    big_data_redis:insert(Ref,
                          BigKey,
                          #row_data{row_id = <<"1">>,
                                    term = {a, 1},
                                    time = 1}),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {a, 1},
                            time = 1}],
                 big_data_redis:get(Ref, BigKey)),

    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {a, 1},
                            time = 1}],
                 big_data_redis:get_row(Ref, BigKey, <<"1">>)),
    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = ?BD_RAND_BYTES(32),
                                        term = ?BD_RAND_BYTES(500),
                                        time = 1}),
    ok.

insert_new(_) ->
    {ok, Ref} = big_data_redis:new(),
    BigKey = <<"a">>,
    big_data_redis:remove(Ref, BigKey),
    ok =
        big_data_redis:insert(Ref,
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
                 big_data_redis:get(Ref, BigKey)),
    ok.

not_found(_) ->
    {ok, Ref} = big_data_redis:new(),
    BigKey = <<"a">>,
    RowID = <<"1">>,
    big_data_redis:remove(Ref, BigKey),
    ?assertEqual([], big_data_redis:get(Ref, BigKey)),
    ?assertEqual([], big_data_redis:get_row(Ref, BigKey, RowID)),
    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = <<"2">>,
                                        time = 1,
                                        term = 1}),
    ?assertEqual([], big_data_redis:get_row(Ref, BigKey, RowID)),
    ok.

remove(_) ->
    {ok, Ref} = big_data_redis:new(),
    BigKey = <<"a">>,
    RowID = <<"1">>,
    big_data_redis:remove(Ref, BigKey),
    big_data_redis:insert(Ref,
                          BigKey,
                          #row_data{row_id = RowID,
                                    term = 1,
                                    time = 1}),
    ?assertEqual([#row_data{row_id = RowID,
                            term = 1,
                            time = 1}],
                 big_data_redis:get(Ref, BigKey)),
    big_data_redis:remove(Ref, BigKey),
    ?assertEqual([], big_data_redis:get(Ref, BigKey)),
    ?assertEqual([], big_data_redis:get_row(Ref, BigKey, RowID)),
    ok.

remove_row(_) ->
    {ok, Ref} = big_data_redis:new(),
    BigKey = <<"a">>,
    RowID = <<"1">>,
    big_data_redis:remove(Ref, BigKey),
    big_data_redis:insert(Ref,
                          BigKey,
                          #row_data{row_id = RowID,
                                    term = 1,
                                    time = 1}),
    big_data_redis:insert(Ref,
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
                 big_data_redis:get(Ref, BigKey)),
    big_data_redis:remove_row(Ref, BigKey, RowID),
    ?assertEqual([#row_data{row_id = <<"2">>,
                            term = 1,
                            time = 2}],
                 big_data_redis:get(Ref, BigKey)),
    ?assertEqual([], big_data_redis:get_row(Ref, BigKey, RowID)),
    ?assertEqual([#row_data{row_id = <<"2">>,
                            term = 1,
                            time = 2}],
                 big_data_redis:get_row(Ref, BigKey, <<"2">>)),
    ok.

clear(_) ->
    {ok, Ref} = big_data_redis:new(),
    BigKey = <<"a">>,
    RowID = <<"1">>,
    big_data_redis:remove(Ref, BigKey),
    big_data_redis:insert(Ref,
                          BigKey,
                          #row_data{row_id = RowID,
                                    term = 1,
                                    time = 1}),
    big_data_redis:insert(Ref,
                          BigKey,
                          #row_data{row_id = <<"2">>,
                                    term = 1,
                                    time = 2}),
    big_data_redis:insert(Ref,
                          <<"b">>,
                          #row_data{row_id = <<"2">>,
                                    term = 1,
                                    time = 2}),
    ?assertEqual([#row_data{row_id = <<"2">>,
                            term = 1,
                            time = 2}],
                 big_data_redis:get(Ref, <<"b">>)),
    ?assertEqual([#row_data{row_id = RowID,
                            term = 1,
                            time = 1}],
                 big_data_redis:get_row(Ref, BigKey, RowID)),
    ok.

range(_) ->
    {ok, Ref} = big_data_redis:new(),
    BigKey = <<"a">>,
    big_data_redis:remove(Ref, BigKey),
    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = <<"1">>,
                                        term = 1,
                                        time = 10}),
    ok =
        big_data_redis:insert(Ref,
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
                 big_data_redis:get_range(Ref, BigKey, 2, 10)),

    ?assertEqual([#row_data{row_id = <<"2">>,
                            term = 2,
                            time = 2}],
                 big_data_redis:get_range(Ref, BigKey, 2, 2)),

    ok =
        big_data_redis:insert(Ref,
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
                 big_data_redis:get_range(Ref, BigKey, 2, 2)),

    ?assertEqual([<<"2">>, <<"1">>], big_data_redis:get_range_row_ids(Ref, BigKey, 2, 2)),
    ?assertEqual([<<"2">>, <<"1">>], big_data_redis:get_row_ids(Ref, BigKey, 2)),
    ok.

get_time_index(_) ->
    {ok, Ref} = big_data_redis:new(),
    BigKey = <<"a">>,
    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = <<"1">>,
                                        term = 1,
                                        time = 10}),
    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = <<"2">>,
                                        term = 2,
                                        time = 2}),

    ?assertEqual(2, big_data_redis:get_time_index(Ref, BigKey, <<"2">>)),
    ok.

update_counter(_) ->
    {ok, Ref} = big_data_redis:new(),
    BigKey = <<"a">>,
    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = <<"1">>,
                                        term = 1,
                                        time = 10}),
    ?assertEqual(notfound, big_data_redis:update_counter(Ref, <<"t">>, <<"1">>, {0, 2})),
    ?assertEqual([true], big_data_redis:update_counter(Ref, BigKey, <<"1">>, {0, 2})),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = 3,
                            time = 10}],
                 big_data_redis:get_row(Ref, BigKey, <<"1">>)),

    ?assertEqual([true], big_data_redis:update_elem(Ref, BigKey, <<"1">>, {0, {1, 2}})),
    ?assertEqual([true, true],
                 big_data_redis:update_counter(Ref, BigKey, <<"1">>, [{0, 2}, {1, 3}])),

    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {3, 5},
                            time = 10}],
                 big_data_redis:get_row(Ref, BigKey, <<"1">>)),

    ?assertEqual([false, true, true, false],
                 big_data_redis:update_counter(Ref,
                                               BigKey,
                                               <<"1">>,
                                               [{4, 0}, {0, 2}, {1, 3}, {2, 4}])),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {5, 8},
                            time = 10}],
                 big_data_redis:get_row(Ref, BigKey, <<"1">>)),

    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = <<"1">>,
                                        term = 1,
                                        time = 10}),
    ?assertEqual([true], big_data_redis:update_counter(Ref, BigKey, <<"1">>, {0, 2.6})),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = 3.6,
                            time = 10}],
                 big_data_redis:get_row(Ref, BigKey, <<"1">>)),
    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = <<"1">>,
                                        term = {1, 1.0},
                                        time = 10}),
    ?assertEqual([true, true],
                 big_data_redis:update_counter(Ref, BigKey, <<"1">>, [{1, 6}, {0, 2}])),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {3, 7.0},
                            time = 10}],
                 big_data_redis:get_row(Ref, BigKey, <<"1">>)),

    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = <<"1">>,
                                        term = <<"aaa">>,
                                        time = 10}),

    ?assertEqual([true], big_data_redis:update_counter(Ref, BigKey, <<"1">>, {0, 2.6})),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = <<"aaa">>,
                            time = 10}],
                 big_data_redis:get_row(Ref, BigKey, <<"1">>)),

    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = <<"1">>,
                                        term = {a, 0.1},
                                        time = 10}),

    ?assertEqual([true], big_data_redis:update_counter(Ref, BigKey, <<"1">>, {1, 2.6})),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {a, 2.7},
                            time = 10}],
                 big_data_redis:get_row(Ref, BigKey, <<"1">>)),

    ?assertEqual([true],
                 big_data_redis:update_counter(Ref, BigKey, <<"1">>, {1, 9223372036854775807})),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {a, 9.223372036854776e18},
                            time = 10}],
                 big_data_redis:get_row(Ref, BigKey, <<"1">>)),

    ok.

update_elem(_) ->
    {ok, Ref} = big_data_redis:new(),
    BigKey = <<"a">>,
    big_data_redis:remove(Ref, BigKey),
    big_data_redis:remove(Ref, <<"b">>),
    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = <<"1">>,
                                        term = 1,
                                        time = 10}),

    ?assertEqual(notfound, big_data_redis:update_elem(Ref, <<"b">>, <<"0">>, {0, 2})),
    ?assertEqual([true], big_data_redis:update_elem(Ref, BigKey, <<"1">>, {0, 2})),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = 2,
                            time = 10}],
                 big_data_redis:get_row(Ref, BigKey, <<"1">>)),

    ?assertEqual([false], big_data_redis:update_elem(Ref, BigKey, <<"2">>, {0, 2})),
    ?assertEqual([false], big_data_redis:update_elem(Ref, BigKey, <<"1">>, {1, 2})),

    ?assertEqual([true],
                 big_data_redis:update_elem(Ref, BigKey, <<"1">>, {0, {a, 1, <<"hello">>}})),

    ?assertEqual([true, true, true],
                 big_data_redis:update_elem(Ref,
                                            BigKey,
                                            <<"1">>,
                                            [{0, b}, {1, 2}, {2, <<"world">>}])),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {b, 2, <<"world">>},
                            time = 10}],
                 big_data_redis:get_row(Ref, BigKey, <<"1">>)),

    ?assertEqual([true, true, false],
                 big_data_redis:update_elem(Ref,
                                            BigKey,
                                            <<"1">>,
                                            [{0, c}, {1, 3}, {3, <<"world">>}])),

    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {c, 3, <<"world">>},
                            time = 10}],
                 big_data_redis:get_row(Ref, BigKey, <<"1">>)),

    ok.

lookup_elem(_) ->
    {ok, Ref} = big_data_redis:new(),
    BigKey = <<"a">>,
    big_data_redis:remove(Ref, BigKey),
    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = <<"1">>,
                                        term = 1,
                                        time = 10}),
    ?assertEqual({1}, big_data_redis:lookup_elem(Ref, BigKey, <<"1">>, [0])),
    ?assertEqual({1}, big_data_redis:lookup_elem(Ref, BigKey, <<"1">>, {0})),
    ?assertEqual({1}, big_data_redis:lookup_elem(Ref, BigKey, <<"1">>, 0)),
    [true] = big_data_redis:update_elem(Ref, BigKey, <<"1">>, {0, {a, 1}}),
    ?assertEqual({a, 1}, big_data_redis:lookup_elem(Ref, BigKey, <<"1">>, {0, 1, 2})),
    ?assertEqual({1, a}, big_data_redis:lookup_elem(Ref, BigKey, <<"1">>, {2, 1, 0})),
    ?assertEqual([#row_data{row_id = <<"1">>,
                            term = {a, 1},
                            time = 10}],
                 big_data_redis:get_row(Ref, BigKey, <<"1">>)),

    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = <<"1">>,
                                        term = 1,
                                        time = 10}),
    [true] =
        big_data_redis:update_elem(Ref,
                                   BigKey,
                                   <<"1">>,
                                   {0, {{a, b}, [1, 2, 3], <<"hello">>, 8}}),

    ?assertEqual({{a, b}, [1, 2, 3], <<"hello">>},
                 big_data_redis:lookup_elem(Ref, BigKey, <<"1">>, {0, 1, 2})),

    ?assertEqual({}, big_data_redis:lookup_elem(Ref, BigKey, <<"1">>, {9})),

    ok.

remove_row_ids(_) ->
    {ok, Ref} = big_data_redis:new(),
    BigKey = <<"a">>,
    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = <<"1">>,
                                        term = 1,
                                        time = 1}),
    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = <<"2">>,
                                        term = 1,
                                        time = 1}),
    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = <<"3">>,
                                        term = 1,
                                        time = 0}),

    ?assertEqual(ok, big_data_redis:remove_row_ids(Ref, BigKey, 1, 1)),
    ?assertEqual([#row_data{row_id = <<"3">>,
                            term = 1,
                            time = 0}],
                 big_data_redis:get(Ref, BigKey)),

    ?assertEqual(ok, big_data_redis:remove_row_ids(Ref, BigKey, 0, 1)),
    ?assertEqual([], big_data_redis:get(Ref, BigKey)),
    ok.

insert_binary(_) ->
    {ok, Ref} = big_data_redis:new(),
    BigKey = <<"a">>,
    big_data_redis:remove(Ref, BigKey),
    [begin
         Term = crypto:strong_rand_bytes(100),
         RowData =
             #row_data{row_id = <<"1">>,
                       term = Term,
                       time = 1},
         ok = big_data_redis:insert(Ref, BigKey, RowData),
         ?assertEqual([RowData], big_data_redis:get(Ref, BigKey))
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
    do_insert_check(?LINE, erlang:binary_to_atom(erlang:list_to_binary(string:copies("a", 100)),utf8)),
    ok.
string(_) ->
    do_insert_check(?LINE, "a"),
    do_insert_check(?LINE, string:copies("a", 1000)),
    ok.
binary(_) ->
    do_insert_check(?LINE, <<"a">>),
    do_insert_check(?LINE, <<"你好我是,中国人"/utf8>>),
    do_insert_check(?LINE, erlang:term_to_binary({})),
    do_insert_check(?LINE, erlang:list_to_binary(string:copies("a", 1000))),
    do_insert_check(?LINE, {<<"a">>}),
    ok.
list(_) ->
    do_insert_check(?LINE, [N || N <- lists:seq(1, 10000)]),
    ok.
tuple(_) ->
    do_insert_check(?LINE, erlang:list_to_tuple([N || N <- lists:seq(1, 10000)])),
    ok.
all_term(_) ->
    do_insert_check(?LINE, {}),
    do_insert_check(?LINE, {[]}),
    do_insert_check(?LINE, []),
    do_insert_check(?LINE, [[]]),
    do_insert_check(?LINE, {a}),
    do_insert_check(?LINE, {"a"}),
    do_insert_check(?LINE, {["a"]}),
    ok.

do_insert_check(Line, Term) ->
    {ok, Ref} = big_data_redis:new(),
    BigKey = uid(),
    RowID = uid(),
    T = erlang:system_time(1),
    ok = big_data_redis:insert(Ref, BigKey, RowID, T, Term),
    ?assertEqual({Line,
                  [#row_data{row_id = RowID,
                             term = Term,
                             time = T}]},
                 {Line, big_data_redis:get(Ref, BigKey)}),
    ok = big_data_redis:remove(Ref, BigKey),
    ok.

overflow(_) ->
    {ok, Ref} = big_data_redis:new(),
    BigKey = uid(),
    RowID = uid(),
    T = erlang:system_time(1),

    ok =
        big_data_redis:insert(Ref,
                              BigKey,
                              #row_data{row_id = RowID,
                                        term = {a, 1},
                                        time = T}),
    ?assertEqual([false],
                 big_data_redis:update_counter(Ref, BigKey, RowID, {1, 9223372036854775807})),
    ?assertEqual([#row_data{row_id = RowID,
                            term = {a, 1},
                            time = T}],
                 big_data_redis:get(Ref, BigKey)),

    ok.

uid() ->
    base64:encode(
        crypto:strong_rand_bytes(32)).
