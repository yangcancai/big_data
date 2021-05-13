# big_data_nif
Safe Rust code for creating Erlang NIF to store big data 
## Features

* Sorted
* Range query data by time
* Range query row_ids by time
* Update element/counter by positon
* Lookup element by position
* Remove a row
* Remove Range row by time
* Clear Bucket
* Query all data of the Bucket
* Safe thread

## Required 
- cargo 1.52.0-nightly (32da9eaa5 2021-03-13) or later
- rebar 3.14.4 on Erlang/OTP 22 Erts 10.7.2.1

## Comand 
```shell
## suite test
$ make ct
./rebar3 do ct --dir test/ct -v --config test/ct/ct.config --sys_config config/test.config
===> Verifying dependencies...
    Finished release [optimized] target(s) in 0.07s
===> Analyzing applications...
===> Compiling big_data_nif
===> Running Common Test suites...

Common Test starting (cwd is /Users/admin/proj/rust/big_data_nif)



CWD set to: "/Users/admin/proj/rust/big_data_nif/_build/test/logs/ct_run.nonode@nohost.2021-05-12_18.22.45"

TEST INFO: 1 test(s), 10 case(s) in 1 suite(s)

Testing test.ct: Starting test, 10 test cases
%%% big_data_SUITE: ..........
Testing test.ct: TEST COMPLETE, 10 ok, 0 failed of 10 test cases

Updating /Users/admin/proj/rust/big_data_nif/_build/test/logs/index.html ... done
Updating /Users/admin/proj/rust/big_data_nif/_build/test/logs/all_runs.html ... done
$ make shell
# ./tool.sh replace_config
./rebar3 as test shell
===> Verifying dependencies...
    Finished release [optimized] target(s) in 0.04s
===> Analyzing applications...
===> Compiling big_data_nif
Erlang/OTP 22 [erts-10.7.2.1] [source] [64-bit] [smp:8:8] [ds:8:8:10] [async-threads:1] [hipe]

Eshell V10.7.2.1  (abort with ^G)
1> ===> Booted big_data_nif


```
## Example
### Insert and Get
```erlang
%% create a player bucket
3> {ok, Ref} = big_data_nif:new().
{ok,#Ref<0.2349964349.40239108.14596>}
%% insert a row to bucket
4> big_data_nif:insert(Ref, <<"player">>, {row_data,<<"1">>,{a,1},1}).
ok
%% query all data of player bucket
5> big_data_nif:get(Ref,<<"player">>).
[{row_data,<<"1">>,{a,1},1}]
%% query a row 
6> big_data_nif:get_row(Ref,<<"player">>, <<"1">>).
{row_data,<<"1">>,{a,1},1}
%% insert other row
7> big_data_nif:insert(Ref, <<"player">>, {row_data,<<"2">>,{a,1},1}).
ok
8> big_data_nif:get_row(Ref,<<"player">>, <<"2">>).                   
{row_data,<<"2">>,{a,1},1}
9> big_data_nif:get(Ref,<<"player">>).                                
[{row_data,<<"1">>,{a,1},1},{row_data,<<"2">>,{a,1},1}]
```

### Range Query

```erlang
1> big_data_nif:get(Ref,<<"player">>).
[]
12> 
12> big_data_nif:insert(Ref, <<"player">>, {row_data,<<"2">>,{a,1},1}).
ok
13> big_data_nif:insert(Ref, <<"player">>, {row_data,<<"1">>,{a,1},10}).
ok
14> big_data_nif:insert(Ref, <<"player">>, {row_data,<<"3">>,{a,1},0}). 
ok
%% query all data which will be sorted by time 
15> big_data_nif:get(Ref,<<"player">>).                                 
[{row_data,<<"3">>,{a,1},0},
 {row_data,<<"2">>,{a,1},1},
 {row_data,<<"1">>,{a,1},10}]
16> big_data_nif:get_range(Ref,<<"player">>,0,1).
[{row_data,<<"3">>,{a,1},0},{row_data,<<"2">>,{a,1},1}]
17> big_data_nif:get_range(Ref,<<"player">>,0,0).
[{row_data,<<"3">>,{a,1},0}]
18> big_data_nif:get_range(Ref,<<"player">>,0,10).
[{row_data,<<"3">>,{a,1},0},
 {row_data,<<"2">>,{a,1},1},
 {row_data,<<"1">>,{a,1},10}]

%% query the row_ids by range time
%% included 0 and included 10
%% result will be sorted by time
 19> big_data_nif:get_range_row_ids(Ref,<<"player">>,0,10).
[<<"3">>,<<"2">>,<<"1">>]
%% query the time by row_id
22> big_data_nif:get_time_index(Ref,<<"player">>,<<"1">>).
10
```
### Lookup Elem
```erlang
33> big_data_nif:insert(Ref, <<"player">>, {row_data,<<"3">>,{a,1,{a,b},<<"hello">>},0}).
ok
34> big_data_nif:get(Ref,<<"player">>).                                                  
[{row_data,<<"3">>,{a,1,{a,b},<<"hello">>},0}]
35> big_data_nif:lookup_elem(Ref,<<"player">>,<<"3">>,0).
{a}
36> big_data_nif:lookup_elem(Ref,<<"player">>,<<"3">>,[0,1]).
{a,1}
37> big_data_nif:lookup_elem(Ref,<<"player">>,<<"3">>,[0,1,3]).
{a,1,<<"hello">>}
```

### Update Elem

```erlang
24> big_data_nif:insert(Ref, <<"player">>, {row_data,<<"3">>,{a,1},0}).
ok
25> big_data_nif:update_elem(Ref,<<"player">>,<<"3">>,[{0,b},{1,2}]).  
[true,true]
26> big_data_nif:get(Ref,<<"player">>).
[{row_data,<<"3">>,{b,2},0}]
```
### Update Counter
```erlang
28> big_data_nif:insert(Ref, <<"player">>, {row_data,<<"3">>,{a,1,3,{4,5}},0}).
ok
29> big_data_nif:get(Ref,<<"player">>).                                        
[{row_data,<<"3">>,{a,1,3,{4,5}},0}]
30> big_data_nif:update_counter(Ref,<<"player">>,<<"3">>,[{1,1},{2,5}]).       
[true,true]
31> big_data_nif:get(Ref,<<"player">>).                                 
[{row_data,<<"3">>,{a,2,8,{4,5}},0}]
```

### Remove 

```erlang
2> big_data_nif:insert(Ref, <<"player">>, {row_data,<<"3">>,{a,1,3,{4,5}},0}).
ok
3> big_data_nif:insert(Ref, <<"player">>, {row_data,<<"4">>,{a,1,3,{4,5}},0}).
4> big_data_nif:get(Ref,<<"player">>).
[{row_data,<<"3">>,{a,1,3,{4,5}},0},
 {row_data,<<"4">>,{a,1,3,{4,5}},0}]
 %% remove player bucket
6> big_data_nif:remove(Ref,<<"player">>).
ok
7> big_data_nif:get(Ref,<<"player">>).   
[]
8> big_data_nif:insert(Ref, <<"player">>, {row_data,<<"4">>,{a,1,3,{4,5}},0}).
ok
9> big_data_nif:insert(Ref, <<"player">>, {row_data,<<"3">>,{a,1,3,{4,5}},0}).
ok
%% remove a row which belong to player bucket
10> big_data_nif:remove_row(Ref,<<"player">>,<<"3">>).
ok
11> big_data_nif:get(Ref,<<"player">>).                                        
[{row_data,<<"4">>,{a,1,3,{4,5}},0}]
12> big_data_nif:insert(Ref, <<"player">>, {row_data,<<"3">>,{a,1,3,{4,5}},0}).
ok
%% remove all rows which time between 0 and 0
13> big_data_nif:remove_row_ids(Ref,<<"player">>,0,0).                         
ok
14> big_data_nif:get(Ref,<<"player">>).                                        
[]
15> big_data_nif:insert(Ref, <<"player">>, {row_data,<<"3">>,{a,1,3,{4,5}},0}).
ok
16> big_data_nif:insert(Ref, <<"player1">>, {row_data,<<"4">>,{a,1,3,{4,5}},0}).
ok
17> big_data_nif:get(Ref,<<"player1">>).
[{row_data,<<"4">>,{a,1,3,{4,5}},0}]
18> big_data_nif:get(Ref,<<"player">>). 
[{row_data,<<"3">>,{a,1,3,{4,5}},0}]
%% clear all buckets
19> big_data_nif:clear(Ref).
ok
20> big_data_nif:get(Ref,<<"player1">>).
[]
21> big_data_nif:get(Ref,<<"player">>). 
[]
```