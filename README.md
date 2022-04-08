# big_data
Safe Rust code for creating Erlang NIF to store big data, supported redis module  

![CI](https://github.com/yangcancai/big_data/actions/workflows/ci.yml/badge.svg)

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
* Supported redis module(supoorted aof,rdb_save,rdb_load and aofrewrite)
* Erlang External Term Format to communicate with redis

## Supported erlang type
* float(f64)
* integer(i64)
* atom
* string
* list
* binary
* tuple
* map
* pid
* function
* reference

## Required 
- cargo 1.52.0 (32da9eaa5 2021-03-13) or later
- rebar 3.14.4 on Erlang/OTP 22 Erts 10.7.2.1
- redis v4.0 or greater

## Comand 
```shell
## suite test
$ make ct  
sh crates/build_crates.sh clippy
    Finished dev [unoptimized + debuginfo] target(s) in 0.02s
sh crates/build_crates.sh test
    Finished test [unoptimized + debuginfo] target(s) in 0.02s
     Running unittests (crates/big_data/target/debug/deps/big_data-f3da29e6da47f8c3)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/big_data_test.rs (crates/big_data/target/debug/deps/big_data_test-f8d535b5018e2ef3)

running 14 tests
test clear ... ok
test get ... ok
test get_time_index ... ok
test get_row_ids ... ok
test get_range ... ok
test get_range_row_ids ... ok
test insert ... ok
test len_range_row_ids ... ok
test len_row_ids ... ok
test remove ... ok
test to_list ... ok
test remove_row_ids ... ok
test update_counter ... ok
test update_elem ... ok

test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests big_data

running 12 tests
test src/big_data.rs - big_data::BigData::get (line 387) ... ok
test src/big_data.rs - big_data::BigData::insert (line 90) ... ok
test src/big_data.rs - big_data::BigData::len_range_row_ids (line 192) ... ok
test src/big_data.rs - big_data::BigData::get_time_index (line 405) ... ok
test src/big_data.rs - big_data::BigData::get_range (line 464) ... ok
test src/big_data.rs - big_data::BigData::clear (line 129) ... ok
test src/big_data.rs - big_data::BigData::get_range_row_ids (line 491) ... ok
test src/big_data.rs - big_data::BigData::get_row_ids (line 429) ... ok
test src/big_data.rs - big_data::BigData::len_row_ids (line 165) ... ok
test src/big_data.rs - big_data::BigData::remove (line 556) ... ok
test src/big_data.rs - big_data::BigData::remove_row_ids (line 591) ... ok
test src/big_data.rs - big_data::BigData::to_list (line 526) ... ok

test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.51s

./rebar3 do ct --dir test/ct -v --config test/ct/ct.config --sys_config config/test.config
===> Verifying dependencies...
    Finished release [optimized] target(s) in 0.02s
===> Analyzing applications...
===> Compiling big_data_nif
===> Running Common Test suites...

Common Test starting (cwd is /Users/admin/proj/rust/big_data_nif)



CWD set to: "/Users/admin/proj/rust/big_data_nif/_build/test/logs/ct_run.nonode@nohost.2021-05-13_15.54.32"

TEST INFO: 1 test(s), 11 case(s) in 1 suite(s)

Testing test.ct: Starting test, 11 test cases
%%% big_data_SUITE: ...........
Testing test.ct: TEST COMPLETE, 11 ok, 0 failed of 11 test cases

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
## Redis Example(BigData)

### Redis Command
```rust
        ["big_data.set", big_data_set, "write", 1, 1, 1],
        ["big_data.update_elem", big_data_update_elem, "write", 1, 1, 1],
        ["big_data.update_counter", big_data_update_counter, "write", 1, 1, 1],
        ["big_data.remove", big_data_remove, "write", 1, 1, 1],
        ["big_data.remove_row", big_data_remove_row, "write", 1, 1, 1],
        ["big_data.remove_row_ids", big_data_remove_row_ids, "write", 1, 1, 1],
        ["big_data.get_row", big_data_get_row, "readonly", 1, 1, 1],
        ["big_data.get", big_data_get, "readonly", 1, 1, 1],
        ["big_data.get_range", big_data_get_range, "readonly", 1, 1, 1],
        ["big_data.get_range_row_ids", big_data_get_range_row_ids, "readonly", 1, 1, 1],
        ["big_data.get_row_ids", big_data_get_row_ids, "readonly", 1, 1, 1],
        ["big_data.get_time_index", big_data_get_time_index, "readonly", 1, 1, 1],
        ["big_data.lookup_elem", big_data_lookup_elem, "readonly", 1, 1, 1],

```
### Erlang Command(big_data_redis)
```erlang
158> {ok,P} = big_data_redis:new().
{ok,<0.410.0>}
159> big_data_redis:insert(P,<<"a">>,<<"rowid1">>,erlang:system_time(1),{a,8,<<"a">>,"d",[{a,b}]}).
ok
160> big_data_redis:get_row(P,<<"a">>,<<"rowid1">>).
[{row_data,<<"rowid1">>,
           {a,8,<<"a">>,"d",[{a,b}]},
           1641522837}]
161> big_data_redis:get(P,<<"a">>).
[
 {row_data,<<"rowid1">>,
           {a,8,<<"a">>,"d",[{a,b}]},
           1641522837}]
163> big_data_redis:update_elem(P,<<"a">>,<<"rowid1">>,{0,{1,a,<<"a">>}}).
[true]
164> big_data_redis:get_row(P,<<"a">>,<<"rowid1">>).
[{row_data,<<"rowid1">>,
           {{1,a,<<"a">>},8,<<"a">>,"d",[{a,b}]},
           1641522837}]
166> big_data_redis:update_counter(P,<<"a">>,<<"rowid1">>,[{1,1}]).
[true]
167> big_data_redis:get_row(P,<<"a">>,<<"rowid1">>).
[{row_data,<<"rowid1">>,
           {{1,a,<<"a">>},9,<<"a">>,"d",[{a,b}]},
           1641522837}]
168> big_data_redis:update_counter(P,<<"a">>,<<"rowid1">>,[{1,10}]).
[true]
169> big_data_redis:get_row(P,<<"a">>,<<"rowid1">>).
[{row_data,<<"rowid1">>,
           {{1,a,<<"a">>},19,<<"a">>,"d",[{a,b}]},
           1641522837}]
170> big_data_redis:lookup_elem(P,<<"a">>,<<"rowid1">>,{0,1}).
{{1,a,<<"a">>},19}
171> big_data_redis:lookup_elem(P,<<"a">>,<<"rowid1">>,{0}).
{{1,a,<<"a">>}}
172> big_data_redis:lookup_elem(P,<<"a">>,<<"rowid1">>,{10}).
{}
174> big_data_redis:lookup_elem(P,<<"a">>,<<"rowid1">>,[4]).
{[{a,b}]}
```

## Nif Example
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

## Bench
### cargo bench
```shell
$ sh crates/build_crates.sh bench
    Finished bench [optimized] target(s) in 0.02s
     Running unittests (crates/big_data/target/release/deps/big_data-a47418a78ef79a70)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests (crates/big_data/target/release/deps/bench-2cb028b9ea72486c)

running 2 tests
test get    ... bench:         216 ns/iter (+/- 133)
test insert ... bench:       1,160 ns/iter (+/- 141)

test result: ok. 0 passed; 0 failed; 0 ignored; 2 measured
```
### FlameGraph

#### MacOS start dtrace to set probe
```shell
$ ./flamegraph.sh
dtrace: system integrity protection is on, some features will not be available

dtrace: description 'profile-997 ' matched 1 probe
```
#### Start redis-server
First of all build redis_api `make redis_api` then output library to `target/debug/libredis_api.dylib` or 
`target/release/libredis_api.dylib`. The below command to load redis module
```shell
$ redis-server --loadmodule target/debug/libredis_api.dylib
42527:C 11 Jan 2022 17:24:07.942 # oO0OoO0OoO0Oo Redis is starting oO0OoO0OoO0Oo
42527:C 11 Jan 2022 17:24:07.942 # Redis version=6.2.6, bits=64, commit=00000000, modified=0, pid=42527, just started
42527:C 11 Jan 2022 17:24:07.942 # Configuration loaded
42527:M 11 Jan 2022 17:24:07.943 * Increased maximum number of open files to 10032 (it was originally set to 256).
42527:M 11 Jan 2022 17:24:07.943 * monotonic clock: POSIX clock_gettime
                _._
           _.-``__ ''-._
      _.-``    `.  `_.  ''-._           Redis 6.2.6 (00000000/0) 64 bit
  .-`` .-```.  ```\/    _.,_ ''-._
 (    '      ,       .-`  | `,    )     Running in standalone mode
 |`-._`-...-` __...-.``-._|'` _.-'|     Port: 6379
 |    `-._   `._    /     _.-'    |     PID: 42527
  `-._    `-._  `-./  _.-'    _.-'
 |`-._`-._    `-.__.-'    _.-'_.-'|
 |    `-._`-._        _.-'_.-'    |           https://redis.io
  `-._    `-._`-.__.-'_.-'    _.-'
 |`-._`-._    `-.__.-'    _.-'_.-'|
 |    `-._`-._        _.-'_.-'    |
  `-._    `-._`-.__.-'_.-'    _.-'
      `-._    `-.__.-'    _.-'
          `-._        _.-'
              `-.__.-'

42527:M 11 Jan 2022 17:24:07.944 # Server initialized
42527:M 11 Jan 2022 17:24:07.946 * <big_data> Created new data type 'big_data1'
42527:M 11 Jan 2022 17:24:07.946 * Module 'big_data' loaded from target/debug/libredis_api.dylib
```
#### Start big_data erlang client
```shell
$ make shell
./rebar3 as test shell
===> Verifying dependencies...
/Users/admin/proj/erlang/big_data/target
   Compiling big_data v0.1.2 (/Users/admin/proj/erlang/big_data/crates/big_data)
    Finished release [optimized] target(s) in 2.70s
===> Analyzing applications...
===> Compiling big_data
Erlang/OTP 22 [erts-10.7.2.1] [source] [64-bit] [smp:8:8] [ds:8:8:10] [async-threads:1] [hipe]

Eshell V10.7.2.1  (abort with ^G)
1> =INFO REPORT==== 11-Jan-2022::17:26:46.165809 ===
Setup running ...
=INFO REPORT==== 11-Jan-2022::17:26:46.175540 ===
Directories verified. Res = ok
=INFO REPORT==== 11-Jan-2022::17:26:46.175985 ===
Setup finished processing hooks (Mode=normal)...
=DEBUG REPORT==== 11-Jan-2022::17:26:46.641428 ===
Waiting wal recover...
=DEBUG REPORT==== 11-Jan-2022::17:26:46.641624 ===
Recover: ..#{dir => "data",waiting_pid => <0.393.0>}
=DEBUG REPORT==== 11-Jan-2022::17:26:46.648339 ===
CheckpointSeq = 0, id_seq = 0, walfiles = ["data/1_00000001.wal",
                                           "data/1_00000002.wal"]
=DEBUG REPORT==== 11-Jan-2022::17:26:46.648581 ===
Checkpoint sync timeout:600000
=DEBUG REPORT==== 11-Jan-2022::17:26:46.648733 ===
Finish checkpoint id_seq = 0, checkpointseq = 0
=DEBUG REPORT==== 11-Jan-2022::17:26:46.660010 ===
Notify recover finished, Pid = <0.393.0>
=DEBUG REPORT==== 11-Jan-2022::17:26:46.660359 ===
Wal recover finished, Recover total_time = 19 ms
===> Booted recon
===> Booted observer_cli
===> Booted hut
===> Booted setup
===> Booted bear
===> Booted folsom
===> Booted syntax_tools
===> Booted parse_trans
===> Booted exometer_core
===> Booted pa
===> Booted quickrand
===> Booted uuid
===> Booted xmerl
===> Booted jiffy
===> Booted flatlog
===> Booted cool_tools
===> Booted big_data
1>bd_bench:run(500,500).
......
Bech done pid = <0.857.0>
Overview = #{aver => 20,sum_time => 1488897303,total_command => 72868,
             total_time => 4684,tps => 18217}
```
![FlameGraph](pretty-graph.svg)

## Reference
* [redismodule-rs](https://github.com/RedisLabsModules/redismodule-rs.git)
* [rabbitmq/ra](https://github.com/rabbitmq/ra.git)