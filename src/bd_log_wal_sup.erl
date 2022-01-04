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
%%% Created : 2021-05-14T04:28:34+00:00
%%%-------------------------------------------------------------------

-module(bd_log_wal_sup).

-author("yangcancai").

-include("big_data.hrl").

-behaviour(supervisor).

%% API
-export([start_link/0, start_child/0, stop_child/0]).
%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc Starts the supervisor
-spec start_link() -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec start_child() -> supervisor:startchild_ret().
start_child() ->
    Dir = application:get_env(big_data, dir, "data"),
    WaitingTimeout = application:get_env(big_data, wating_recover_timeout, 60000),
    Child =
        #{id => bd_log_wal,
          start => {bd_log_wal, start_link, [#{dir => Dir, waiting_pid => self()}]},
          restart => permanent,
          shutdown => 2000,
          type => worker,
          modules => [bd_log_wal]},
    S = erlang:system_time(1000),
    ?DEBUG("Waiting wal recover...", []),
    {ok, Pid} = supervisor:start_child(?MODULE, Child),
    receive
        {_, ?BD_RECOVER_FINISHED} ->
            ok
    after WaitingTimeout ->
        ?ERROR("Waiting wal recover timeout: ~p ms", [WaitingTimeout]),
        exit({timeout, waiting_recover_timeout})
    end,
    ?DEBUG("Wal recover finished, Recover total_time = ~p ms",
           [erlang:system_time(1000) - S]),
    {ok, Pid}.

stop_child() ->
    ok = bd_log_wal:stop().    % supervisor:delete_child(?MODULE,

                            % bd_log_wal).%%%===================================================================
                                        %%% Supervisor callbacks
                                        %%%===================================================================

%% @private
%% @doc Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
init([]) ->
    MaxRestarts = 1,
    MaxSecondsBetweenRestarts = 5,
    SupFlags =
        #{strategy => one_for_one,
          intensity => MaxRestarts,
          period => MaxSecondsBetweenRestarts},

    {ok, {SupFlags, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
