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

-behaviour(supervisor).

%% API
-export([start_link/0]).
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

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @private
%% @doc Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
-spec init(Args :: term()) ->
              {ok,
               {SupFlags ::
                    {RestartStrategy :: supervisor:strategy(),
                     MaxR :: non_neg_integer(),
                     MaxT :: non_neg_integer()},
                [ChildSpec :: supervisor:child_spec()]}} |
              ignore |
              {error, Reason :: term()}.
init([]) ->
    MaxRestarts = 1,
    MaxSecondsBetweenRestarts = 5,
    SupFlags =
        #{strategy => one_for_one,
          intensity => MaxRestarts,
          period => MaxSecondsBetweenRestarts},
    Dir = application:get_env(big_data, dir, "data"),
    AChild =
        #{id => bd_log_wal,
          start => {bd_log_wal, start_link, [#{dir => Dir}]},
          restart => permanent,
          shutdown => 2000,
          type => worker,
          modules => [bd_log_wal]},

    {ok, {SupFlags, [AChild]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
