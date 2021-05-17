%%%-------------------------------------------------------------------
%% @doc big_data_nif top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(big_data_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).
-define(TABLES, [bd_metrics, bd_open_file_metrics, bd_io_metrics]).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    _ = [ets:new(Table, [named_table, public, {write_concurrency, true}])
         || Table <- ?TABLES],
    {ok, Ref} = big_data_nif:new(),
    ok = persistent_term:put(big_data, Ref),
    SupFlags =
        #{strategy => one_for_one,
          intensity => 1,
          period => 5},
    ChildSpecs =
        [#{id => bd_file_handle, start => {bd_file_handle, start_link, []}},
         #{id => bd_log_wal_sup,
           type => supervisor,
           start => {bd_log_wal_sup, start_link, []}}],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
