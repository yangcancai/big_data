-module(big_data_nif).

%% API
-export([new/0, new/1, insert/3, get_row/3, get/2, get_range/4, get_range_row_ids/4,
         get_row_ids/3, get_time_index/3, lookup_elem/4, clear/1, remove/2, remove_row/3,
         update_elem/4, update_counter/4]).
%% Native library support
-export([load/0]).

-on_load load/0.

-include("big_data_nif.hrl").

-opaque big_data() :: reference().

-type big_key() :: binary().
-type row_id() :: binary().
-type row_id_list() :: [row_id()].
-type row_data() :: #row_data{}.
-type row_data_list() :: [row_data()].
-type t() :: integer().
-type pos() :: integer().
%%  pos from 0
-type elem_spec() :: {pos(), term()} | pos().
-type elem_specs() :: [elem_spec()].

-export_type([big_data/0]).

-spec new() -> {ok, Ref :: big_data()} | {error, Reason :: binary()}.
new() ->
    new(#{}).

-spec new(_Opts :: map()) -> {ok, Ref :: big_data()} | {error, Reason :: binary()}.
new(_Opts) ->
    not_loaded(?LINE).

-spec insert(Ref :: big_data(), BigKey :: big_key(), row_data()) -> ok.
insert(_Ref, _BigKey, #row_data{} = _RowData) ->
    not_loaded(?LINE).

-spec update_elem(Ref :: big_data(),
                  BigKey :: big_key(),
                  RowID :: binary(),
                  ElemSpecs :: elem_specs()) ->
                     [boolean()].
update_elem(_Ref, _BigKey, _RowID, _ElemSpecs) ->
    not_loaded(?LINE).

-spec update_counter(Ref :: big_data(),
                     BigKey :: big_key(),
                     RowID :: binary(),
                     ElemSpecs :: elem_specs()) ->
                        [boolean()].
update_counter(_Ref, _BigKey, _RowID, _ElemSpecs) ->
    not_loaded(?LINE).

-spec get_row(Ref :: big_data(), BigKey :: big_key(), RowID :: row_id()) ->
                 row_data() | notfound.
get_row(_Ref, _BigKey, _RowID) ->
    not_loaded(?LINE).

-spec get(Ref :: big_data(), BigKey :: big_key()) -> row_data_list().
get(_Ref, _BigKey) ->
    not_loaded(?LINE).

-spec get_range(Ref :: big_data(),
                BigKey :: binary(),
                StartTime :: t(),
                EndTime :: t()) ->
                   row_data_list().
get_range(_Ref, _BigKey, _StartTime, _EndTime) ->
    not_loaded(?LINE).

-spec get_range_row_ids(Ref :: big_data(),
                        BigKey :: big_key(),
                        StartTime :: t(),
                        EndTime :: t()) ->
                           row_id_list().
get_range_row_ids(_Ref, _BigKey, _StartTime, _EndTime) ->
    not_loaded(?LINE).

-spec get_row_ids(Ref :: big_data(), BigKey :: big_key(), Time :: t()) -> row_id_list().
get_row_ids(_Ref, _BigKey, _Time) ->
    not_loaded(?LINE).

-spec get_time_index(Ref :: big_data(), BigKey :: big_key(), RowID :: row_id()) ->
                        t() | notfound.
get_time_index(_Ref, _BigKey, _RowID) ->
    not_loaded(?LINE).

-spec lookup_elem(Ref :: big_data(),
                  BigKey :: big_key(),
                  RowID :: row_id(),
                  ElemSpec :: elem_specs()) ->
                     tuple().
lookup_elem(_Ref, _BigKey, _RowID, _ElemSpecs) ->
    not_loaded(?LINE).

-spec clear(Ref :: big_data()) -> ok.
clear(_Ref) ->
    not_loaded(?LINE).

-spec remove(Ref :: big_data(), BigKey :: binary()) -> ok.
remove(_Ref, _BigKey) ->
    not_loaded(?LINE).

-spec remove_row(Ref :: big_data(), BigKey :: binary(), RowID :: binary()) -> ok.
remove_row(_Ref, _BigKey, _RowID) ->
    not_loaded(?LINE).

%% @private
load() ->
    erlang:load_nif(
        filename:join(priv(), "libbig_data"), none).

not_loaded(Line) ->
    erlang:nif_error({error, {not_loaded, [{module, ?MODULE}, {line, Line}]}}).

priv() ->
    case code:priv_dir(?MODULE) of
        {error, _} ->
            EbinDir =
                filename:dirname(
                    code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            filename:join(AppPath, "priv");
        Path ->
            Path
    end.
