-module(big_data_nif).

%% API
-export([new/0, new/1, insert/3, insert_new/3, get_row/3, get/2, big_key_list/1,
         get_range/4, get_range_row_ids/4, get_row_ids/3, get_time_index/3, lookup_elem/4, clear/1,
         remove/2, remove_row/3, remove_row_ids/4, update_elem/4, update_counter/4]).
%% Native library support
-export([load/0]).

-on_load load/0.

-include("big_data.hrl").

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

-spec insert_new(Ref :: big_data(), BigKey :: big_key(), row_data_list()) -> ok.
insert_new(_Ref, _BigKey, _RowDataList) ->
    not_loaded(?LINE).

-spec update_elem(Ref :: big_data(),
                  BigKey :: big_key(),
                  RowID :: binary(),
                  ElemSpecs :: elem_specs()) ->
                     [boolean()] | ?BD_NOTFOUND.
update_elem(_Ref, _BigKey, _RowID, _ElemSpecs) ->
    not_loaded(?LINE).

-spec update_counter(Ref :: big_data(),
                     BigKey :: big_key(),
                     RowID :: binary(),
                     ElemSpecs :: elem_specs()) ->
                        [boolean()] | ?BD_NOTFOUND.
update_counter(_Ref, _BigKey, _RowID, _ElemSpecs) ->
    not_loaded(?LINE).

-spec get_row(Ref :: big_data(), BigKey :: big_key(), RowID :: row_id()) ->
                 row_data() | nil.
get_row(_Ref, _BigKey, _RowID) ->
    not_loaded(?LINE).

-spec get(Ref :: big_data(), BigKey :: big_key()) -> row_data_list() | ?BD_NOTFOUND.
get(_Ref, _BigKey) ->
    not_loaded(?LINE).

big_key_list(_Ref) ->
    not_loaded(?LINE).

-spec get_range(Ref :: big_data(),
                BigKey :: binary(),
                StartTime :: t(),
                EndTime :: t()) ->
                   row_data_list() | ?BD_NOTFOUND.
get_range(_Ref, _BigKey, _StartTime, _EndTime) ->
    not_loaded(?LINE).

-spec get_range_row_ids(Ref :: big_data(),
                        BigKey :: big_key(),
                        StartTime :: t(),
                        EndTime :: t()) ->
                           row_id_list() | ?BD_NOTFOUND.
get_range_row_ids(_Ref, _BigKey, _StartTime, _EndTime) ->
    not_loaded(?LINE).

-spec get_row_ids(Ref :: big_data(), BigKey :: big_key(), Time :: t()) ->
                     row_id_list() | ?BD_NOTFOUND.
get_row_ids(_Ref, _BigKey, _Time) ->
    not_loaded(?LINE).

-spec get_time_index(Ref :: big_data(), BigKey :: big_key(), RowID :: row_id()) ->
                        t() | ?BD_NOTFOUND.
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

-spec remove_row_ids(Ref :: big_data(),
                     BigKey :: big_key(),
                     StartTime :: t(),
                     EndTime :: t()) ->
                        ok.
remove_row_ids(_Ref, _BigKey, _StartTime, _EndTime) ->
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
