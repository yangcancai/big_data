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
%%% Created : 2021-05-17T02:55:43+00:00
%%%-------------------------------------------------------------------
-module(big_data).

-author("yangcancai").

-include("big_data.hrl").

-export([insert/5, update_elem/4]).

-spec insert(Ref :: big_data(),
             BigKey :: big_key(),
             RowID :: row_id(),
             Time :: t(),
             Term :: term()) ->
                ok.
insert(Ref, BigKey, RowID, Time, Term) ->
    big_data_nif:insert(Ref,
                        BigKey,
                        #row_data{row_id = RowID,
                                  time = Time,
                                  term = Term}).

update_elem(Ref, BigKey, RowID, ElemSpecs) ->
    case big_data_nif:update_elem(Ref, BigKey, RowID, ElemSpecs) of
        ?BD_NOTFOUND ->
            %% import from db
            ok;
        ok ->
            ok
    end.
