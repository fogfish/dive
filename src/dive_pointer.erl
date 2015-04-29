%%
%%   Copyright (c) 2012 - 2014, Dmitry Kolesnikov
%%   All Rights Reserved.
%%
%%   Licensed under the Apache License, Version 2.0 (the "License");
%%   you may not use this file except in compliance with the License.
%%   You may obtain a copy of the License at
%%
%%       http://www.apache.org/licenses/LICENSE-2.0
%%
%%   Unless required by applicable law or agreed to in writing, software
%%   distributed under the License is distributed on an "AS IS" BASIS,
%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%   See the License for the specific language governing permissions and
%%   limitations under the License.
%%
%% @description
%%
-module(dive_pointer).
-include("dive.hrl").

-export([
   new/2
  ,free/1
  ,move/2
  ,next/1
  ,prev/1
]).

-record(ptr, {
   type = undefined
  ,fd   = undefined :: any()
  ,key  = undefined :: any()
  ,val  = undefined :: any()
}).

%% eleveldb allowed options
-define(ALLOWED, [verify_checksums, fill_cache, iterator_refresh]).

%%
%%
new(#dd{type = ephemeral,  fd = FD}, _Opts) ->
   #ptr{type = ephemeral,  fd = FD};

new(#dd{type = persistent, fd = FD}, Opts) ->
   {ok, I} = case opts:val(keys_only, undefined, Opts) of
      undefined -> 
         eleveldb:iterator(FD, opts:filter(?ALLOWED, Opts));
      _ ->
         eleveldb:iterator(FD, opts:filter(?ALLOWED, Opts), keys_only)
   end,
   #ptr{type = persistent, fd = I}.

%%
%%
free(#ptr{type = ephemeral}) ->
   ok;
free(#ptr{type = persistent, fd = FD}) ->
   (catch eleveldb:iterator_close(FD)),
   ok.


%%
%%
move(first, #ptr{type = ephemeral, fd = FD}=State) ->
   case ets:lookup(FD, ets:first(FD)) of
      [{Key, Val}] ->
         State#ptr{key = Key, val=Val};
      [] ->
         State#ptr{key = eof}
   end;
   
move(first, #ptr{type = persistent, fd = FD}=State) ->
   case eleveldb:iterator_move(FD, first) of
      {ok, Key, Val} ->
         State#ptr{key = Key, val = Val};
      {ok,  Key} ->
         State#ptr{key = Key};
      {error,invalid_iterator} ->
         State#ptr{key = eof}
   end;

move(Key0, #ptr{type = ephemeral, fd = FD}=State) ->
   case ets:lookup(FD, Key0) of
      [{Key1, Val1}] ->
         State#ptr{key = Key1, val=Val1};
      [] ->
         case ets:next(FD, Key0) of
            '$end_of_table' ->
               State#ptr{key = eof};
            Key1 ->
               [{Key1, Val1}] = ets:lookup(FD, Key1),
               State#ptr{key = Key1, val=Val1}
         end
   end;

move(Key0, #ptr{type = persistent, fd = FD}=State) ->
   case eleveldb:iterator_move(FD, Key0) of
      {ok, Key1, Val1} ->
         State#ptr{key = Key1, val = Val1};
      {ok, Key1} ->
         State#ptr{key = Key1};
      {error,invalid_iterator} ->
         State#ptr{key = eof}
   end.
   
%%
%%
next(#ptr{key = eof}=State) ->
   {eof, State};
next(#ptr{type = ephemeral, fd = FD, key = Key0, val = Val0}=State) ->
   case ets:next(FD, Key0) of
      '$end_of_table' ->
         {{Key0, Val0}, State#ptr{key = eof}};
      Key1 ->
         [{Key1, Val1}] = ets:lookup(FD, Key1),
         {{Key0, Val0}, State#ptr{key = Key1, val=Val1}}
   end;

next(#ptr{type = persistent, fd = FD, key = Key0, val = Val0}=State) ->
   case eleveldb:iterator_move(FD, prefetch) of
      {ok, Key1, Val1} ->
         {{Key0, Val0}, State#ptr{key = Key1, val = Val1}};
      {ok, Key1} ->
         {Key0, State#ptr{key = Key1}};
      {error, _} ->
         {{Key0, Val0}, State#ptr{key = eof}}
   end.

%%
%%
prev(#ptr{key = eof}=State) ->
   {eof, State};
prev(#ptr{type = ephemeral, fd = FD, key = Key0, val = Val0}=State) ->
   case ets:prev(FD, Key0) of
      '$end_of_table' ->
         {{Key0, Val0}, State#ptr{key = eof}};
      Key1 ->
         [{Key1, Val1}] = ets:lookup(FD, Key1),
         {{Key0, Val0}, State#ptr{key = Key1, val=Val1}}
   end;

prev(#ptr{type = persistent, fd = FD, key = Key0, val = Val0}=State) ->
   case eleveldb:iterator_move(FD, prev) of
      {ok, Key1, Val1} ->
         {{Key0, Val0}, State#ptr{key = Key1, val = Val1}};
      {ok, Key1} ->
         {Key0, State#ptr{key = Key1}};
      {error, _} ->
         {{Key0, Val0}, State#ptr{key = eof}}
   end.
