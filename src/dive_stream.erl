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
-module(dive_stream).
-include("dive.hrl").

-export([
   new/3
]).

% %% internal state
% -record(stream, {     
%    fd      = undefined :: any()       %% file descriptor
%   ,cursor  = undefined :: any()       %% iterator descriptor
%   ,pattern = undefined :: any()       %% 
% }).

%% eleveldb allowed options
-define(ALLOWED, [verify_checksums, fill_cache, iterator_refresh]).

%%
%% 
new('_', FD, Opts) ->
   next_element(dive_pointer:move(first, dive_pointer:new(FD, Opts)));

new({'~', Key0}, FD, Opts) ->
   Len = byte_size(Key0),
   next_element(
      fun({Key1, _}) ->
         case Key1 of
            <<Key0:Len/binary, _/binary>> -> 
               true;
            _ ->
               false
         end
      end,
      dive_pointer:move(Key0, dive_pointer:new(FD, Opts))
   );

new({'=', Key0}, FD, Opts) ->
   next_element(
      fun({Key1, _}) -> Key1 =:= Key0 end,
      dive_pointer:move(Key0, dive_pointer:new(FD, Opts))
   );

new({'>=', Key0}, FD, Opts) ->
   next_element(dive_pointer:move(Key0, dive_pointer:new(FD, Opts)));

new({'=<', Key0}, FD, Opts) ->
   prev_element(dive_pointer:move(Key0, dive_pointer:new(FD, Opts)));

new({'>',  Key0}, FD, Opts) ->
   stream:dropwhile(
      fun({Key1, _}) -> Key1 =:= Key0 end,
      next_element(dive_pointer:move(Key0, dive_pointer:new(FD, Opts)))
   );

new({'<',  Key0}, FD, Opts) ->
   stream:dropwhile(
      fun({Key1, _}) -> Key1 =:= Key0 end,
      prev_element(dive_pointer:move(Key0, dive_pointer:new(FD, Opts)))
   ).

%%
%%
next_element(Ptr0) ->
   try
      case dive_pointer:next(Ptr0) of
         {eof, _} ->
            stream:new();
         {Value, Ptr1} ->
            stream:new(Value, fun() -> next_element(Ptr1) end)
      end
   catch _:_Reason ->
      dive_pointer:free(Ptr0),
      stream:new()
   end.

next_element(Pred, Ptr0) ->
   try
      case dive_pointer:next(Ptr0) of
         {eof, _} ->
            stream:new();
         {Value, Ptr1} ->
            true = Pred(Value),
            stream:new(Value, fun() -> next_element(Pred, Ptr1) end)
      end
   catch _:_Reason ->
      dive_pointer:free(Ptr0),
      stream:new()
   end.

%%
%%
prev_element(Ptr0) ->
   try
      case dive_pointer:prev(Ptr0) of
         {eof, _} ->
            stream:new();
         {Value, Ptr1} ->
            stream:new(Value, fun() -> prev_element(Ptr1) end)
      end
   catch _:_Reason ->
      dive_pointer:free(Ptr0),
      stream:new()
   end.

