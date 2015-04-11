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
%%  @todo: encode / decode stream data types
-module(dive_stream).

-export([
   new/3
]).

%% internal state
-record(stream, {     
   fd      = undefined :: any()       %% file descriptor
  ,io      = undefined :: any()       %% file iterator
  ,pattern = undefined :: any()       %% 
  ,read    = undefined :: any()       %%
}).

%% eleveldb allowed options
-define(ALLOWED, [verify_checksums, fill_cache, iterator_refresh]).

%%
%% create stream
new(FD, Pattern, Opts) ->
   {ok, IO} = create_iterator(FD, Opts),
   next_element(#stream{fd=FD, io=IO, pattern=Pattern}).

%%
%% create iterator
create_iterator(FD, Opts) ->
   case opts:val(keys_only, undefined, Opts) of
      undefined -> 
         eleveldb:iterator(FD, opts:filter(?ALLOWED, Opts));
      _ ->
         eleveldb:iterator(FD, opts:filter(?ALLOWED, Opts), keys_only)
   end.

%%
%%
next_element(S) ->
   try
      next_element_unsafe(S)
   catch _:_Reason ->
      (catch eleveldb:iterator_close(S#stream.io)),
      stream:new()
   end.

%%
%% full scan
next_element_unsafe(#stream{pattern='_', read=undefined}=State) ->
   next_element_unsafe(State#stream{read=first});

next_element_unsafe(#stream{pattern='_'}=State) ->
   stream:new(
      read_element(State), 
      fun() -> next_element(State#stream{read=prefetch}) end
   );

%%
%% partial match / prefix scan
next_element_unsafe(#stream{pattern={'~', _Len, Key}, read=undefined}=State) ->
   next_element_unsafe(State#stream{read=Key});

next_element_unsafe(#stream{pattern={'~', Len, Key}}=State) ->
   case read_element(State) of
      {<<Key:Len/binary, _/binary>>, _} = Head ->
         stream:new(Head, fun() -> next_element(State#stream{read=prefetch}) end);
      <<Key:Len/binary, _/binary>> = Head ->
         stream:new(Head, fun() -> next_element(State#stream{read=prefetch}) end);
      _ ->
         eleveldb:iterator_close(State#stream.io),
         stream:new()
   end;

%%
%% full match / key scan
next_element_unsafe(#stream{pattern={'=', _Len, Key}, read=undefined}=State) ->
   next_element_unsafe(State#stream{read=Key});

next_element_unsafe(#stream{pattern={'=', _Len, Key}}=State) ->
   case read_element(State) of
      {Key, _} = Head ->
         stream:new(Head, fun() -> next_element(State#stream{read=prefetch}) end);
      Key  when is_binary(Key) ->
         stream:new(Key,  fun() -> next_element(State#stream{read=prefetch}) end);
      _   ->
         eleveldb:iterator_close(State#stream.io),
         stream:new()
   end;

%%
%% 
next_element_unsafe(#stream{pattern={'>=', _Len, Key}, read=undefined}=State) ->
   next_element_unsafe(State#stream{read=Key});

next_element_unsafe(#stream{pattern={'>=', _Len, _Key}}=State) ->
   case read_element(State) of
      {_, _} = Head ->
         stream:new(Head, fun() -> next_element(State#stream{read=prefetch}) end);
      Head when is_binary(Head) ->
         stream:new(Head,  fun() -> next_element(State#stream{read=prefetch}) end);
      _   ->
         eleveldb:iterator_close(State#stream.io),
         stream:new()
   end;

%%
%%
next_element_unsafe(#stream{pattern={'=<', _Len, Key}, read=undefined}=State) ->
   next_element_unsafe(State#stream{read=Key});

next_element_unsafe(#stream{pattern={'=<', _Len, Key0}}=State) ->
   case read_element(State) of
      {Key, _} = Head when Key =< Key0 ->
         stream:new(Head, fun() -> next_element(State#stream{read=prev}) end);
      Key when is_binary(Key), Key =< Key0 ->
         stream:new(Key,  fun() -> next_element(State#stream{read=prev}) end);
      _   ->
         eleveldb:iterator_close(State#stream.io),
         stream:new()
   end;

%%
%%
next_element_unsafe(#stream{pattern={'>', _Len, Key}, read=undefined}=State) ->
   next_element_unsafe(State#stream{read=Key});

next_element_unsafe(#stream{pattern={'>', _Len, Key}}=State) ->
   case read_element(State) of
      {Key, _} ->
         next_element_unsafe(State);
      Key when is_binary(Key) ->
         next_element_unsafe(State);
      {_, _} = Head ->
         stream:new(Head, fun() -> next_element(State#stream{read=prefetch}) end);
      Head when is_binary(Head) ->
         stream:new(Head,  fun() -> next_element(State#stream{read=prefetch}) end);
      _   ->
         eleveldb:iterator_close(State#stream.io),
         stream:new()
   end;

%%
%%
next_element_unsafe(#stream{pattern={'<', _Len, Key}, read=undefined}=State) ->
   next_element_unsafe(State#stream{read=Key});

next_element_unsafe(#stream{pattern={'<', _Len, Key0}}=State) ->
   case read_element(State) of
      {Key, _} = Head when Key < Key0 ->
         stream:new(Head, fun() -> next_element(State#stream{read=prev}) end);
      Key when is_binary(Key), Key < Key0 ->
         stream:new(Key,  fun() -> next_element(State#stream{read=prev}) end);
      _   ->
         eleveldb:iterator_close(State#stream.io),
         stream:new()
   end.


% %%
% %% range scan
% next_element_unsafe(#stream{req={KeyA, KeyB}, read=undefined}=S)
%  when (is_binary(KeyA) orelse is_atom(KeyA)), is_binary(KeyB) ->
%    next_element_unsafe(S#stream{read=KeyA});

% next_element_unsafe(#stream{req={KeyA, KeyB}}=S)
%  when is_binary(KeyA), is_binary(KeyB), KeyA > KeyB ->
%    case read_element(S) of
%       {Key, _} = Head when Key =< KeyA, Key >= KeyB ->
%          stream:new(Head, fun() -> next_element(S#stream{read=prev}) end);
%       Head when is_binary(Head), Head =< KeyA, Head >= KeyB ->
%          stream:new(Head, fun() -> next_element(S#stream{read=prev}) end);
%       _ ->
%          eleveldb:iterator_close(S#stream.io),
%          stream:new()
%    end;

% next_element_unsafe(#stream{req={KeyA, KeyB}}=S)
%  when is_binary(KeyA), is_binary(KeyB) ->
%    case read_element(S) of
%       {Key, _} = Head when Key >= KeyA, Key =< KeyB ->
%          stream:new(Head, fun() -> next_element(S#stream{read=prefetch}) end);
%       Head when is_binary(Head), Head >= KeyA, Head =< KeyB ->
%          stream:new(Head, fun() -> next_element(S#stream{read=prefetch}) end);
%       _ ->
%          eleveldb:iterator_close(S#stream.io),
%          stream:new()
%    end;

% next_element_unsafe(#stream{req={last, KeyB}}=S)
%  when is_binary(KeyB) ->
%    case read_element(S) of
%       {Key, _} = Head when Key >= KeyB ->
%          stream:new(Head, fun() -> next_element(S#stream{read=prev}) end);
%       Head when is_binary(Head), Head >= KeyB ->
%          stream:new(Head, fun() -> next_element(S#stream{read=prev}) end);
%       _ ->
%          eleveldb:iterator_close(S#stream.io),
%          stream:new()
%    end;

% next_element_unsafe(#stream{req={first, KeyB}}=S)
%  when is_binary(KeyB) ->
%    case read_element(S) of
%       {Key, _} = Head when Key =< KeyB ->
%          stream:new(Head, fun() -> next_element(S#stream{read=prefetch}) end);
%       Head when is_binary(Head), Head =< KeyB ->
%          stream:new(Head, fun() -> next_element(S#stream{read=prefetch}) end);
%       _ ->
%          eleveldb:iterator_close(S#stream.io),
%          stream:new()
%    end;

%%
%% N-scan
% next_element_unsafe(#stream{req={Key, N}, read=undefined}=S)
%  when (is_binary(Key) orelse is_atom(Key)), is_integer(N), N > 0 ->
%    next_element_unsafe(S#stream{req={Key, N - 1}, read=Key});

% next_element_unsafe(#stream{req={Key, N}, read=undefined}=S)
%  when (is_binary(Key) orelse is_atom(Key)), is_integer(N), N < 0 ->
%    next_element_unsafe(S#stream{req={Key, N + 1}, read=Key});

% next_element_unsafe(#stream{req={Key, 0}}=S)
%  when (is_binary(Key) orelse is_atom(Key)) ->
%    Head = read_element(S),
%    stream:new(Head);

% next_element_unsafe(#stream{req={Key, N}}=S)
%  when (is_binary(Key) orelse is_atom(Key)), is_integer(N), N > 0 ->
%    Head = read_element(S),
%    stream:new(Head, fun() -> next_element(S#stream{req={Key, N - 1}, read=prefetch}) end);

% next_element_unsafe(#stream{req={Key, N}}=S)
%  when (is_binary(Key) orelse is_atom(Key)), is_integer(N), N < 0 ->
%    Head = read_element(S),
%    stream:new(Head, fun() -> next_element(S#stream{req={Key, N + 1}, read=prev}) end).

%%
%%
read_element(#stream{}=S) ->
   case eleveldb:iterator_move(S#stream.io, S#stream.read) of
      {ok, Key, Val} ->
         {Key, Val};
      {ok,  Key} ->
         Key
   end.


