-module(dive).

-export([start/0]).
-export([
   new/1
  ,new/2
  ,free/1
  ,fd/1
  %% common
  ,put/3
  ,put_/3
  ,get/2
  ,remove/2
  ,remove_/2
  ,ttl/1
  %% data structure
  ,append/3
  ,append_/3
  ,head/2
  ,take/2

  % ,stream/1
  % ,stream/2
]).

%%
-type(fd()  :: binary()).
-type(key() :: binary()).
-type(val() :: binary() | list()).

-define(is_fd(X), is_binary(X)).

%%
%%
start() ->
   applib:boot(?MODULE, []).

%%
%% create new database instance
-spec(new/1 :: (list()) -> {ok, pid()} | {error, any()}).
-spec(new/2 :: (atom(), list()) -> {ok, pid()} | {error, any()}).

new(Opts) ->
   new(undefined, Opts).

new(Name, Opts) ->
   File = opts:val(file, Opts),
   case pns:whereis(dive, File) of
      undefined ->
         supervisor:start_child(dive_db_sup, [Name, [{owner, self()}|Opts]]);
      Pid       ->
         ok = pipe:call(Pid, {init, self()}),
         {ok, Pid}
   end.

%%
%% release database instance
-spec(free/1 :: (pid()) -> ok).

free(Pid) ->
   pipe:call(Pid, {free, self()}).

%%
%% get file descriptor 
-spec(fd/1 :: (pid()) -> fd()).

fd(Pid) ->
   pipe:ioctl(Pid, fd).

%%%----------------------------------------------------------------------------   
%%%
%%% common
%%%
%%%----------------------------------------------------------------------------   

%%
%% synchronous put key / val to storage
-spec(put/3 :: (fd(), key(), val()) -> ok | {error, any()}).

put(Fd, Key, Val) ->
   ok = cache:put(dive_cache, Key, Val),
   eleveldb:put(Fd, Key, encode(Val), [{sync, true}]).

%%
%% asynchronous put key / val to storage
-spec(put_/3 :: (fd(), key(), val()) -> ok | {error, any()}).

put_(Fd, Key, Val) ->
   ok = cache:put_(dive_cache, Key, Val),
   eleveldb:put(Fd, Key, encode(Val), [{sync, false}]).

%%
%% synchronous get key / val from storage
-spec(get/2 :: (fd(), key()) -> {ok, val()} | {error, any()}).

get(Fd, Key) ->
   case cache:get(dive_cache, Key) of
      undefined ->
         case eleveldb:get(Fd, Key, []) of
            {ok, Val} ->
               Value = decode(Val),
               cache:put_(dive_cache, Key, Value),
               {ok, Value};
            not_found ->
               {error, not_found};
            {error,_} = Error ->
               Error
         end;
      Val ->
         {ok, Val}
   end.

%%
%% return key ttl
-spec(ttl/1 :: (key()) -> integer() | false).

ttl(Key) ->
   cache:ttl(Key).

%%
%% synchronous remove key from dataset
-spec(remove/2 :: (fd(), key()) -> ok | {error, any()}).

remove(Fd, Key) ->
   _ = cache:remove(dive_cache, Key),
   eleveldb:delete(Fd, Key, [{sync, true}]).

%%
%% asynchronous remove key from dataset
-spec(remove_/2 :: (fd(), key()) -> ok | {error, any()}).

remove_(Fd, Key) ->
   _ = cache:remove(dive_cache, Key),
   eleveldb:delete(Fd, Key, [{sync, false}]).

%%%----------------------------------------------------------------------------   
%%%
%%% data structure
%%%
%%%----------------------------------------------------------------------------   

%%
%% synchronous append element to data structure
-spec(append/3 :: (fd(), key(), val()) -> {ok, any()} | {error, any()}).

append(Fd, Key, Val) ->
   case dive:get(Fd, Key) of
      {ok, List} when is_list(List) ->
         NList = [Val | List],
         ok    = dive:put(Fd, Key, NList),
         {ok, NList}; 
      {error, not_found} ->
         ok    = dive:put(Fd, Key, [Val]),
         {ok, [Val]}; 
      {error, _} = Error ->
         Error
   end.

%%
%% asynchronous append element to data structure
-spec(append_/3 :: (fd(), key(), val()) -> {ok, any()} | {error, any()}).

append_(Fd, Key, Val) ->
   case dive:get(Fd, Key) of
      {ok, List} when is_list(List) ->
         NList = [Val | List],
         ok    = dive:put_(Fd, Key, NList),
         {ok, NList}; 
      {error, not_found} ->
         ok    = dive:put_(Fd, Key, [Val]),
         {ok, [Val]}; 
      {error, _} = Error ->
         Error
   end.

%%
%% return head element
-spec(head/2 :: (fd(), key()) -> {ok, any()} | {error, any()}).

head(Fd, Key) ->
   case dive:get(Fd, Key) of
      {ok, [Head|_]} ->
         {ok, Head};
      {ok, _} ->
         {error, not_found};
      {error, _} = Error ->
         Error
   end.

%%
%% take head element
-spec(take/2 :: (fd(), key()) -> {ok, any()} | {error, any()}).

take(Fd, Key) ->
   case dive:get(Fd, Key) of
      {ok, [Head|Tail]} ->
         ok    = dive:put(Fd, Key, Tail),
         {ok, Head};
      {ok, _} ->
         {error, not_found};
      {error, _} = Error ->
         Error
   end.


% %%
% %%  lookup key, the query is expressed as
% %%
% %%  binary()       - key lookup (get)
% %%  {prefix,  Key} - key prefix lookup
% %%  {KeyA,   KeyB} - key range lookup
% %%  {Key,       N} - key batch lookup
% lookup(Fd, Query) ->
%    ok.
%    % stream:list(stream(Fd, Query)).

% %% @todo: -> {first, KeyB} {KeyA, last}


% %%
% %% create bucket iterator and package it to the stream.
% %% the stream resources are garbage collected when it 
% %% reaches eof or owner process dies.
% stream(Fd) ->
%    stream(Fd, undefined).

% stream(Fd, Req)
%  when ?is_fd(Fd) ->
%    kvs_stream:new(Fd, Req, []);

% stream(Pid, Req)
%  when is_pid(Pid) ->
%    stream(fd(Pid), Req).


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
encode(X)
 when is_binary(X) ->
   X;
encode(X) ->
   erlang:term_to_binary(X).

%%
%%
decode(<<131, _/binary>>=X) ->
   try
      erlang:binary_to_term(X)
   catch _:_ ->
      X
   end;
decode(X) ->
   X.





