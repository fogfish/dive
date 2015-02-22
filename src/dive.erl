-module(dive).
-include("dive.hrl").

-export([start/0]).
-export([
   new/1
  ,new/2
  ,free/1
   %% hashmap interface
  ,put/3
  ,put/4
  ,put_/3
  ,put_/4
  ,get/2
  ,get/3
  ,remove/2
  ,remove/3
  ,remove_/2
  ,remove_/3
   %% memcached-like interface
  ,set/3
  ,set/4
  ,set_/3
  ,set_/4
  ,add/3
  ,add/4
  ,add_/3
  ,add_/4
  ,replace/3
  ,replace/4
  ,replace_/3
  ,replace_/4
  ,append/3
  ,append/4
  ,append_/3
  ,append_/4
  ,prepend/3
  ,prepend/4
  ,prepend_/3
  ,prepend_/4
  ,delete/2
  ,delete/3
  ,delete_/2
  ,delete_/3
   % match
  ,match/2
]).

%%
-type(fd()  :: pid()).
-type(key() :: binary()).
-type(val() :: binary() | list()).

-define(is_fd(X), is_binary(X)).

%%
%%
start() ->
   applib:boot(?MODULE, []).


%%
%% create new database instance
-spec(new/1 :: (list()) -> {ok, fd()} | {error, any()}).
-spec(new/2 :: (atom(), list()) -> {ok, fd()} | {error, any()}).

new(Opts) ->
   make(Opts).

new(Name, Opts) ->
   try
      {ok, Ref} = new(Opts),
      ok = pns:register(dive, Name, Ref),
      {ok, Ref}
   catch _:{batch, {error,Reason}} ->
      {error, Reason}
   end. 

make(Opts) ->
   try
      File = opts:val(file, Opts),
      {ok, Pid} = case pns:whereis(dive, File) of
         undefined ->
            supervisor:start_child(dive_db_sup, [Opts]);
         X         ->
            {ok, X}
      end,
      pipe:call(Pid, {init, self()}),
      {ok, Fd} = pipe:call(Pid, fd, ?CONFIG_TIMEOUT),
      {ok, #dd{fd = Fd, pid = Pid}}
   catch _:{batch, {error,Reason}} ->
      {error, Reason}
   end. 

%%
%% release database instance
-spec(free/1 :: (pid()) -> ok).

free(Pid)
 when is_pid(Pid) ->
   pipe:call(Pid, {free, self()});

free(Uid) ->
   free(which(Uid)).


%%%----------------------------------------------------------------------------   
%%%
%%% hashmap-like interface
%%%
%%%----------------------------------------------------------------------------   

%%
%% synchronous storage put
-spec(put/3 :: (fd(), key(), val()) -> ok | {error, any()}).
-spec(put/4 :: (fd(), key(), val(), timeout()) -> ok | {error, any()}).

put(#dd{fd = Fd}, Key, Val) ->
   eleveldb:put(Fd, Key, Val, [{sync, true}]).

% put(Pid, Key, Val) ->
%    put(Pid, Key, Val, ?CONFIG_TIMEOUT).

put(#dd{pid = Pid}, Key, Val, Timeout)
 when is_binary(Key), is_binary(Val) ->
   request(Pid, {put, Key, Val, true}, Timeout).

%%
%% asynchronous storage put
-spec(put_/3 :: (fd(), key(), val()) -> ok | reference()).
-spec(put_/4 :: (fd(), key(), val(), true | false) -> ok | reference()).

put_(#dd{pid = Pid}, Key, Val) ->
   put_(Pid, Key, Val, true).

put_(#dd{pid = Pid}, Key, Val, Flag)
 when is_binary(Key), is_binary(Val) ->
   request(Pid, {put, Key, Val, false}, Flag).

%%
%% synchronous get entity from storage
-spec(get/2 :: (fd(), key()) -> {ok, val()} | {error, any()}).
-spec(get/3 :: (fd(), key(), timeout()) -> {ok, val()} | {error, any()}).

get(#dd{fd = Fd}, Key) ->
   case eleveldb:get(Fd, Key, []) of
      {ok, Val} ->
         {ok, Val};
      not_found ->
         {error, not_found};
      {error,_} = Error ->
         Error
   end.

get(#dd{pid = Pid}, Key, Timeout)
 when is_binary(Key) ->
   request(Pid, {get, Key}, Timeout).

%%
%% synchronous remove entity from storage
-spec(remove/2 :: (fd(), key()) -> ok | {error, any()}).
-spec(remove/3 :: (fd(), key(), timeout()) -> ok | {error, any()}).

remove(Pid, Key) ->
   remove(Pid, Key, ?CONFIG_TIMEOUT).

remove(Pid, Key, Timeout)
 when is_binary(Key) ->
   request(Pid, {remove, Key, true}, Timeout).

%%
%% asynchronous remove key from dataset
-spec(remove_/2 :: (fd(), key()) -> ok | reference()).
-spec(remove_/3 :: (fd(), key(), true | false) -> ok | reference()).

remove_(Pid, Key) ->
   remove_(Pid, Key, true).

remove_(Pid, Key, Flag)
 when is_binary(Key) ->
   request(Pid, {remove, Key, false}, Flag).


%%%----------------------------------------------------------------------------   
%%%
%%% memcached-like interface
%%%
%%%----------------------------------------------------------------------------   

%%
%% synchronous store key/val
-spec(set/3  :: (fd(), key(), val()) -> ok | {error, any()}).
-spec(set/4  :: (fd(), key(), val(), timeout()) -> ok | {error, any()}).

set(Pid, Key, Val) ->
   set(Pid, Key, Val, ?CONFIG_TIMEOUT).

set(Pid, Key, Val, Timeout) ->
   put(Pid, Key, Val, Timeout).

%%
%% asynchronous store key/val
-spec(set_/3 :: (fd(), key(), val()) -> ok | reference()).
-spec(set_/4 :: (fd(), key(), val(), true | false) -> ok | reference()).

set_(Pid, Key, Val) ->
   set_(Pid, Key, Val, true).

set_(Pid, Key, Val, Flag) ->
   put_(Pid, Key, Val, Flag).

%%
%% synchronous store key/val only if storage does not already hold data for this key
-spec(add/3  :: (fd(), key(), val()) -> ok | {error, any()}).
-spec(add/4  :: (fd(), key(), val(), timeout()) -> ok | {error, any()}).

add(Pid, Key, Val) ->
   add(Pid, Key, Val, ?CONFIG_TIMEOUT).

add(#dd{pid = Pid}, Key, Val, Timeout)
 when is_binary(Key), is_binary(Val) ->
   request(Pid, {add, Key, Val, true}, Timeout).

%%
%% asynchronous store key/val only if cache does not already hold data for this key
-spec(add_/3  :: (fd(), key(), val()) -> ok | reference()).
-spec(add_/4  :: (fd(), key(), val(), true | false) -> ok | reference()).

add_(#dd{pid = Pid}, Key, Val) ->
   add_(Pid, Key, Val, true).

add_(#dd{pid = Pid}, Key, Val, Flag)
 when is_binary(Key), is_binary(Val) ->
   request(Pid, {add, Key, Val, false}, Flag).

%%
%% synchronous store key/val only if cache does hold data for this key
-spec(replace/3  :: (fd(), key(), val()) -> ok | {error, any()}).
-spec(replace/4  :: (fd(), key(), val(), timeout()) -> ok | {error, any()}).

replace(Pid, Key, Val) ->
   replace(Pid, Key, Val, ?CONFIG_TIMEOUT).

replace(Pid, Key, Val, Timeout)
 when is_binary(Key), is_binary(Val) ->
   request(Pid, {replace, Key, Val, true}, Timeout).

%%
%% asynchronous store key/val only if cache does hold data for this key
-spec(replace_/3  :: (fd(), key(), val()) -> ok | reference()).
-spec(replace_/4  :: (fd(), key(), val(), true | false) -> ok | reference()).

replace_(Pid, Key, Val) ->
   replace_(Pid, Key, Val, true).

replace_(Pid, Key, Val, Flag)
 when is_binary(Key), is_binary(Val) ->
   request(Pid, {replace, Key, Val, false}, Flag).

%%
%% synchronously add data to existing key after existing data, the operation do not prolong entry ttl
-spec(append/3  :: (fd(), key(), val()) -> ok | {error, any()}).
-spec(append/4  :: (fd(), key(), val(), timeout()) -> ok | {error, any()}).

append(Pid, Key, Val) ->
   append(Pid, Key, Val, ?CONFIG_TIMEOUT).

append(Pid, Key, Val, Timeout)
 when is_binary(Key), is_binary(Val) ->
   request(Pid, {append, Key, Val, true}, Timeout).


%%
%% asynchronously add data to existing key after existing data, the operation do not prolong entry ttl
-spec(append_/3  :: (fd(), key(), val()) -> ok | reference()).
-spec(append_/4  :: (fd(), key(), val(), true | false) -> ok | reference()).

append_(Pid, Key, Val) ->
   append_(Pid, Key, Val, true).

append_(Pid, Key, Val, Flag)
 when is_binary(Key), is_binary(Val) ->
   request(Pid, {append, Key, Val, false}, Flag).

%%
%% synchronously add data to existing key before existing data
-spec(prepend/3  :: (fd(), key(), val()) -> ok | {error, any()}).
-spec(prepend/4  :: (fd(), key(), val(), timeout()) -> ok | {error, any()}).

prepend(Pid, Key, Val) ->
   prepend(Pid, Key, Val, ?CONFIG_TIMEOUT).

prepend(Pid, Key, Val, Timeout)
 when is_binary(Key), is_binary(Val) ->
   request(Pid, {prepend, Key, Val, true}, Timeout).

%%
%% asynchronously add data to existing key before existing data
-spec(prepend_/3  :: (fd(), key(), val()) -> ok | reference()).
-spec(prepend_/4  :: (fd(), key(), val(), true | false) -> ok | reference()).

prepend_(Pid, Key, Val) ->
   prepend_(Pid, Key, Val, true).

prepend_(Pid, Key, Val, Flag)
 when is_binary(Key), is_binary(Val) ->
   request(Pid, {prepend, Key, Val, false}, Flag).

%%
%% synchronous remove entry from cache
-spec(delete/2  :: (fd(), key()) -> ok | {error, any()}).
-spec(delete/3  :: (fd(), key(), timeout()) -> ok | {error, any()}).

delete(Pid, Key) ->
   delete(Pid, Key, ?CONFIG_TIMEOUT).

delete(Pid, Key, Timeout) ->
   remove(Pid, Key, Timeout).

%%
%% asynchronous remove entry from cache
-spec(delete_/2 :: (fd(), key()) -> ok | reference()).
-spec(delete_/3 :: (fd(), key(), true | false) -> ok | reference()).

delete_(Pid, Key) ->
   delete_(Pid, Key, true).

delete_(Pid, Key, Flag) ->
   remove_(Pid, Key, Flag).

%%
%% match key pattern
%%    ~    while prefix match
%%    =    while key equals
%%   >=
%%   =<
%%    >    while great   (Key, inf]
%%    <    while smaller [nil, Key)

-spec(match/2 :: (fd(), key()) -> datum:stream()).

match(Pid, {Pred, Prefix})
 when is_pid(Pid), is_binary(Prefix) ->
   {ok, Fd} = pipe:call(Pid, fd, ?CONFIG_TIMEOUT),
   dive_stream:new(Fd, {Pred, byte_size(Prefix), Prefix}, []);

match(Pid, '_') 
 when is_pid(Pid) ->
   {ok, Fd} = pipe:call(Pid, fd, ?CONFIG_TIMEOUT),
   dive_stream:new(Fd, '_', []);

match(Uid, Prefix) ->
   match(which(Uid), Prefix).


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
which(Name) -> 
   case pns:whereis(dive, Name) of
      Pid when is_pid(Pid) ->
         Pid;
      _ ->
         exit(no_proc)
   end.

%%
%% request
request(Pid, Req, true)
 when is_pid(Pid) ->
   pipe:cast(Pid, Req);

request(Pid, Req, false)
 when is_pid(Pid) ->
   pipe:send(Pid, Req), ok;

request(Pid, Req, Timeout)
 when is_pid(Pid) ->
   pipe:call(Pid, Req, Timeout);

request(Uid, Key, Flag) ->
   request(which(Uid), Key, Flag).

