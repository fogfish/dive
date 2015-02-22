-module(dive).
-include("dive.hrl").

-export([start/0]).
-export([
   new/1
  ,free/1
  ,apply/2
  ,apply_/2
   %% hashmap interface
  ,put/3
  ,put_/3
  ,get/2
  ,remove/2
  ,remove_/2
   %% memcached-like interface
  ,set/3
  ,set_/3
  ,add/3
  ,add_/3
  ,replace/3
  ,replace_/3
  ,append/3
  ,append_/3
  ,prepend/3
  ,prepend_/3
  ,delete/2
  ,delete_/2
   % match interface
  ,match/2
]).

%%
-type(fd()  :: #dd{}).
-type(key() :: binary()).
-type(val() :: binary() | list()).

-define(is_fd(X), is_binary(X)).

%%
%%
start() ->
   applib:boot(?MODULE, []).


%%
%% create new database instance
%%  Options:
%%    {file, list()} - name of folder to maintain database
%%    {owner, pid()} - database owner process
%%    {cache, pid()} - database in-memory cache  
%%    ...
%%    see eleveldb options
-spec(new/1 :: (list()) -> {ok, fd()} | {error, any()}).

new(Opts) ->
   try
      File = opts:val(file, Opts),
      {ok, Pid} = ensure(File, Opts), 
      {ok,   _} = pipe:call(Pid, {init, self()}),
      FD    = pipe:ioctl(Pid, fd),
      Cache = pipe:ioctl(Pid, cache),
      {ok, #dd{fd = FD, pid = Pid, cache = Cache}}
   catch _:{badmatch, {error,Reason}} ->
      {error, Reason}
   end. 

%%
%% ensure database leader process
ensure(File, Opts) ->
   case pns:whereis(dive, File) of
      undefined ->
         supervisor:start_child(dive_db_sup, [Opts]);
      Pid       ->
         {ok, Pid}
   end.

%%
%% release database instance
-spec(free/1 :: (pid()) -> ok).

free(#dd{pid = Pid})
 when is_pid(Pid) ->
   pipe:call(Pid, {free, self()}).

%%
%% execute closure
-spec(apply/2 :: (fd(), function()) -> ok | {ok, any()} | {error, any()}).

apply(Pid, Fun)
 when is_function(Fun) ->
   request(Pid, {apply, Fun}, infinity).

%%
%% execute closure asynchronously
-spec(apply_/2  :: (fd(), function()) -> ok).

apply_(Pid, Fun)
 when is_function(Fun) ->
   request(Pid, {apply, Fun}, false).

%%%----------------------------------------------------------------------------   
%%%
%%% hashmap-like interface
%%%
%%%----------------------------------------------------------------------------   

%%
%% synchronous storage put
-spec(put/3 :: (fd(), key(), val()) -> ok | {error, any()}).

put(#dd{fd = FD, cache = undefined}, Key, Val)
 when is_binary(Key), is_binary(Val) ->
   eleveldb:put(FD, Key, Val, [{sync, true}]);

put(#dd{fd = FD, cache = Cache}, Key, Val)
 when is_binary(Key), is_binary(Val) ->
   cache:put(Cache, Key, Val),
   eleveldb:put(FD, Key, Val, [{sync, true}]).   

%%
%% asynchronous storage put
-spec(put_/3 :: (fd(), key(), val()) -> ok | reference()).

put_(#dd{fd = Fd, cache = undefined}, Key, Val)
 when is_binary(Key), is_binary(Val) ->
   eleveldb:put(Fd, Key, Val, [{sync, false}]);

put_(#dd{fd = FD, cache = Cache}, Key, Val)
 when is_binary(Key), is_binary(Val) ->
   cache:put_(Cache, Key, Val),
   eleveldb:put(FD, Key, Val, [{sync, false}]).   

%%
%% synchronous get entity from storage
-spec(get/2 :: (fd(), key()) -> {ok, val()} | {error, any()}).

get(#dd{fd = FD, cache = undefined}, Key)
 when is_binary(Key) ->
   db_get(FD, Key);

get(#dd{fd = FD, cache = Cache}, Key)
 when is_binary(Key) ->
   case cache:get(Cache, Key) of
      undefined ->
         db_get(FD, Cache, Key);
      Val ->
         {ok, Val}
   end.

%%
%% synchronous remove entity from storage
-spec(remove/2 :: (fd(), key()) -> ok | {error, any()}).

remove(#dd{fd = FD, cache = undefined}, Key)
 when is_binary(Key) ->
   eleveldb:delete(FD, Key, [{sync, true}]);

remove(#dd{fd = FD, cache = Cache}, Key)
 when is_binary(Key) ->
   cache:remove(Cache, Key),
   eleveldb:delete(FD, Key, [{sync, true}]).

%%
%% asynchronous remove key from dataset
-spec(remove_/2 :: (fd(), key()) -> ok | reference()).

remove_(#dd{fd = FD, cache = undefined}, Key)
 when is_binary(Key) ->
   eleveldb:delete(FD, Key, [{sync, false}]);

remove_(#dd{fd = FD, cache = Cache}, Key)
 when is_binary(Key) ->
   cache:remove_(Cache, Key),
   eleveldb:delete(FD, Key, [{sync, false}]).


%%%----------------------------------------------------------------------------   
%%%
%%% memcached-like interface
%%%
%%%----------------------------------------------------------------------------   

%%
%% synchronous store key/val
-spec(set/3  :: (fd(), key(), val()) -> ok | {error, any()}).

set(Pid, Key, Val) ->
   put(Pid, Key, Val).

%%
%% asynchronous store key/val
-spec(set_/3 :: (fd(), key(), val()) -> ok | reference()).

set_(Pid, Key, Val) ->
   put_(Pid, Key, Val).

%%
%% synchronous store key/val only if storage does not already hold data for this key
-spec(add/3  :: (fd(), key(), val()) -> ok | {error, any()}).

add(Pid, Key, Val)
 when is_binary(Key), is_binary(Val) ->
   dive:apply(Pid,
      fun() ->
         case dive:get(Pid, Key) of
            {error, not_found} ->
               dive:put(Pid, Key, Val);
            {ok, _} ->
               {error, conflict};
            Error   ->
               Error
         end
      end
   ).

%%
%% asynchronous store key/val only if cache does not already hold data for this key
-spec(add_/3  :: (fd(), key(), val()) -> ok | reference()).

add_(#dd{pid = Pid}, Key, Val)
 when is_binary(Key), is_binary(Val) ->
   dive:apply_(Pid,
      fun() ->
         case dive:get(Pid, Key) of
            {error, not_found} ->
               dive:put_(Pid, Key, Val);
            {ok, _} ->
               {error, conflict};
            Error   ->
               Error
         end
      end
   ).

%%
%% synchronous store key/val only if cache does hold data for this key
-spec(replace/3  :: (fd(), key(), val()) -> ok | {error, any()}).

replace(Pid, Key, Val)
 when is_binary(Key), is_binary(Val) ->
   dive:apply(Pid,
      fun() ->
         case dive:get(Pid, Key) of
            {ok, _} ->
               dive:put(Pid, Key, Val);
            Error   ->
               Error
         end
      end
   ).

%%
%% asynchronous store key/val only if cache does hold data for this key
-spec(replace_/3  :: (fd(), key(), val()) -> ok | reference()).

replace_(Pid, Key, Val)
 when is_binary(Key), is_binary(Val) ->
   dive:apply(Pid,
      fun() ->
         case dive:get(Pid, Key) of
            {ok, _} ->
               dive:put_(Pid, Key, Val);
            Error   ->
               Error
         end
      end
   ).

%%
%% synchronously add data to existing key after existing data, the operation do not prolong entry ttl
-spec(append/3  :: (fd(), key(), val()) -> ok | {error, any()}).

append(Pid, Key, Val)
 when is_binary(Key), is_binary(Val) ->
   dive:apply(Pid,
      fun() ->
         case dive:get(Pid, Key) of
            {ok, Head} ->
               dive:put(Pid, Key, <<Head/binary, Val/binary>>);

            {error, not_found} ->
               dive:put(Pid, Key, Val);

            Error ->
               Error
         end
      end
   ).

%%
%% asynchronously add data to existing key after existing data, the operation do not prolong entry ttl
-spec(append_/3  :: (fd(), key(), val()) -> ok | reference()).

append_(Pid, Key, Val)
 when is_binary(Key), is_binary(Val) ->
   dive:apply_(Pid,
      fun() ->
         case dive:get(Pid, Key) of
            {ok, Head} ->
               dive:put_(Pid, Key, <<Head/binary, Val/binary>>);

            {error, not_found} ->
               dive:put_(Pid, Key, Val);

            Error ->
               Error
         end
      end
   ).

%%
%% synchronously add data to existing key before existing data
-spec(prepend/3  :: (fd(), key(), val()) -> ok | {error, any()}).

prepend(Pid, Key, Val)
 when is_binary(Key), is_binary(Val) ->
   dive:apply(Pid,
      fun() ->
         case dive:get(Pid, Key) of
            {ok, Head} ->
               dive:put(Pid, Key, <<Val/binary, Head/binary>>);

            {error, not_found} ->
               dive:put(Pid, Key, Val);

            Error ->
               Error
         end
      end
   ).

%%
%% asynchronously add data to existing key before existing data
-spec(prepend_/3  :: (fd(), key(), val()) -> ok | reference()).

prepend_(Pid, Key, Val)
 when is_binary(Key), is_binary(Val) ->
   dive:apply_(Pid,
      fun() ->
         case dive:get(Pid, Key) of
            {ok, Head} ->
               dive:put_(Pid, Key, <<Val/binary, Head/binary>>);

            {error, not_found} ->
               dive:put_(Pid, Key, Val);

            Error ->
               Error
         end
      end
   ).

%%
%% synchronous remove entry from cache
-spec(delete/2  :: (fd(), key()) -> ok | {error, any()}).

delete(Pid, Key) ->
   remove(Pid, Key).

%%
%% asynchronous remove entry from cache
-spec(delete_/2 :: (fd(), key()) -> ok | reference()).

delete_(Pid, Key) ->
   remove_(Pid, Key).

%%
%% match key pattern
%%    ~    while prefix match
%%    =    while key equals
%%   >=
%%   =<
%%    >    while great   (Key, inf]
%%    <    while smaller [nil, Key)
-spec(match/2 :: (fd(), key()) -> datum:stream()).

match(#dd{fd = FD}, {Pred, Prefix})
 when is_binary(Prefix) ->
   dive_stream:new(FD, {Pred, byte_size(Prefix), Prefix}, []);

match(#dd{fd = FD}, '_') ->
   dive_stream:new(FD, '_', []).


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
db_get(FD, Key) ->
   case eleveldb:get(FD, Key, []) of
      {ok, Val} ->
         {ok, Val};
      not_found ->
         {error, not_found};
      {error,_} = Error ->
         Error
   end.

db_get(FD, Cache, Key) ->
   case eleveldb:get(FD, Key, []) of
      {ok, Val} ->
         cache:put(Cache, Key, Val),
         {ok, Val};
      not_found ->
         {error, not_found};
      {error,_} = Error ->
         Error
   end.


%%
%% request
request(#dd{pid = Pid}, Req, true)
 when is_pid(Pid) ->
   pipe:cast(Pid, Req);

request(#dd{pid = Pid}, Req, false)
 when is_pid(Pid) ->
   pipe:send(Pid, Req), ok;

request(#dd{pid = Pid}, Req, Timeout)
 when is_pid(Pid) ->
   pipe:call(Pid, Req, Timeout).

