-module(dive).
-include("dive.hrl").

-export([start/0]).
-export([
   new/1
  ,free/1
  ,ioctl/2
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

%% @todo -> remove put_ / remove_ -> replace with i/o flag (sync)

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
%%    ephemeral | persistent
%%    {file, list()} - name of folder to maintain database
%%    {owner, pid()} - database owner process
%%    {cache, pid()} - database in-memory cache  
%%    terminate      - terminate database on exit
%%    ...
%%    see eleveldb options
-spec new(list()) -> {ok, fd()} | {error, any()}.

new(Opts) ->
   try
      Type  = typeof(Opts),
      {ok, Pid} = ensure(Type, Opts), 
      {ok,   _} = pipe:call(Pid, {init, self()}),
      FD    = pipe:ioctl(Pid, fd),
      Cache = pipe:ioctl(Pid, cache),
      {ok, #dd{type = Type, fd = FD, pid = Pid, cache = Cache}}
   catch _:{badmatch, {error,Reason}} ->
      {error, Reason}
   end. 

typeof(Opts) ->
   case
      {opts:val(persistent, true, Opts), opts:val(ephemeral, false, Opts)} 
   of
      {_, true} -> 
         ephemeral;
      {true, _} ->
         persistent
   end.

%%
%% ensure database leader process
ensure(ephemeral,  Opts) ->
   supervisor:start_child(dive_db_sup, [ephemeral, Opts]);
ensure(persistent, Opts) ->
   File = opts:val(file, Opts),
   case pns:whereis(dive, File) of
      undefined ->
         supervisor:start_child(dive_db_sup, [persistent, Opts]);
      Pid       ->
         {ok, Pid}
   end.

%%
%% release database instance
-spec free(pid()) -> ok.

free(#dd{pid = Pid})
 when is_pid(Pid) ->
   pipe:call(Pid, {free, self()}).

%%
%% control i/o 
-spec ioctl(atom(), fd()) -> fd().

ioctl(nocache, #dd{} = FD) ->
   FD#dd{cache = undefined}.

%%
%% execute lambda expression
-spec apply(fd(), function()) -> ok | {ok, any()} | {error, any()}.

apply(Pid, Fun)
 when is_function(Fun) ->
   request(Pid, {apply, Fun}, infinity).

%%
%% execute lambda expression asynchronously
-spec apply_(fd(), function()) -> ok.

apply_(Pid, Fun)
 when is_function(Fun) ->
   request(Pid, {apply, Fun}, false).

%%%----------------------------------------------------------------------------   
%%%
%%% hashmap-like interface
%%%
%%%----------------------------------------------------------------------------   

%%
%% put key/val
-spec put(fd(), key(), val())  -> ok | {error, any()}.
-spec put_(fd(), key(), val()) -> ok | {error, any()}.

put(FD, Key, Val)
 when is_binary(Key), is_binary(Val) ->
   put_cache(FD, Key, Val, true),
   put_btree(FD, Key, Val, true).

put_(FD, Key, Val)
 when is_binary(Key), is_binary(Val) ->
   put_cache(FD, Key, Val, false),
   put_btree(FD, Key, Val, false).

put_cache(#dd{cache = undefined}, _, _, _) ->
   ok;
put_cache(#dd{cache = Cache}, Key, Val,  true) ->
   cache:put(Cache, Key, Val);
put_cache(#dd{cache = Cache}, Key, Val, false) ->
   cache:put_(Cache, Key, Val).

put_btree(#dd{type = ephemeral,  fd = FD}, Key, Val, _) ->
   true = ets:insert(FD, {Key, Val}), 
   ok;
put_btree(#dd{type = persistent, fd = FD}, Key, Val, Sync) ->
   eleveldb:put(FD, Key, Val, [{sync, Sync}]).

%%
%% synchronous get entity from storage
-spec get(fd(), key()) -> {ok, val()} | {error, any()}.

get(#dd{cache = undefined}=FD, Key)
 when is_binary(Key) ->
   get_btree(FD, Key);

get(#dd{cache = Cache}=FD, Key)
 when is_binary(Key) ->
   case cache:get(Cache, Key) of
      undefined ->
         get_btree(FD, Key);
      Val ->
         {ok, Val}
   end.

get_btree(#dd{type = ephemeral, fd = FD}=DD, Key) ->
   case ets:lookup(FD, Key) of
      [{_, Val}] ->
         put_cache(DD, Key, Val, true),
         {ok, Val};
      [] ->
         {error, not_found}
   end;

get_btree(#dd{type = persistent, fd = FD}=DD, Key) ->
   case eleveldb:get(FD, Key, []) of
      {ok, Val} ->
         put_cache(DD, Key, Val, true),
         {ok, Val};
      not_found ->
         {error, not_found};
      {error,_} = Error ->
         Error
   end.

%%
%% remove entity from storage
-spec remove(fd(), key()) -> ok | {error, any()}.
-spec remove_(fd(), key()) -> ok | {error, any()}.


remove(FD, Key)
 when is_binary(Key) ->
   remove_cache(FD, Key, true),
   remove_btree(FD, Key, true).

remove_(FD, Key)
 when is_binary(Key) ->
   remove_cache(FD, Key, false),
   remove_btree(FD, Key, false).


remove_cache(#dd{cache = undefined}, _, _) ->
   ok;
remove_cache(#dd{cache = Cache}, Key, true) ->
   cache:remove(Cache, Key);
remove_cache(#dd{cache = Cache}, Key, false) ->
   cache:remove_(Cache, Key).

remove_btree(#dd{type = ephemeral,  fd = FD}, Key, _) ->
   ets:delete(FD, Key), 
   ok;
remove_btree(#dd{type = persistent, fd = FD}, Key, Sync) ->
   eleveldb:delete(FD, Key, [{sync, Sync}]).

%%%----------------------------------------------------------------------------   
%%%
%%% memcached-like interface
%%%
%%%----------------------------------------------------------------------------   

%%
%% synchronous store key/val
-spec set(fd(), key(), val()) -> ok | {error, any()}.

set(Pid, Key, Val) ->
   put(Pid, Key, Val).

%%
%% asynchronous store key/val
-spec set_(fd(), key(), val()) -> ok | reference().

set_(Pid, Key, Val) ->
   put_(Pid, Key, Val).

%%
%% synchronous store key/val only if storage does not already hold data for this key
-spec add(fd(), key(), val()) -> ok | {error, any()}.

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
-spec add_(fd(), key(), val()) -> ok | reference().

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
-spec replace(fd(), key(), val()) -> ok | {error, any()}.

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
-spec replace_(fd(), key(), val()) -> ok | reference().

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
-spec append(fd(), key(), val()) -> ok | {error, any()}.

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
-spec append_(fd(), key(), val()) -> ok | reference().

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
-spec prepend(fd(), key(), val()) -> ok | {error, any()}.

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
-spec prepend_(fd(), key(), val()) -> ok | reference().

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
-spec delete(fd(), key()) -> ok | {error, any()}.

delete(Pid, Key) ->
   remove(Pid, Key).

%%
%% asynchronous remove entry from cache
-spec delete_(fd(), key()) -> ok | reference().

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
-spec match(fd(), key()) -> datum:stream().

match(FD, {Pred, Prefix})
 when is_binary(Prefix) ->
   dive_stream:new({Pred, Prefix}, FD, []);

match(FD, '_') ->
   dive_stream:new('_', FD, []).


%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   



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

