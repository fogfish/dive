-module(dive_benchmark).

-export([
   new/1, run/4
]).


%%%------------------------------------------------------------------
%%%
%%% factory
%%%
%%%------------------------------------------------------------------

new(_) ->
   _ = dive:start(),
   {ok, Pid} = dive:new([{file, basho_bench_config:get(db, "/tmp/dive")}]),
   {ok, dive:fd(Pid)}.

%%%------------------------------------------------------------------
%%%
%%% run
%%%
%%%------------------------------------------------------------------

run(put, KeyGen, ValGen, Fd) ->
   case dive:put(Fd, scalar:s(KeyGen()), ValGen()) of
      ok ->
         {ok, Fd};
      {error, Reason} ->
         {error, Reason, Fd}
   end;

run(get, KeyGen, _ValGen, Fd) ->
   case dive:get(Fd, scalar:s(KeyGen())) of
      {ok, _} ->
         {ok, Fd};
      {error, Reason} ->
         {error, Reason, Fd}
   end;

run(remove, KeyGen, _ValGen, Fd) ->
   case dive:remove(Fd, scalar:s(KeyGen())) of
      ok ->
         {ok, Fd};
      {error, Reason} ->
         {error, Reason, Fd}
   end;

%%
%%
run(append, KeyGen, ValGen, Fd) ->
   case dive:append(Fd, scalar:s(KeyGen()), ValGen()) of
      {ok, _} ->
         {ok, Fd};
      {error, Reason} ->
         {error, Reason, Fd}
   end;

run(head, KeyGen, _ValGen, Fd) ->
   case dive:head(Fd, scalar:s(KeyGen())) of
      {ok, _} ->
         {ok, Fd};
      {error, Reason} ->
         {error, Reason, Fd}
   end;

run(take, KeyGen, _ValGen, Fd) ->
   case dive:take(Fd, scalar:s(KeyGen())) of
      {ok, _} ->
         {ok, Fd};
      {error, Reason} ->
         {error, Reason, Fd}
   end.



