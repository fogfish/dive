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
%%   database leader process
-module(dive_db).
-behaviour(pipe).

-include("dive.hrl").

-export([
   start_link/1
  ,init/1
  ,free/2
  ,handle/3
]).

%%
%% internal state
-record(fsm, {
   fd    = undefined :: any()    %% file descriptor
  ,type  = undefined :: ephemeral | persistent
  ,file  = undefined :: list()   %%
  ,cache = undefined :: pid()    %% database side-cache
  ,terminate = undefined :: true | false
  ,opts  = []        :: list()   %% list of file bucket options
  ,pids  = []        :: [pid()]  %% list of client pids
}).

%%%----------------------------------------------------------------------------   
%%%
%%% factory
%%%
%%%----------------------------------------------------------------------------   

%%
%%
start_link(Opts) ->
   pipe:start_link(?MODULE, [Opts], []).

init([Opts]) ->
   _ = erlang:process_flag(trap_exit, true),
   {ok, handle, init(Opts, #fsm{type = typeof(Opts)})}.

init([{owner, Pid} | Opts], State) ->
   Ref  = erlang:monitor(process, Pid),
   init(Opts, State#fsm{pids=[{Pid, Ref}]});

init([{file, File} | Opts], State) ->
   %% dive database is a singleton on file name
   pns:register(dive, File, self()),
   init(Opts, State#fsm{file=File});

init([{cache, Cache} | Opts], State) ->
   {ok, Pid} = cache:start_link(Cache),
   init(Opts, State#fsm{cache=Pid});

init([terminate | Opts], State) ->
   init(Opts, State#fsm{terminate=true});
   
init([_| Opts], State) ->
   init(Opts, State);

init([], State) ->
   {ok, FD} = init_db(State),
   State#fsm{fd = FD}.

%%
%%
free(_Reason, State) ->
   free_cache(State),
   free_db(State),
   ok.


%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

handle({dd, Pid}, Pipe, #fsm{pids = Pids} = State) ->
   case lists:keyfind(Pid, 1, Pids) of
      false ->
         Ref = erlang:monitor(process, Pid),
         pipe:ack(Pipe, {ok, dd(State)}),
         {next_state, handle, 
            State#fsm{
               pids=[{Pid, Ref} | Pids]
            }
         };
      _ ->
         pipe:ack(Pipe, {ok, dd(State)}),
         {next_state, handle, State}
   end;
   

handle({free, Pid}, Tx, S) ->
   case lists:keytake(Pid, 1, S#fsm.pids) of
      false ->
         pipe:ack(Tx, ok),
         {next_state, handle, S};
      {value, {_, Ref},   []} ->
         _ = erlang:demonitor(Ref, [flush]),
         pipe:ack(Tx, ok),         
         {stop, normal, S#fsm{pids=[]}};
      {value, {_, Ref}, Tail} ->
         _ = erlang:demonitor(Ref, [flush]),
         pipe:ack(Tx, ok),         
         {next_state, handle, S#fsm{pids=Tail}}
   end;

handle({'DOWN', _Ref, _Type, Pid, _Reason}, _Tx, S) ->
   case lists:keytake(Pid, 1, S#fsm.pids) of
      false ->
         {next_state, handle, S};
      {value, _,   []} ->
         {stop, normal, S#fsm{pids=[]}};
      {value, _, Tail} ->
         {next_state, handle, S#fsm{pids=Tail}}
   end;

handle({'EXIT', _, _}, _Tx, S) ->
   {stop, normal, S};


handle({apply, Fun}, Tx, State) ->
   try
      % Note: non-well behaving function causes side effect to mailbox of this process
      pipe:ack(Tx, Fun())
   catch _:_ ->
      pipe:ack(Tx, {error, abort})      
   end,
   {next_state, handle, State};

handle(Msg, _Tx, S) ->
   error_logger:error_msg("dive [db]: unexpected message ~p", [Msg]),
   {next_state, handle, S}.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

dd(#fsm{type = Type, fd = FD, cache = Cache}) ->
   #dd{type = Type, fd = FD, pid = self(), cache = Cache}.

%%
%%
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
%%
db_opts(Opts) ->
   [{create_if_missing, true} | lists:delete(persistent, Opts)].

%%
%% open file description
init_db(#fsm{type = ephemeral}) ->
   {ok, ets:new(undefined, [
      ordered_set,
      public, 
      {keypos, 1}, 
      {read_concurrency,  true}
   ])};

init_db(#fsm{type = persistent, file = File, opts = Opts}) ->
   ok = filelib:ensure_dir(
      filename:join([File, "README"])
   ),
   eleveldb:open(File, db_opts(Opts)).

%%
%%
free_cache(#fsm{cache = undefined}) ->
   ok;
free_cache(#fsm{cache = Cache}) ->
   cache:drop(Cache).


%%
%%
free_db(#fsm{type = ephemeral, fd = FD}) ->
   ets:delete(FD);

free_db(#fsm{type = persistent, terminate = true, fd = FD, file = File}) ->
   eleveldb:close(FD),
   os:cmd("rm -Rf " ++ File);

free_db(#fsm{type = persistent, fd = FD}) ->
   eleveldb:close(FD).

