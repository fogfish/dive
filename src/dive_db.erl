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

-export([
   start_link/1
  ,init/1
  ,free/2
  ,ioctl/2
  ,handle/3
]).

%%
%% internal state
-record(fsm, {
   fd    = undefined :: any()    %% file descriptor
  ,file  = undefined :: list()   %%
  ,cache = undefined :: pid()    %% database side-cache
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
   {ok, handle, init(Opts, #fsm{})}.

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

init([_| Opts], State) ->
   init(Opts, State);

init([], State) ->
   {ok, FD} = eleveldb:open(
      State#fsm.file
     ,db_opts(State#fsm.opts)
   ),
   State#fsm{fd = FD}.

%%
%%
free(_, #fsm{fd = FD, cache = Cache}) ->
   eleveldb:close(FD),
   cache:drop(Cache),
   ok.

%%
%%
ioctl(fd, #fsm{fd=X}) ->
   X;
ioctl(cache, #fsm{cache=X}) ->
   X;   
ioctl(_, _) ->
   throw(not_supported).

%%%----------------------------------------------------------------------------   
%%%
%%% pipe
%%%
%%%----------------------------------------------------------------------------   

handle({init, Pid}, Tx, S) ->
   case lists:keyfind(Pid, 1, S#fsm.pids) of
      false ->
         Ref = erlang:monitor(process, Pid),
         pipe:ack(Tx, {ok, self()}),
         {next_state, handle, S#fsm{pids=[{Pid, Ref}|S#fsm.pids]}};
      _ ->
         pipe:ack(Tx, {ok, self()}),
         {next_state, handle, S}
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
      pipe:ack(Tx, Fun())
   catch _:_ ->
      pipe:ack(Tx, {error, abort})      
   end,
   {next_state, handle, State};

handle(Msg, _Tx, S) ->
   error_logger:error_message("dive [db]: unexpected message ~p", [Msg]),
   {next_state, handle, S}.

%%%----------------------------------------------------------------------------   
%%%
%%% private
%%%
%%%----------------------------------------------------------------------------   

%%
%%
db_opts(Opts) ->
   [{create_if_missing, true} | Opts].



