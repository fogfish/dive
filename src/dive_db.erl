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
   start_link/2
  ,init/1
  ,free/2
  ,ioctl/2
  ,handle/3
]).

%%
%% internal state
-record(fsm, {
   fd    = undefined :: any()    %% file descriptor
  ,file  = undefined :: list()
  ,opts  = []        :: list()   %% list of file bucket options
  ,pids  = []        :: [pid()]  %% list of client pids
}).

%%%----------------------------------------------------------------------------   
%%%
%%% factory
%%%
%%%----------------------------------------------------------------------------   

start_link(undefined, Opts) ->
   pipe:start_link(?MODULE, [Opts], []);

start_link(Name, Opts) ->
   pipe:start_link({local, Name}, ?MODULE, [Opts], []).

init([Opts]) ->
   _ = erlang:process_flag(trap_exit, true),
   {ok, handle, init(Opts, #fsm{})}.

init([{owner, Pid} | Opts], S) ->
   Ref  = erlang:monitor(process, Pid),
   init(Opts, S#fsm{pids=[{Pid, Ref}]});

init([{file, File} | Opts], S) ->
   pns:register(dive, File, self()),
   init(Opts, S#fsm{file=File});

init([_| Opts], S) ->
   init(Opts, S);

init([], S) ->
   {ok, FD} = eleveldb:open(S#fsm.file, db_opts(S#fsm.opts)),
   S#fsm{
      fd = FD
   }.

free(_, S) ->
   eleveldb:close(S#fsm.fd),
   ok.

ioctl(fd, #fsm{fd=X}) ->
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
         pipe:ack(Tx, ok),
         {next_state, handle, S#fsm{pids=[{Pid, Ref}|S#fsm.pids]}};
      _ ->
         pipe:ack(Tx, ok),
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



