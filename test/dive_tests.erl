-module(dive_tests).
-include_lib("eunit/include/eunit.hrl").

-define(DIVE, "/tmp/dive").

%%%------------------------------------------------------------------
%%%
%%% suite(s)
%%%
%%%------------------------------------------------------------------   

hashmap_test_() ->
   {foreach,
      fun init/0,
      fun free/1,
      [
         fun put/1
        ,fun get/1
        ,fun remove/1
        ,fun add/1
        ,fun replace/1
        ,fun append/1
        ,fun prepend/1
      ]
   }.

%%%------------------------------------------------------------------
%%%
%%% setup
%%%
%%%------------------------------------------------------------------   

init() ->
   dive:start(),
   {ok, Pid} = dive:new([{file, ?DIVE}]),
   Pid.

free(Pid) ->
   dive:free(Pid).

%%%------------------------------------------------------------------
%%%
%%% unit test
%%%
%%%------------------------------------------------------------------   

put(Pid) ->
   [
      ?_assertMatch(ok, dive:put(Pid,  <<"key-1">>, <<"val-1">>))
     ,?_assertMatch(ok, dive:put(Pid,  <<"key-2">>, <<"val-2">>, 5000))
     ,?_assertMatch(_,  dive:put_(Pid, <<"key-3">>, <<"val-3">>))
     ,?_assertMatch(_,  dive:put_(Pid, <<"key-4">>, <<"val-4">>, true))
     ,?_assertMatch(ok, dive:put_(Pid, <<"key-5">>, <<"val-5">>, false))
   ].

get(Pid) ->
   [
      ?_assertMatch({ok, <<"val-1">>}, dive:get(Pid, <<"key-1">>))
     ,?_assertMatch({ok, <<"val-2">>}, dive:get(Pid, <<"key-2">>, 5000))
     ,?_assertMatch({ok, <<"val-3">>}, dive:get(Pid, <<"key-3">>))
     ,?_assertMatch({ok, <<"val-4">>}, dive:get(Pid, <<"key-4">>))
     ,?_assertMatch({ok, <<"val-5">>}, dive:get(Pid, <<"key-5">>))
   ].

remove(Pid) ->
   [
      ?_assertMatch(ok, dive:remove(Pid,  <<"key-1">>))
     ,?_assertMatch(ok, dive:remove(Pid,  <<"key-2">>, 5000))
     ,?_assertMatch(_,  dive:remove_(Pid, <<"key-3">>))
     ,?_assertMatch(_,  dive:remove_(Pid, <<"key-4">>, true))
     ,?_assertMatch(ok, dive:remove_(Pid, <<"key-5">>, false))

     ,?_assertMatch({error, not_found}, dive:get(Pid, <<"key-1">>))
     ,?_assertMatch({error, not_found}, dive:get(Pid, <<"key-2">>, 5000))
     ,?_assertMatch({error, not_found}, dive:get(Pid, <<"key-3">>))
     ,?_assertMatch({error, not_found}, dive:get(Pid, <<"key-4">>))
     ,?_assertMatch({error, not_found}, dive:get(Pid, <<"key-5">>))
   ].

add(Pid) ->
   [
      ?_assertMatch(ok, dive:add(Pid, <<"key-1">>, <<"val-1">>))
     ,?_assertMatch({error, conflict}, dive:add(Pid, <<"key-1">>, <<"val-2">>))
     ,?_assertMatch(ok, dive:remove(Pid, <<"key-1">>))
   ].

replace(Pid) ->
   [
      ?_assertMatch({error, not_found}, dive:replace(Pid, <<"key-1">>, <<"val-2">>))
     ,?_assertMatch(ok, dive:put(Pid,  <<"key-1">>, <<"val-1">>))
     ,?_assertMatch(ok, dive:replace(Pid, <<"key-1">>, <<"val-2">>))
     ,?_assertMatch(ok, dive:remove(Pid, <<"key-1">>))
   ].

append(Pid) ->
   [
      ?_assertMatch(ok, dive:append(Pid, <<"key-1">>, <<"val-1">>))
     ,?_assertMatch({ok, <<"val-1">>}, dive:get(Pid, <<"key-1">>))
     ,?_assertMatch(ok, dive:append(Pid, <<"key-1">>, <<"val-2">>))
     ,?_assertMatch({ok, <<"val-1val-2">>}, dive:get(Pid, <<"key-1">>))
     ,?_assertMatch(ok, dive:remove(Pid, <<"key-1">>))
   ].

prepend(Pid) ->
   [
      ?_assertMatch(ok, dive:prepend(Pid, <<"key-1">>, <<"val-1">>))
     ,?_assertMatch({ok, <<"val-1">>}, dive:get(Pid, <<"key-1">>))
     ,?_assertMatch(ok, dive:prepend(Pid, <<"key-1">>, <<"val-2">>))
     ,?_assertMatch({ok, <<"val-2val-1">>}, dive:get(Pid, <<"key-1">>))
     ,?_assertMatch(ok, dive:remove(Pid, <<"key-1">>))
   ].
