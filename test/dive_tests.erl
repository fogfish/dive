-module(dive_tests).
-include_lib("eunit/include/eunit.hrl").

%%%------------------------------------------------------------------
%%%
%%% suite(s)
%%%
%%%------------------------------------------------------------------   

ephemeral_test_() ->
   {foreach,
      fun ephemeral/0,
      fun free/1,
      [
         fun crud/1
        ,fun add/1
        ,fun replace/1
        ,fun append/1
        ,fun prepend/1
        ,fun stream/1
      ]
   }.

persistent_test_() ->
   {foreach,
      fun persistent/0,
      fun free/1,
      [
         fun crud/1
        ,fun add/1
        ,fun replace/1
        ,fun append/1
        ,fun prepend/1
        ,fun stream/1
      ]
   }.

%%%------------------------------------------------------------------
%%%
%%% setup
%%%
%%%------------------------------------------------------------------   

ephemeral() ->
   dive:start(),
   {ok, FD} = dive:new([ephemeral]),
   FD.

persistent() ->
   dive:start(),
   {ok, FD} = dive:new([persistent, {file, "/tmp/dive"}]),
   FD.

free(FD) ->
   dive:free(FD),
   os:cmd("rm -Rf /tmp/dive").

%%%------------------------------------------------------------------
%%%
%%% unit test
%%%
%%%------------------------------------------------------------------   

crud(FD) ->
   [
      ?_assertMatch(ok, dive:put(FD,  <<"key-1">>, <<"val-1">>))
     ,?_assertMatch(_,  dive:put_(FD, <<"key-2">>, <<"val-2">>))
     ,?_assertMatch({ok, <<"val-1">>}, dive:get(FD, <<"key-1">>))
     ,?_assertMatch({ok, <<"val-2">>}, dive:get(FD, <<"key-2">>))
     ,?_assertMatch(ok, dive:put(FD,  <<"key-1">>, <<"val-x">>))
     ,?_assertMatch(_,  dive:put_(FD, <<"key-2">>, <<"val-x">>))
     ,?_assertMatch(ok, dive:remove(FD,  <<"key-1">>))
     ,?_assertMatch(_,  dive:remove_(FD, <<"key-2">>))
     ,?_assertMatch({error, not_found}, dive:get(FD, <<"key-1">>))
     ,?_assertMatch({error, not_found}, dive:get(FD, <<"key-2">>))
   ].

add(FD) ->
   [
      ?_assertMatch(ok, dive:add(FD, <<"key-1">>, <<"val-1">>))
     ,?_assertMatch({error, conflict}, dive:add(FD, <<"key-1">>, <<"val-2">>))
     ,?_assertMatch(ok, dive:remove(FD, <<"key-1">>))
   ].

replace(FD) ->
   [
      ?_assertMatch({error, not_found}, dive:replace(FD, <<"key-1">>, <<"val-2">>))
     ,?_assertMatch(ok, dive:put(FD,  <<"key-1">>, <<"val-1">>))
     ,?_assertMatch(ok, dive:replace(FD, <<"key-1">>, <<"val-2">>))
     ,?_assertMatch(ok, dive:remove(FD, <<"key-1">>))
   ].

append(FD) ->
   [
      ?_assertMatch(ok, dive:append(FD, <<"key-1">>, <<"val-1">>))
     ,?_assertMatch({ok, <<"val-1">>}, dive:get(FD, <<"key-1">>))
     ,?_assertMatch(ok, dive:append(FD, <<"key-1">>, <<"val-2">>))
     ,?_assertMatch({ok, <<"val-1val-2">>}, dive:get(FD, <<"key-1">>))
     ,?_assertMatch(ok, dive:remove(FD, <<"key-1">>))
   ].

prepend(FD) ->
   [
      ?_assertMatch(ok, dive:prepend(FD, <<"key-1">>, <<"val-1">>))
     ,?_assertMatch({ok, <<"val-1">>}, dive:get(FD, <<"key-1">>))
     ,?_assertMatch(ok, dive:prepend(FD, <<"key-1">>, <<"val-2">>))
     ,?_assertMatch({ok, <<"val-2val-1">>}, dive:get(FD, <<"key-1">>))
     ,?_assertMatch(ok, dive:remove(FD, <<"key-1">>))
   ].

stream(FD) ->
   [
      ?_assertMatch(ok, dive:put(FD,  <<"key-1">>, <<"val-1">>))
     ,?_assertMatch(_,  dive:put_(FD, <<"key-2">>, <<"val-2">>))
     ,?_assertMatch(
         [{<<"key-1">>, <<"val-1">>}, {<<"key-2">>, <<"val-2">>}],
         stream:list(dive:match(FD, '_'))
      )
     ,?_assertMatch(
         [{<<"key-1">>, <<"val-1">>}, {<<"key-2">>, <<"val-2">>}],
         stream:list(dive:match(FD, {'~', <<"key">>}))
      )
     ,?_assertMatch(
         [{<<"key-1">>, <<"val-1">>}],
         stream:list(dive:match(FD, {'=', <<"key-1">>}))
      )
     ,?_assertMatch(
         [{<<"key-1">>, <<"val-1">>}, {<<"key-2">>, <<"val-2">>}],
         stream:list(dive:match(FD, {'>=', <<"key-1">>}))
      )
     ,?_assertMatch(
         [{<<"key-2">>, <<"val-2">>}, {<<"key-1">>, <<"val-1">>}],
         stream:list(dive:match(FD, {'=<', <<"key-2">>}))
      )
     ,?_assertMatch(
         [{<<"key-2">>, <<"val-2">>}],
         stream:list(dive:match(FD, {'>', <<"key-1">>}))
      )
     ,?_assertMatch(
         [{<<"key-1">>, <<"val-1">>}],
         stream:list(dive:match(FD, {'<', <<"key-2">>}))
      )
     ,?_assertMatch(ok, dive:remove(FD, <<"key-1">>))
     ,?_assertMatch(ok, dive:remove(FD, <<"key-2">>))
   ].
