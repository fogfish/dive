%%
%% helper functions
-module(dive_struct).

-export([
   encode/1
  ,decode/1
]).

%%
%% encode structure to binary format
encode(X)
 when is_binary(X) ->
   X;
encode(X) ->
   erlang:term_to_binary(X).

%%
%% decode structure from binary format
decode(<<131, _/binary>>=X) ->
   try
      erlang:binary_to_term(X)
   catch _:_ ->
      X
   end;
decode(X) ->
   X.
