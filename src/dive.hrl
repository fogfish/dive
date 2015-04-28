
%%
%% dive database descriptor
-record(dd, {
   type  = undefined :: persistent | ephemeral
  ,fd    = undefined :: any() %% file description
  ,pid   = undefined :: pid() %% leader process
  ,cache = undefined :: pid() %% cache process
}).

