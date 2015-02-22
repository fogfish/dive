
%%
%% dive database descriptor
-record(dd, {
   fd    = undefined :: any() %% file description
  ,pid   = undefined :: pid() %% leader process
  ,cache = undefined :: pid() %% cache process
}).