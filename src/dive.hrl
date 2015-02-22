
-define(CONFIG_TIMEOUT, infinity).

%%
%% dive file description
-record(dd, {
   fd  = undefined :: any() %% file description
  ,pid = undefined :: any() %% leader process
}).