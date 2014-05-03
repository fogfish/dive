

%% dive default cache
-define(CACHE_DEF, [
   {n,          12}   %% number of cache segments
  ,{ttl,  2 * 3600}   %% default time to live
  ,{policy,    lru}   %% cache policy
  ,{memory,    10 * 1024 * 1024} %% memory quota per segment
  ,{quota,       1}   %% quota frequency  
]).
