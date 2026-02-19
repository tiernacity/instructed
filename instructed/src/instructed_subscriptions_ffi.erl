-module(instructed_subscriptions_ffi).
-export([monotonic_time_ns/0]).

%% Monotonic time in nanoseconds for TTL tracking.
monotonic_time_ns() ->
    erlang:monotonic_time(nanosecond).
