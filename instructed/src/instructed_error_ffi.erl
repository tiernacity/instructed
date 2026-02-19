-module(instructed_error_ffi).
-export([random_jitter_ms/0]).

%% Random jitter between 0 and 1000 milliseconds.
%% Uses rand:uniform/1 which returns 1..N, subtract 1 for 0..999.
random_jitter_ms() ->
    rand:uniform(1000) - 1.
