#!/usr/bin/env escript
%% Build script that creates a wrapper for example_todo
%% Uses erlang-shipment to support NIF libraries like esqlite.

main(_) ->
    ShipmentDir = "build/erlang-shipment",

    %% Verify shipment exists
    case filelib:is_dir(ShipmentDir) of
        false ->
            io:format("Error: shipment directory not found. Run 'gleam export erlang-shipment' first.~n"),
            halt(1);
        true -> ok
    end,

    %% Create a wrapper shell script that delegates to the shipment entrypoint
    ScriptName = "todo",
    Script =
        "#!/bin/sh\n"
        "set -e\n"
        "SCRIPT_DIR=\"$(cd \"$(dirname \"$0\")\" && pwd)\"\n"
        "exec \"$SCRIPT_DIR/build/erlang-shipment/entrypoint.sh\" run \"$@\"\n",

    ok = file:write_file(ScriptName, Script),
    os:cmd("chmod +x " ++ ScriptName),

    io:format("Built executable wrapper: ~s~n", [ScriptName]),
    io:format("Uses erlang-shipment at: ~s~n", [ShipmentDir]).
