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

    %% Get absolute path to shipment
    {ok, Cwd} = file:get_cwd(),
    AbsShipment = filename:join(Cwd, ShipmentDir),

    %% Create a wrapper shell script with the shipment path baked in
    ScriptName = "todo",
    Script = io_lib:format(
        "#!/bin/sh\n"
        "set -e\n"
        "exec \"~s/entrypoint.sh\" run \"$@\"\n",
        [AbsShipment]),

    ok = file:write_file(ScriptName, Script),
    os:cmd("chmod +x " ++ ScriptName),

    io:format("Built executable wrapper: ~s~n", [ScriptName]),
    io:format("Shipment path: ~s~n", [AbsShipment]).
