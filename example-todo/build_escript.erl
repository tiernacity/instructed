#!/usr/bin/env escript
%% Build script that creates a self-contained executable wrapper for example_todo
%% Uses erlang-shipment (not escript) to support NIF libraries like esqlite.

main(_) ->
    ShipmentDir = "build/erlang-shipment",

    %% Verify shipment exists
    case filelib:is_dir(ShipmentDir) of
        false ->
            io:format("Error: shipment directory not found. Run 'gleam export erlang-shipment' first.~n"),
            halt(1);
        true -> ok
    end,

    %% Create a wrapper shell script that runs the shipment
    ScriptName = "todo",
    Script =
        "#!/bin/sh\n"
        "set -e\n"
        "SCRIPT_DIR=\"$(cd \"$(dirname \"$0\")\" && pwd)\"\n"
        "SHIPMENT_DIR=\"$SCRIPT_DIR/build/erlang-shipment\"\n"
        "\n"
        "# Build code paths for all ebin directories\n"
        "PA_ARGS=\"\"\n"
        "for dir in \"$SHIPMENT_DIR\"/*/ebin; do\n"
        "  PA_ARGS=\"$PA_ARGS -pa $dir\"\n"
        "done\n"
        "\n"
        "# Build NIF paths - add priv dirs to ERL_LIBS\n"
        "export ERL_LIBS=\"$SHIPMENT_DIR\"\n"
        "\n"
        "exec erl -noshell $PA_ARGS -eval \"'example_todo@@main':run(example_todo)\" -s init stop -- \"$@\"\n",

    ok = file:write_file(ScriptName, Script),
    os:cmd("chmod +x " ++ ScriptName),

    io:format("Built executable wrapper: ~s~n", [ScriptName]),
    io:format("Uses erlang-shipment at: ~s~n", [ShipmentDir]).
