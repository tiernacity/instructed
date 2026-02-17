#!/usr/bin/env escript
%% Build script that creates a self-contained escript binary for example_todo

main(_) ->
    ShipmentDir = "build/erlang-shipment",

    %% Collect all .beam and .app files from ebin directories
    EbinDirs = filelib:wildcard(ShipmentDir ++ "/*/ebin"),
    AllShipmentFiles = lists:flatmap(
        fun(Dir) ->
            lists:map(
                fun(F) ->
                    {ok, Bin} = file:read_file(F),
                    {filename:basename(F), Bin}
                end,
                filelib:wildcard(Dir ++ "/*.beam") ++ filelib:wildcard(Dir ++ "/*.app")
            )
        end,
        EbinDirs
    ),

    %% Create wrapper module that bridges escript main/1 to Gleam's run/1
    WrapperSrc = "/tmp/escript_entry.erl",
    ok = file:write_file(WrapperSrc,
        "-module(escript_entry).\n"
        "-export([main/1]).\n"
        "main(_Args) ->\n"
        "    'example_todo@@main':run(example_todo).\n"),
    {ok, escript_entry, WrapperBeam} = compile:file(WrapperSrc, [binary]),

    AllFiles = [{"escript_entry.beam", WrapperBeam} | AllShipmentFiles],

    %% Create the escript
    EscriptName = "todo",
    ok = escript:create(EscriptName, [
        shebang,
        {emu_args, "-escript main escript_entry"},
        {archive, AllFiles, []}
    ]),
    os:cmd("chmod +x " ++ EscriptName),

    io:format("Built executable: ~s~n", [EscriptName]),
    io:format("Included ~b files (~.1f KB)~n", [
        length(AllFiles),
        filelib:file_size(EscriptName) / 1024.0
    ]).
