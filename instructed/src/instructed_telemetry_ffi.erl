%% Erlang FFI for instructed/telemetry.gleam
%%
%% Provides:
%%   - A persistent-term-backed Gleam handler for receiving TelemetryEvent values
%%   - Optional Erlang :telemetry.execute/3 emission (no-op if :telemetry not available)
%%   - Monotonic time in nanoseconds for duration measurements

-module(instructed_telemetry_ffi).
-export([set_handler/1, clear_handler/0, emit/1, monotonic_time_ns/0]).

-define(HANDLER_KEY, instructed_telemetry_gleam_handler).

%% Set a Gleam function as the telemetry handler.
%% Stored in persistent_term for fast process-safe access.
set_handler(Handler) ->
    persistent_term:put(?HANDLER_KEY, {handler, Handler}),
    nil.

%% Clear the Gleam telemetry handler.
clear_handler() ->
    persistent_term:erase(?HANDLER_KEY),
    nil.

%% Emit a TelemetryEvent.
%% 1. Calls the Gleam handler if one is registered.
%% 2. Attempts to call :telemetry.execute/3 — silently no-ops if not available.
emit(Event) ->
    % Call the Gleam handler if registered
    case persistent_term:get(?HANDLER_KEY, undefined) of
        {handler, Handler} ->
            catch Handler(Event);
        _ ->
            ok
    end,
    % Attempt to emit via Erlang :telemetry (optional dependency)
    emit_via_telemetry(Event),
    nil.

%% Attempt :telemetry.execute/3, no-op on undef / missing dependency.
emit_via_telemetry(Event) ->
    EventName = event_name(Event),
    Measurements = event_measurements(Event),
    Metadata = event_metadata(Event),
    catch telemetry:execute(EventName, Measurements, Metadata),
    ok.

%% Map a TelemetryEvent to a list-of-atoms event name for :telemetry.
event_name({command_dispatch_start, _CommandId, _StreamId, _SysTime}) ->
    [instructed, command, dispatch, start];
event_name({command_dispatch_stop, _CommandId, _StreamId, _Dur, _Count}) ->
    [instructed, command, dispatch, stop];
event_name({command_dispatch_exception, _CommandId, _StreamId, _Dur, _Error}) ->
    [instructed, command, dispatch, exception];
event_name({aggregate_execute_start, _StreamId, _SysTime}) ->
    [instructed, aggregate, execute, start];
event_name({aggregate_execute_stop, _StreamId, _Dur, _Count}) ->
    [instructed, aggregate, execute, stop];
event_name({aggregate_execute_exception, _StreamId, _Dur, _Error}) ->
    [instructed, aggregate, execute, exception];
event_name({event_handle_start, _Name, _Type, _Num, _SysTime}) ->
    [instructed, event, handle, start];
event_name({event_handle_stop, _Name, _Type, _Num, _Dur}) ->
    [instructed, event, handle, stop];
event_name({event_handle_exception, _Name, _Type, _Num, _Dur, _Error}) ->
    [instructed, event, handle, exception];
event_name({process_manager_handle_start, _Name, _Type, _Num, _SysTime}) ->
    [instructed, process_manager, handle, start];
event_name({process_manager_handle_stop, _Name, _Type, _Num, _Dur, _Cmds}) ->
    [instructed, process_manager, handle, stop];
event_name({process_manager_handle_exception, _Name, _Type, _Num, _Dur, _Error}) ->
    [instructed, process_manager, handle, exception];
event_name(_) ->
    [instructed, unknown].

%% Extract numeric measurements for :telemetry.
event_measurements({command_dispatch_start, _, _, SysTime}) ->
    #{system_time => SysTime};
event_measurements({command_dispatch_stop, _, _, Dur, Count}) ->
    #{duration => Dur, event_count => Count};
event_measurements({command_dispatch_exception, _, _, Dur, _}) ->
    #{duration => Dur};
event_measurements({aggregate_execute_start, _, SysTime}) ->
    #{system_time => SysTime};
event_measurements({aggregate_execute_stop, _, Dur, Count}) ->
    #{duration => Dur, event_count => Count};
event_measurements({aggregate_execute_exception, _, Dur, _}) ->
    #{duration => Dur};
event_measurements({event_handle_start, _, _, _, SysTime}) ->
    #{system_time => SysTime};
event_measurements({event_handle_stop, _, _, _, Dur}) ->
    #{duration => Dur};
event_measurements({event_handle_exception, _, _, _, Dur, _}) ->
    #{duration => Dur};
event_measurements({process_manager_handle_start, _, _, _, SysTime}) ->
    #{system_time => SysTime};
event_measurements({process_manager_handle_stop, _, _, _, Dur, Cmds}) ->
    #{duration => Dur, commands_dispatched => Cmds};
event_measurements({process_manager_handle_exception, _, _, _, Dur, _}) ->
    #{duration => Dur};
event_measurements(_) ->
    #{}.

%% Extract metadata map for :telemetry.
event_metadata({command_dispatch_start, CmdId, StreamId, _}) ->
    #{command_id => CmdId, aggregate_stream_id => StreamId};
event_metadata({command_dispatch_stop, CmdId, StreamId, _, _}) ->
    #{command_id => CmdId, aggregate_stream_id => StreamId};
event_metadata({command_dispatch_exception, CmdId, StreamId, _, Error}) ->
    #{command_id => CmdId, aggregate_stream_id => StreamId, error => Error};
event_metadata({aggregate_execute_start, StreamId, _}) ->
    #{aggregate_stream_id => StreamId};
event_metadata({aggregate_execute_stop, StreamId, _, _}) ->
    #{aggregate_stream_id => StreamId};
event_metadata({aggregate_execute_exception, StreamId, _, Error}) ->
    #{aggregate_stream_id => StreamId, error => Error};
event_metadata({event_handle_start, Name, Type, Num, _}) ->
    #{handler_name => Name, event_type => Type, event_number => Num};
event_metadata({event_handle_stop, Name, Type, Num, _}) ->
    #{handler_name => Name, event_type => Type, event_number => Num};
event_metadata({event_handle_exception, Name, Type, Num, _, Error}) ->
    #{handler_name => Name, event_type => Type, event_number => Num, error => Error};
event_metadata({process_manager_handle_start, PmName, Type, Num, _}) ->
    #{pm_name => PmName, event_type => Type, event_number => Num};
event_metadata({process_manager_handle_stop, PmName, Type, Num, _, _}) ->
    #{pm_name => PmName, event_type => Type, event_number => Num};
event_metadata({process_manager_handle_exception, PmName, Type, Num, _, Error}) ->
    #{pm_name => PmName, event_type => Type, event_number => Num, error => Error};
event_metadata(_) ->
    #{}.

%% Monotonic time in nanoseconds.
monotonic_time_ns() ->
    erlang:monotonic_time(nanosecond).
