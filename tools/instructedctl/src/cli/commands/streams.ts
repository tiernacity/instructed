// `streams` command group: list / get / read. Plus the top-level `all` group
// for reading the global $all stream.

import { Command } from "@cliffy/command";
import { getStream, listStreams, readAll, readStream } from "../../core/index.ts";
import { action, type GlobalOptions, runWith } from "../options.ts";
import { printJson, printKeyValue, printTable } from "../output.ts";
import type { EventRecord } from "../../core/index.ts";

const DEFAULT_COUNT = 20;

function eventsTable(events: EventRecord[]): void {
  printTable(
    ["EVENT#", "STREAM", "VER", "TYPE", "CREATED"],
    events.map((e) => [
      e.eventNumber,
      e.streamUuid,
      e.streamVersion,
      e.eventType,
      e.createdAt.toISOString(),
    ]),
  );
}

type ReadOpts = GlobalOptions & { from: number; count: number };

export function streamsCommand() {
  return new Command()
    .description("Inspect streams and read events")
    .default("list")
    .command(
      "list",
      new Command()
        .description("List streams with head version and event count")
        .alias("ls")
        .action((opts) =>
          action(async () => {
            const g = opts as unknown as GlobalOptions;
            const streams = await runWith(g, listStreams);
            if (g.json) printJson(streams);
            else {
              printTable(
                ["STREAM", "HEAD", "EVENTS"],
                streams.map((s) => [s.streamUuid, s.head, s.eventCount]),
              );
            }
          })
        ),
    )
    .command(
      "get",
      new Command()
        .description("Show one stream by uuid")
        .arguments("<uuid:string>")
        .action((opts, uuid) =>
          action(async () => {
            const g = opts as unknown as GlobalOptions;
            const stream = await runWith(g, (db) => getStream(db, uuid));
            if (g.json) {
              printJson(stream);
              return;
            }
            if (stream === null) {
              console.error(`Stream '${uuid}' not found`);
              Deno.exit(1);
            }
            printKeyValue([
              ["stream", stream.streamUuid],
              ["head", stream.head],
              ["events", stream.eventCount],
            ]);
          })
        ),
    )
    .command(
      "read",
      new Command()
        .description("Read a range of events from a stream")
        .arguments("<uuid:string>")
        .option("--from <version:integer>", "Start at this stream version", {
          default: 0,
        })
        .option("--count <n:integer>", "Maximum events to return", {
          default: DEFAULT_COUNT,
        })
        .action((opts, uuid) =>
          action(async () => {
            const o = opts as unknown as ReadOpts;
            const events = await runWith(o, (db) =>
              readStream(db, { streamUuid: uuid, from: o.from, count: o.count }));
            if (o.json) {
              printJson(events);
            } else eventsTable(events);
          })
        ),
    );
}

export function allCommand() {
  return new Command()
    .description("Read the global $all stream")
    .default("read")
    .command(
      "read",
      new Command()
        .description("Read a range of events from $all")
        .option("--from <eventNumber:integer>", "Start at this event_number", {
          default: 0,
        })
        .option("--count <n:integer>", "Maximum events to return", {
          default: DEFAULT_COUNT,
        })
        .action((opts) =>
          action(async () => {
            const o = opts as unknown as ReadOpts;
            const events = await runWith(o, (db) =>
              readAll(db, { from: o.from, count: o.count }));
            if (o.json) {
              printJson(events);
            } else eventsTable(events);
          })
        ),
    );
}
