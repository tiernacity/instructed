// CLI output formatting. Presentation lives here, never in core. Commands take
// core data and render it either as a human-readable table or as JSON
// (--json), so the tool is scriptable.

import { Table } from "@cliffy/table";

export function printJson(value: unknown): void {
  console.log(
    JSON.stringify(value, (_k, v) => (typeof v === "bigint" ? v.toString() : v), 2),
  );
}

// Render an array of records as a bordered table with the given column order
// and headers. Values are stringified; null/undefined render as a dash.
export function printTable(
  headers: string[],
  rows: Array<Array<string | number | null | undefined>>,
): void {
  if (rows.length === 0) {
    console.log("(none)");
    return;
  }
  const body = rows.map((r) => r.map(cell));
  new Table()
    .header(headers)
    .body(body)
    .border(true)
    .render();
}

function cell(value: string | number | null | undefined): string {
  if (value === null || value === undefined) return "-";
  return String(value);
}

// Render a flat key/value block (for single-record views and status).
export function printKeyValue(pairs: Array<[string, string | number | null]>): void {
  const width = Math.max(...pairs.map(([k]) => k.length));
  for (const [k, v] of pairs) {
    console.log(`${k.padEnd(width)} : ${v === null ? "-" : v}`);
  }
}
