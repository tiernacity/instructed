/**
 * INV-* coverage reporter (Phase 9, D-0022 / step 8/8).
 *
 * Walks every `tests/conformance/test/*.test.ts`, scrapes annotations
 * of the form
 *
 *   // INV-FOO-NNN
 *   // INV-FOO-NNN: <free-form prose>
 *   // INV-FOO-NNN: dropped — see NG-XXXX
 *   // INV-FOO-NNN: deferred — see ML-NNNN
 *   // INV-FOO-NNN: above adapter line — see <ref>
 *
 * and classifies each INV-* identifier as:
 *
 *   covered      — at least one annotation present, no special suffix.
 *   deferred     — annotated `: deferred — see ML-NNNN`. Expected to
 *                  appear in a `test.skip(...)` slot (D-0024).
 *   dropped      — annotated `: dropped — see NG-NNNN`. The capability
 *                  is deliberately not provided in v1.
 *   above-line   — annotated `: above adapter line — see <ref>`.
 *                  Realised in SDK code; intentionally not in scope
 *                  for the conformance harness per D-0023.
 *   missing      — listed in the INV-* catalogue (this file) but
 *                  referenced by no test annotation. Exit non-zero
 *                  if any `missing` rows remain.
 *
 * The expected catalogue is hardcoded below, mirroring
 * `docs/invariants.md` Parts B–F. Drift between the two is a bug —
 * the failing case is "an invariant exists in docs but the reporter
 * doesn't know about it" (silently uncovered) or "the reporter
 * expects an invariant that doesn't exist" (false missing). Keep
 * them in sync by hand on each docs update.
 *
 * Usage:
 *   node coverage-report.ts
 *
 * Exit codes:
 *   0  every expected INV-* has an annotation (covered, deferred,
 *      dropped, or above-line).
 *   1  one or more expected INV-* is missing, OR an orphaned
 *      annotation (an INV-* found in tests but not in the
 *      catalogue) is present. Both indicate a sync bug.
 */

import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const TEST_DIR = join(__dirname, "test");

// -----------------------------------------------------------------------------
// Expected INV-* catalogue (mirrors docs/invariants.md Parts B–F).
// Keep in sync by hand. Each entry: identifier -> short label.
// -----------------------------------------------------------------------------

const CATALOGUE = {
  "Part B — Append": [
    "INV-APPEND-001",
    "INV-APPEND-002",
    "INV-APPEND-003",
    "INV-APPEND-004",
    "INV-APPEND-005",
    "INV-APPEND-006",
    "INV-APPEND-007",
    "INV-APPEND-010",
    "INV-APPEND-011",
    "INV-APPEND-012",
    "INV-APPEND-013",
    "INV-APPEND-014",
    "INV-APPEND-020",
    "INV-APPEND-021",
    "INV-APPEND-022", // [reference-only mechanism]
    "INV-APPEND-030",
    "INV-APPEND-040",
    "INV-APPEND-041",
  ],
  "Part C — Read": [
    "INV-READ-001",
    "INV-READ-002",
    "INV-READ-003",
    "INV-READ-004",
    "INV-READ-005",
    "INV-READ-006",
    "INV-READ-007",
    "INV-READ-008",
    "INV-READ-020",
  ],
  "Part D — Snapshots": [
    "INV-SNAP-001",
    "INV-SNAP-002",
    "INV-SNAP-003",
    "INV-SNAP-004",
    "INV-SNAP-005",
    "INV-SNAP-006",
  ],
  "Part E — Subscriptions (transient)": [
    "INV-SUB-T-001",
    "INV-SUB-T-002",
    "INV-SUB-T-003",
    "INV-SUB-T-004",
    "INV-SUB-T-005",
  ],
  "Part E — Subscriptions (persistent)": [
    "INV-SUB-P-001",
    "INV-SUB-P-002",
    "INV-SUB-P-010",
    "INV-SUB-P-011", // [reference-only mechanism]
    "INV-SUB-P-012",
    "INV-SUB-P-020",
    "INV-SUB-P-021",
    "INV-SUB-P-030",
    "INV-SUB-P-031",
    "INV-SUB-P-032",
    "INV-SUB-P-033",
    "INV-SUB-P-034",
    // INV-SUB-P-040/041/042 omitted: sharded routing is unbuilt; see
    // docs/maybe-later.md ML-0013.
    "INV-SUB-P-050", // above adapter line
    "INV-SUB-P-060",
    "INV-SUB-P-061",
    "INV-SUB-P-062",
  ],
  "Part E — Subscriptions (work queue, SUB-A)": [
    "INV-SUB-W-001",
    "INV-SUB-W-002",
    "INV-SUB-W-003", // [mechanism-only]
    "INV-SUB-W-010",
    "INV-SUB-W-011",
    "INV-SUB-W-012",
    "INV-SUB-W-013",
    "INV-SUB-W-020",
    "INV-SUB-W-021",
    "INV-SUB-W-022",
    "INV-SUB-W-030",
  ],
  "Part E — Catch-up predicate (SUB-A)": [
    "INV-SUB-CATCHUP-001",
  ],
  "Part F — Cross-cutting": [
    "INV-META-001",
    "INV-META-010",
    "INV-META-011",
    "INV-STREAM-001",
    "INV-STREAM-002", // [reference-only mechanism]
    "INV-STREAM-003",
    "INV-LINK-001", // dropped
    "INV-DELETE-001", // dropped
  ],
} as const satisfies Record<string, readonly string[]>;

// -----------------------------------------------------------------------------
// Scrape annotations from test files
// -----------------------------------------------------------------------------

type Category = "covered" | "deferred" | "dropped" | "above-line";

interface Annotation {
  inv: string;
  category: Category;
  suffix: string; // raw suffix text after the INV-id, for diagnostics
  file: string;
  line: number;
}

/**
 * A comment line is a candidate annotation line if it starts (after
 * leading whitespace) with `//`. The annotation is then any
 * `INV-FOO-NNN` substring inside it. Multiple identifiers on the
 * same line all receive the same suffix.
 *
 * The suffix is taken from the text *after the last* INV-identifier
 * on the line. A leading `[parenthetical]` (e.g. `[reference-only
 * mechanism]`) is stripped, then an optional `:`, then whitespace.
 * What remains is the classification suffix.
 */
// Only treat a `//` comment line as an annotation if its body STARTS
// with `INV-`. This excludes narrative mentions (`// This is
// INV-FOO-NNN's weaker companion`, `// Dropped invariants (INV-X,
// INV-Y)`) which would otherwise produce false-covered classifications.
const COMMENT_REGEX = /^\s*\/\/\s*(INV-.*)$/;
const INV_REGEX = /INV-[A-Z]+(?:-[A-Z]+)?-\d{3}/g;

function classify(suffix: string): Category {
  const s = suffix.toLowerCase().trim();
  if (s.startsWith("dropped")) return "dropped";
  if (s.startsWith("deferred")) return "deferred";
  if (s.startsWith("above adapter line") || s.startsWith("above-adapter-line")) {
    return "above-line";
  }
  return "covered";
}

function scrapeFile(path: string): Annotation[] {
  const out: Annotation[] = [];
  const lines = readFileSync(path, "utf-8").split("\n");
  for (let i = 0; i < lines.length; i++) {
    const c = COMMENT_REGEX.exec(lines[i]);
    if (!c) continue;
    const body = c[1];
    const matches = [...body.matchAll(INV_REGEX)];
    if (matches.length === 0) continue;
    const last = matches[matches.length - 1];
    let suffix = body.slice((last.index ?? 0) + last[0].length);
    // Strip an optional `[parenthetical]`, then an optional `:`, then ws.
    suffix = suffix
      .replace(/^\s*\[[^\]]*\]/, "")
      .replace(/^\s*:\s*/, "")
      .trim();
    const category = classify(suffix);
    for (const m of matches) {
      out.push({
        inv: m[0],
        category,
        suffix,
        file: path,
        line: i + 1,
      });
    }
  }
  return out;
}

function scrapeAll(): Annotation[] {
  const entries = readdirSync(TEST_DIR, { withFileTypes: true });
  const files = entries
    .filter((e) => e.isFile() && /\.test\.ts$/.test(e.name))
    .map((e) => join(TEST_DIR, e.name))
    .sort();
  return files.flatMap(scrapeFile);
}

// -----------------------------------------------------------------------------
// Render
// -----------------------------------------------------------------------------

const SYMBOLS: Record<Category | "missing", string> = {
  covered: "✓",
  deferred: "⏸",
  dropped: "⊘",
  "above-line": "↑",
  missing: "✗",
};

const LABELS: Record<Category | "missing", string> = {
  covered: "covered",
  deferred: "deferred",
  dropped: "dropped",
  "above-line": "above-line",
  missing: "MISSING",
};

function pad(s: string, n: number): string {
  return s.length >= n ? s : s + " ".repeat(n - s.length);
}

function main(): number {
  const annotations = scrapeAll();
  const byInv = new Map<string, Annotation[]>();
  for (const a of annotations) {
    const arr = byInv.get(a.inv) ?? [];
    arr.push(a);
    byInv.set(a.inv, arr);
  }

  const knownIdentifiers = new Set<string>(Object.values(CATALOGUE).flat());

  // Detect orphaned annotations (INV-* found in tests but not in catalogue).
  const orphans: Annotation[] = [];
  for (const a of annotations) {
    if (!knownIdentifiers.has(a.inv)) orphans.push(a);
  }

  // Render per Part.
  console.log("\nINV-* coverage matrix");
  console.log("=====================\n");

  const totals: Record<Category | "missing", number> = {
    covered: 0,
    deferred: 0,
    dropped: 0,
    "above-line": 0,
    missing: 0,
  };

  const missing: string[] = [];

  for (const [partName, invs] of Object.entries(CATALOGUE)) {
    console.log(partName);
    console.log("-".repeat(partName.length));
    for (const inv of invs) {
      const anns = byInv.get(inv) ?? [];
      let category: Category | "missing";
      let detail = "";
      if (anns.length === 0) {
        category = "missing";
        missing.push(inv);
      } else {
        // If any annotation is "covered", that wins. Otherwise the
        // first annotation's category wins (deferred / dropped /
        // above-line should be consistent across all sites of the
        // same INV-* — we don't enforce that here, just report).
        const covering = anns.find((a) => a.category === "covered");
        if (covering) {
          category = "covered";
          detail = `${anns.length} site${anns.length === 1 ? "" : "s"}`;
        } else {
          category = anns[0].category;
          detail = anns[0].suffix || "(no suffix)";
        }
      }
      totals[category]++;
      const sym = SYMBOLS[category];
      console.log(`  ${sym} ${pad(inv, 22)} ${pad(LABELS[category], 12)} ${detail}`);
    }
    console.log("");
  }

  console.log("Totals");
  console.log("------");
  for (const cat of ["covered", "deferred", "dropped", "above-line", "missing"] as const) {
    console.log(`  ${SYMBOLS[cat]} ${pad(LABELS[cat], 12)} ${totals[cat]}`);
  }
  console.log("");

  let exit = 0;
  if (missing.length > 0) {
    console.error(`MISSING: ${missing.length} expected INV-* have no annotation:`);
    for (const inv of missing) console.error(`  - ${inv}`);
    exit = 1;
  }
  if (orphans.length > 0) {
    console.error(
      `ORPHANED: ${orphans.length} annotation${orphans.length === 1 ? "" : "s"} reference INV-* not in the catalogue:`,
    );
    for (const o of orphans) {
      console.error(`  - ${o.inv} (${o.file}:${o.line})`);
    }
    exit = 1;
  }
  if (exit === 0) {
    console.log("All expected INV-* identifiers are accounted for.");
  }
  return exit;
}

process.exit(main());
