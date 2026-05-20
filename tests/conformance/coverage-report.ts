/**
 * INV-* coverage reporter (Phase 9, D-0022).
 *
 * Walks every `tests/conformance/test/*.test.ts` file, scrapes
 * `// INV-FOO-NNN[: …]` annotations from the lines preceding each
 * `test(...)` / `test.skip(...)` declaration, and renders a matrix
 * of:
 *
 *   covered   — at least one non-skipped test references the INV.
 *   deferred  — only skipped tests reference it (ML-0001 partitioning
 *               is the canonical case).
 *   dropped   — annotated explicitly as `// INV-FOO-NNN: dropped —
 *               see NG-XXXX` and not exercised. The annotation must
 *               cite a non-goal entry for the reporter to accept the
 *               drop.
 *   above-line — annotated `// INV-FOO-NNN: above adapter line — see
 *               <ref>`. Realised in SDK code (e.g. INV-SUB-P-050
 *               selectors per ML-0003); intentionally not in scope
 *               for the conformance harness per D-0023.
 *   missing   — listed in the INV-* catalogue (`docs/invariants.md`)
 *               but referenced by no test. The reporter exits
 *               non-zero if any `missing` row remains.
 *
 * This file is a placeholder for step 1/8. It is not yet wired into
 * `npm test`; the renderer body lands in step 8/8 once enough cases
 * exist to make the matrix meaningful.
 *
 * The annotation grammar (final, frozen now so each step can use it):
 *
 *   // INV-APPEND-013
 *   // INV-APPEND-013: <free-form prose>
 *   // INV-APPEND-013: dropped — see NG-0012
 *   // INV-APPEND-013: deferred — see ML-0001
 *   // INV-APPEND-013: above adapter line — see ML-0003
 *
 * Multiple INV-* lines may precede a single `test(...)` block; all
 * are recorded against that test.
 */

function main(): void {
  // Intentionally a no-op for step 1/8. See file-level docstring.
  console.log(
    "coverage-report: placeholder — implementation lands in step 8/8",
  );
}

main();
