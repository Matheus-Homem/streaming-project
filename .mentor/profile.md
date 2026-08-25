# Profile

<!-- Created by /mentor-sync on first run. Config for this project's mentoring.
     Everything here is English; see references/knowledge-model.md. -->

## Config

- `spec_artifacts`: .specs/features/*/*.md
- `active_feature`: interface-layout
- `gemini_notebook_id`: Streaming
- `gemini_notebook_transport`: manual
- `snapshot_max_age_days`: 14

<!-- gemini_notebook_transport: `manual` (paste or point at an exported ledger) or `mcp:<server>`.
     `manual` always works and is the default; see references/gemini-notebook-contract.md.
     /mentor-sync probes for a known MCP once, right after this file is created —
     never on an ordinary run. Confirmed with you before it's ever written here.
     Changed your mind, or set one up later? `/mentor-sync --detect-mcp` re-checks
     on demand. Editing this line by hand works too.
     snapshot_max_age_days: after this, /mentor-map warns that the snapshot is stale.
     It warns and continues — it never blocks. -->

## Notes

<!-- Anything about how this project should be mentored that does not fit above.
     Free text. -->

- MCP for the "Streaming" Gemini Notebook was tried and is not working (2026-08-21);
  transport stays `manual` until the user reports it working. Do not probe again
  automatically — only on `--detect-mcp` per the command's own rule.
- Node ids in `nodes.md` are English (e.g. `StreamProcessing.ApacheFlink.Windowing.TumblingWindowViaTvf`),
  not Portuguese, despite `references/knowledge-model.md`'s own examples being Portuguese - user
  correction, 2026-08-24, consistent with `CLAUDE.md`'s "identifiers: English" rule. Propose new
  canonical ids in English going forward, still confirming with the user before writing, per usual.
  The `aliases` column stays whatever the user actually types (often Portuguese) - only the id itself
  is translated.
