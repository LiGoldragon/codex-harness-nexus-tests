# Codex rollout fixtures

Each recorded fixture keeps the outer `event_msg` envelope and only the fields read by the ruled rollout observer. Every other field is omitted.

`rollout-2026-11-15T14-36-21-01a1e0ec-dc79-70c0-aab1-810f454162ca.jsonl` supplies `task-started` at line 2, `token-count-null` at line 11, `turn-aborted` at line 13 and `token-count` at line 25.

`rollout-2026-11-14T10-34-31-01a1dae9-17a1-79e1-87b2-5056be617f2f.jsonl` supplies `task-complete-null` at line 823.

`task-complete-response-synthetic` has a synthetic response. A non-null response is present in the first rollout at line 26; its original content is omitted. The fixture tests the two-event sequence `Responded`, then `TurnEnded`.

These are isolated source rows, not a contiguous session transcript. Tests that combine them into a sequence must identify that sequence as synthetic.
