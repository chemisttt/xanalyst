# Evals

Held-out checks that gate classifier changes. Labels are human. Model outputs from `ct_digest_items` are not gold.

## CT digest `golden_v1`

`evals/ct_digest/golden_v1.json` — 18 cases covering the production buckets (`early_signals`, calendar 24h/3d/7d, `state_reconcile`, `paid_hype`) plus include/drop and two injection cases (`g16` semantic, `g18` XML breakout).

Pass = predicted **bucket and included** both match gold. Do not delete failing injection cases to raise the number.

```bash
python evals/ct_digest/run.py          # schema + taxonomy + escape, no API
python evals/ct_digest/run.py --llm    # classify_batch; needs ANTHROPIC_API_KEY
```

`--llm` exits 1 if `g16`/`g18` miss. Offline mode is what CI/pytest runs.
