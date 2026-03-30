# CS505 Assignment 2 - Mini OLAP Engine

Starter scaffold for a simplified in-memory column-store with OLAP scan accelerators and compression.

## Team workflow
- Keep `main` stable.
- Create feature branches for each technique.
- Merge only after correctness tests pass.

## Recommended setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest
python experiments/run_experiments.py
```

## What is included right now
- Column / segment storage scaffold
- Synthetic dataset generator
- Predicate model
- Baseline equality and range scan
- Simple query executor for conjunctions
- Metrics helpers
- Starter tests

## Suggested next steps
1. Get the baseline tests passing.
2. Add zone maps.
3. Add bitmap indexes.
4. Add compression modules.
5. Add experiment reporting.
```
Repo layout:
```
cs505-olap-engine/
├─ README.md
├─ requirements.txt
├─ .gitignore
├─ src/
├─ experiments/
├─ tests/
└─ report/
```
