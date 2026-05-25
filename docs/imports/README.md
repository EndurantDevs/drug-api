# Import Processes

`drug-api` has public data import pipelines for FDA drug reference data and derived condition mappings.

## Imports at a Glance

| Import | Start command | Worker | Purpose |
| --- | --- | --- | --- |
| NDC | `python main.py start ndc` | `python main.py worker process.NDC --burst` | Import OpenFDA NDC products and packages |
| Label | `python main.py start label` | `python main.py worker process.Labeling --burst` | Import OpenFDA drug labels |
| Drug indications | `python main.py start drug-indications` | none | Derive drug-to-condition evidence from local DailyMed/OpenFDA labels |

## Per-import Documentation
- [NDC import](./ndc.md)
- [Label import](./label.md)
- [Drug indications import](./drug-indications.md)
