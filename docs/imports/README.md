# Import Processes

`drug-api` has two public data import pipelines.

## Imports at a Glance

| Import | Start command | Worker | Purpose |
| --- | --- | --- | --- |
| NDC | `python main.py start ndc` | `python main.py worker process.NDC --burst` | Import OpenFDA NDC products and packages |
| Label | `python main.py start label` | `python main.py worker process.Labeling --burst` | Import OpenFDA drug labels |

## Per-import Documentation
- [NDC import](./ndc.md)
- [Label import](./label.md)
