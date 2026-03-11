# Label Import

## Purpose
Imports OpenFDA drug label records and publishes the normalized `label` table used by label and SPL-oriented API responses.

## Source Websites
- OpenFDA: <https://open.fda.gov/>
- FDA download catalog: <https://api.fda.gov/download.json>

## Start Command
```bash
python main.py start label
```

## Worker
```bash
python main.py worker process.Labeling --burst
```

## Main Outputs
- `rx_data.label`

## Notes
- The label import is independent from the NDC import.
- `label.set_id` is stored and can be used for DailyMed-oriented workflows.
- Publish uses the same rebuild-and-swap pattern as the NDC import.
