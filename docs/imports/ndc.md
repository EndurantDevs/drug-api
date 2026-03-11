# NDC Import

## Purpose
Imports OpenFDA NDC data and publishes normalized product and package tables used by search and RxNorm-linked lookups.

## Source Websites
- OpenFDA: <https://open.fda.gov/>
- FDA download catalog: <https://api.fda.gov/download.json>

## Start Command
```bash
python main.py start ndc
```

## Worker
```bash
python main.py worker process.NDC --burst
```

## Main Outputs
- `rx_data.product`
- `rx_data.package`

## Notes
- The import creates dated staging tables, then swaps them into the live schema.
- `product.rxnorm_ids` is populated from `openfda.rxcui` during this import.
- Product and package tables are published together.
