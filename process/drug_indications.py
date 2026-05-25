import asyncio
import datetime
import hashlib
import os
import re

import asyncpg

from db.connection import init_db
from db.models import DrugConditionEvidence, DrugTreatmentMapping, Label, Product, db
from process.ext.utils import make_class, push_objects, print_time_info

NLM_ATTRIBUTION = (
    "This product uses publicly available data from the U.S. National Library of Medicine (NLM), "
    "National Institutes of Health, Department of Health and Human Services; NLM is not responsible "
    "for the product and does not endorse or recommend this or any other product."
)

CONDITION_LEXICON = [
    ("ICD10CM", "I10", "Essential hypertension", ("hypertension", "high blood pressure")),
    ("ICD10CM", "E11.9", "Type 2 diabetes mellitus without complications", ("type 2 diabetes", "diabetes mellitus")),
    ("ICD10CM", "F32.9", "Major depressive disorder, single episode, unspecified", ("major depressive disorder", "depression")),
    ("ICD10CM", "F41.9", "Anxiety disorder, unspecified", ("anxiety disorder", "anxiety")),
    ("ICD10CM", "J45.909", "Unspecified asthma, uncomplicated", ("asthma",)),
    ("ICD10CM", "G43.909", "Migraine, unspecified, not intractable, without status migrainosus", ("migraine",)),
    ("ICD10CM", "K21.9", "Gastro-esophageal reflux disease without esophagitis", ("gastroesophageal reflux", "gerd")),
]


def _schema():
    return os.getenv('DB_SCHEMA') or 'rx_data'


def _import_date(raw=None):
    raw = raw or os.getenv('HLTHPRT_DRUG_INDICATIONS_IMPORT_ID')
    if raw:
        cleaned = ''.join(ch for ch in str(raw) if ch.isalnum())
        if cleaned:
            return cleaned[:32]
    return datetime.datetime.utcnow().strftime('%Y%m%d')


def _text_excerpt(text, needle):
    clean = re.sub(r'\s+', ' ', str(text or '')).strip()
    if not clean:
        return ''
    idx = clean.lower().find(needle.lower())
    if idx < 0:
        return clean[:500]
    start = max(0, idx - 180)
    end = min(len(clean), idx + len(needle) + 320)
    return clean[start:end]


def _hash_id(*parts):
    payload = '|'.join(str(part or '') for part in parts)
    return hashlib.sha1(payload.encode('utf-8')).hexdigest()


def _label_ndcs(label):
    product_ndc = list(getattr(label, 'product_ndc', None) or [])
    package_ndc = list(getattr(label, 'package_ndc', None) or [])
    return product_ndc, package_ndc


def _rxnorm_ids_for_label(product_ndc, rxnorm_by_product):
    rxnorm_ids = set()
    for ndc in product_ndc:
        rxnorm_ids.update(rxnorm_by_product.get(ndc) or [])
    return sorted(rxnorm_ids)


def _matches(label, rxnorm_by_product):
    text = getattr(label, 'indications_and_usage', None) or ''
    text_lower = text.lower()
    product_ndc, package_ndc = _label_ndcs(label)
    rxnorm_ids = _rxnorm_ids_for_label(product_ndc, rxnorm_by_product)
    now = datetime.datetime.utcnow()
    for system, code, display, terms in CONDITION_LEXICON:
        matched_term = next((term for term in terms if term in text_lower), None)
        if not matched_term:
            continue
        evidence_id = _hash_id(getattr(label, 'set_id', None), getattr(label, 'id', None), system, code, matched_term)
        yield {
            'evidence_id': evidence_id,
            'set_id': getattr(label, 'set_id', None),
            'label_id': getattr(label, 'id', None),
            'product_ndc': product_ndc,
            'package_ndc': package_ndc,
            'rxnorm_ids': rxnorm_ids,
            'condition_system': system,
            'condition_code': code,
            'condition_display': display,
            'evidence_text': _text_excerpt(text, matched_term),
            'evidence_source': 'dailymed_label_indications_and_usage',
            'confidence': 0.75,
            'source_attribution': NLM_ATTRIBUTION,
            'imported_at': now,
        }
        yield {
            'mapping_id': evidence_id,
            'set_id': getattr(label, 'set_id', None),
            'product_ndc': product_ndc,
            'package_ndc': package_ndc,
            'rxnorm_ids': rxnorm_ids,
            'treatment_system': 'NDC',
            'treatment_code': product_ndc[0] if product_ndc else None,
            'treatment_display': None,
            'condition_system': system,
            'condition_code': code,
            'condition_display': display,
            'source': 'dailymed_label_indications_and_usage',
            'source_attribution': NLM_ATTRIBUTION,
            'imported_at': now,
        }


async def _load_rxnorm_condition_map():
    clinical_schema = os.getenv('HLTHPRT_CLINICAL_DB_SCHEMA') or 'mrf'
    clinical_database = os.getenv('HLTHPRT_CLINICAL_DB_DATABASE') or os.getenv('HLTHPRT_DB_DATABASE') or 'healthporta'
    clinical_host = os.getenv('HLTHPRT_CLINICAL_DB_HOST') or os.getenv('HLTHPRT_DB_HOST') or '127.0.0.1'
    clinical_port = int(os.getenv('HLTHPRT_CLINICAL_DB_PORT') or '5440')
    clinical_user = os.getenv('HLTHPRT_CLINICAL_DB_USER') or os.getenv('HLTHPRT_DB_USER') or 'postgres'
    clinical_password = os.getenv('HLTHPRT_CLINICAL_DB_PASSWORD') or os.getenv('HLTHPRT_DB_PASSWORD') or ''
    try:
        connection = await asyncpg.connect(
            host=clinical_host,
            port=clinical_port,
            user=clinical_user,
            password=clinical_password,
            database=clinical_database,
        )
        try:
            rows = await connection.fetch(
                f"""
                SELECT r.from_code AS rxcui,
                       r.to_system AS condition_system,
                       r.to_code AS condition_code,
                       c.display_name AS condition_display,
                       r.relationship AS relationship,
                       r.source_attribution AS source_attribution
                  FROM {clinical_schema}.clinical_code_relationship r
                  LEFT JOIN {clinical_schema}.clinical_code_catalog c
                    ON c.code_system = r.to_system
                   AND c.code = r.to_code
                 WHERE r.from_system = 'RXNORM'
                   AND r.relationship IN ('may_treat', 'may_prevent')
                   AND c.code_type = 'condition'
                """
            )
        finally:
            await connection.close()
    except Exception:
        try:
            rows = await db.all(
            f"""
            SELECT r.from_code AS rxcui,
                   r.to_system AS condition_system,
                   r.to_code AS condition_code,
                   c.display_name AS condition_display,
                   r.relationship AS relationship,
                   r.source_attribution AS source_attribution
              FROM {clinical_schema}.clinical_code_relationship r
              LEFT JOIN {clinical_schema}.clinical_code_catalog c
                ON c.code_system = r.to_system
               AND c.code = r.to_code
             WHERE r.from_system = 'RXNORM'
               AND r.relationship IN ('may_treat', 'may_prevent')
               AND c.code_type = 'condition'
            """
            )
        except Exception as exc:
            print(f"Clinical terminology relationship lookup skipped: {exc}")
            return {}
    result = {}
    for row in rows:
        if hasattr(row, "keys"):
            rxcui = row["rxcui"]
            system = row["condition_system"]
            code = row["condition_code"]
            display = row["condition_display"]
            relationship = row["relationship"]
            attribution = row["source_attribution"]
        else:
            rxcui, system, code, display, relationship, attribution = row
        result.setdefault(str(rxcui), []).append({
            'condition_system': system,
            'condition_code': code,
            'condition_display': display or code,
            'relationship': relationship,
            'source_attribution': attribution or NLM_ATTRIBUTION,
        })
    return result


def _relationship_matches(label, rxnorm_by_product, rxnorm_condition_map):
    product_ndc, package_ndc = _label_ndcs(label)
    rxnorm_ids = _rxnorm_ids_for_label(product_ndc, rxnorm_by_product)
    now = datetime.datetime.utcnow()
    for rxcui in rxnorm_ids:
        for item in rxnorm_condition_map.get(str(rxcui), []):
            evidence_id = _hash_id(getattr(label, 'set_id', None), getattr(label, 'id', None), rxcui, item['condition_system'], item['condition_code'])
            yield {
                'evidence_id': evidence_id,
                'set_id': getattr(label, 'set_id', None),
                'label_id': getattr(label, 'id', None),
                'product_ndc': product_ndc,
                'package_ndc': package_ndc,
                'rxnorm_ids': rxnorm_ids,
                'condition_system': item['condition_system'],
                'condition_code': item['condition_code'],
                'condition_display': item['condition_display'],
                'evidence_text': f"RxNorm {rxcui} {item['relationship']} {item['condition_display']} via clinical terminology relationship.",
                'evidence_source': 'clinical_rxnorm_relationship',
                'confidence': 0.85,
                'source_attribution': item['source_attribution'],
                'imported_at': now,
            }
            yield {
                'mapping_id': evidence_id,
                'set_id': getattr(label, 'set_id', None),
                'product_ndc': product_ndc,
                'package_ndc': package_ndc,
                'rxnorm_ids': rxnorm_ids,
                'treatment_system': 'RXNORM',
                'treatment_code': str(rxcui),
                'treatment_display': None,
                'condition_system': item['condition_system'],
                'condition_code': item['condition_code'],
                'condition_display': item['condition_display'],
                'source': 'clinical_rxnorm_relationship',
                'source_attribution': item['source_attribution'],
                'imported_at': now,
            }


async def _create_indexes(schema, evidence_cls, mapping_cls, import_date):
    await db.status(
        f"CREATE UNIQUE INDEX idx_drug_condition_evidence_id_{import_date} "
        f"ON {schema}.{evidence_cls.__tablename__} (evidence_id);"
    )
    await db.status(
        f"CREATE INDEX idx_drug_condition_evidence_set_id_{import_date} "
        f"ON {schema}.{evidence_cls.__tablename__} (set_id);"
    )
    await db.status(
        f"CREATE INDEX idx_drug_condition_evidence_condition_{import_date} "
        f"ON {schema}.{evidence_cls.__tablename__} (condition_system, condition_code);"
    )
    await db.status(
        f"CREATE INDEX idx_drug_condition_evidence_product_ndc_{import_date} "
        f"ON {schema}.{evidence_cls.__tablename__} USING GIN(product_ndc);"
    )
    await db.status(
        f"CREATE UNIQUE INDEX idx_drug_treatment_mapping_id_{import_date} "
        f"ON {schema}.{mapping_cls.__tablename__} (mapping_id);"
    )
    await db.status(
        f"CREATE INDEX idx_drug_treatment_mapping_condition_{import_date} "
        f"ON {schema}.{mapping_cls.__tablename__} (condition_system, condition_code);"
    )
    await db.status(
        f"CREATE INDEX idx_drug_treatment_mapping_product_ndc_{import_date} "
        f"ON {schema}.{mapping_cls.__tablename__} USING GIN(product_ndc);"
    )


async def _swap(schema, import_date):
    for table in ['drug_condition_evidence', 'drug_treatment_mapping']:
        await db.status(f"DROP TABLE IF EXISTS {schema}.{table}_old;")
        await db.status(f"ALTER TABLE IF EXISTS {schema}.{table} RENAME TO {table}_old;")
        await db.status(f"ALTER TABLE IF EXISTS {schema}.{table}_{import_date} RENAME TO {table};")

    for prefix in [
        'idx_drug_condition_evidence_id',
        'idx_drug_condition_evidence_set_id',
        'idx_drug_condition_evidence_condition',
        'idx_drug_condition_evidence_product_ndc',
        'idx_drug_treatment_mapping_id',
        'idx_drug_treatment_mapping_condition',
        'idx_drug_treatment_mapping_product_ndc',
    ]:
        await db.status(f"ALTER INDEX IF EXISTS {schema}.{prefix} RENAME TO {prefix}_old;")
        await db.status(f"ALTER INDEX IF EXISTS {schema}.{prefix}_{import_date} RENAME TO {prefix};")


async def import_drug_indications(test_mode=False, import_id=None):
    start = datetime.datetime.now()
    import_date = _import_date(import_id)
    schema = _schema()
    batch_size = int(os.getenv('HLTHPRT_DRUG_INDICATIONS_BATCH_SIZE', '1000'))
    test_limit = int(os.getenv('HLTHPRT_DRUG_INDICATIONS_TEST_LIMIT', '5000'))

    await db.status(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    evidence_cls = make_class(DrugConditionEvidence, import_date)
    mapping_cls = make_class(DrugTreatmentMapping, import_date)
    await db.status(f"DROP TABLE IF EXISTS {schema}.{evidence_cls.__tablename__};")
    await db.status(f"DROP TABLE IF EXISTS {schema}.{mapping_cls.__tablename__};")
    await evidence_cls.__table__.gino.create()
    await mapping_cls.__table__.gino.create()

    rxnorm_by_product = {}
    async with db.transaction():
        async for product in Product.query.gino.iterate():
            if getattr(product, 'product_ndc', None):
                rxnorm_by_product[product.product_ndc] = list(getattr(product, 'rxnorm_ids', None) or [])
    rxnorm_condition_map = await _load_rxnorm_condition_map()

    evidence_batch = []
    mapping_batch = []
    scanned = 0
    matched = 0
    async with db.transaction():
        async for label in Label.query.gino.iterate():
            scanned += 1
            for row in _matches(label, rxnorm_by_product):
                if 'evidence_id' in row:
                    evidence_batch.append(row)
                    matched += 1
                else:
                    mapping_batch.append(row)
            for row in _relationship_matches(label, rxnorm_by_product, rxnorm_condition_map):
                if 'evidence_id' in row:
                    evidence_batch.append(row)
                    matched += 1
                else:
                    mapping_batch.append(row)
            if len(evidence_batch) >= batch_size:
                await push_objects(evidence_batch, evidence_cls)
                evidence_batch = []
            if len(mapping_batch) >= batch_size:
                await push_objects(mapping_batch, mapping_cls)
                mapping_batch = []
            if test_mode and scanned >= test_limit:
                break

    await push_objects(evidence_batch, evidence_cls)
    await push_objects(mapping_batch, mapping_cls)
    await _create_indexes(schema, evidence_cls, mapping_cls, import_date)

    min_rows = int(os.getenv('HLTHPRT_DRUG_INDICATIONS_MIN_ROWS', '1' if test_mode else '100'))
    allow_empty = os.getenv('HLTHPRT_DRUG_INDICATIONS_ALLOW_EMPTY', '').lower() in {'1', 'true', 'yes'}
    evidence_count = await db.func.count(evidence_cls.evidence_id).gino.scalar()
    if evidence_count < min_rows and not allow_empty:
        raise RuntimeError(f"Drug indication stage has {evidence_count} rows, below minimum {min_rows}.")

    async with db.transaction():
        await _swap(schema, import_date)

    result = {
        'import_id': import_date,
        'labels_scanned': scanned,
        'condition_evidence_rows': evidence_count,
        'matched_labels': matched,
        'test_mode': bool(test_mode),
    }
    print(f"Drug indications import done: {result}")
    print_time_info(start)
    return result


async def main(test_mode=False, import_id=None):
    loop = asyncio.get_event_loop()
    await init_db(db, loop)
    try:
        return await import_drug_indications(test_mode=test_mode, import_id=import_id)
    finally:
        bind = db.pop_bind()
        if bind is not None:
            await bind.close()
