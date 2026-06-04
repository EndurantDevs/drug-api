import asyncio
import datetime
import hashlib
import os
import re
from collections import defaultdict

import asyncpg

from db.connection import init_db
from db.models import DrugConditionEvidence, Label, Product, db
from process.control_lifecycle import mark_control_run
from process.live_progress import enqueue_live_progress
from process.ext.utils import make_class, push_objects, print_time_info

NLM_ATTRIBUTION = (
    "This product uses publicly available data from the U.S. National Library of Medicine (NLM), "
    "National Institutes of Health, Department of Health and Human Services; NLM is not responsible "
    "for the product and does not endorse or recommend this or any other product."
)


def _schema():
    return os.getenv('DB_SCHEMA') or 'rx_data'


def _import_date(raw=None):
    raw = raw or os.getenv('HLTHPRT_DRUG_INDICATIONS_IMPORT_ID')
    if raw:
        cleaned = ''.join(ch for ch in str(raw) if ch.isalnum())
        if cleaned:
            return cleaned[:32]
    return datetime.datetime.utcnow().strftime('%Y%m%d')


def _clinical_schema():
    return os.getenv('HLTHPRT_CLINICAL_DB_SCHEMA') or 'mrf'


def _clinical_connection_kwargs():
    return {
        'host': os.getenv('HLTHPRT_CLINICAL_DB_HOST') or os.getenv('HLTHPRT_DB_HOST') or '127.0.0.1',
        'port': int(os.getenv('HLTHPRT_CLINICAL_DB_PORT') or '5440'),
        'user': os.getenv('HLTHPRT_CLINICAL_DB_USER') or os.getenv('HLTHPRT_DB_USER') or 'postgres',
        'password': os.getenv('HLTHPRT_CLINICAL_DB_PASSWORD') or os.getenv('HLTHPRT_DB_PASSWORD') or '',
        'database': (
            os.getenv('HLTHPRT_CLINICAL_DB_DATABASE')
            or os.getenv('HLTHPRT_DB_DATABASE')
            or os.getenv('DB_NAME')
            or 'healthporta'
        ),
    }


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


def _normalize_term(term):
    return re.sub(r'\s+', ' ', str(term or '').strip().lower())


def _term_matches(text_lower, term):
    normalized = _normalize_term(term)
    if len(normalized) < 4:
        return False
    return re.search(rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])", text_lower) is not None


async def _fetch_clinical_rows(test_mode=False):
    schema = _clinical_schema()
    try:
        connection = await asyncpg.connect(**_clinical_connection_kwargs())
        try:
            relationship_rows = await connection.fetch(
                f"""
                SELECT r.from_code AS rxcui,
                       r.to_system AS condition_system,
                       r.to_code AS condition_code,
                       r.relationship AS relationship,
                       COALESCE(r.source_attribution, c.source_attribution) AS source_attribution
                  FROM {schema}.code_relationship r
                  JOIN {schema}.code_catalog c
                    ON c.code_system = r.to_system
                   AND c.code = r.to_code
                   AND c.code_type = 'condition'
                 WHERE r.from_system = 'RXNORM'
                   AND r.relationship IN ('may_treat', 'may_prevent')
                """
            )
            term_rows = await connection.fetch(
                f"""
                SELECT code_system AS condition_system,
                       code AS condition_code,
                       display_name AS term,
                       'preferred' AS term_type
                  FROM {schema}.code_catalog
                 WHERE code_type = 'condition'
                   AND COALESCE(display_name, '') <> ''
                UNION ALL
                SELECT code_system AS condition_system,
                       code AS condition_code,
                       synonym AS term,
                       term_type
                  FROM {schema}.code_synonym
                 WHERE COALESCE(synonym, '') <> ''
                """
            )
        finally:
            await connection.close()
    except Exception as exc:
        allow_empty = os.getenv('HLTHPRT_DRUG_INDICATIONS_ALLOW_EMPTY', '').lower() in {'1', 'true', 'yes'}
        if test_mode or allow_empty:
            print(f"Clinical terminology lookup skipped: {exc}")
            return [], []
        raise RuntimeError(f"Clinical terminology lookup failed: {exc}") from exc
    return relationship_rows, term_rows


async def _load_official_condition_context(test_mode=False):
    relationship_rows, term_rows = await _fetch_clinical_rows(test_mode=test_mode)
    terms_by_condition = defaultdict(list)
    for row in term_rows:
        term = _normalize_term(row['term'])
        if term:
            terms_by_condition[(row['condition_system'], row['condition_code'])].append(
                {'term': term, 'term_type': row['term_type']}
            )

    relationships_by_rxnorm = defaultdict(list)
    for row in relationship_rows:
        key = (row['condition_system'], row['condition_code'])
        relationships_by_rxnorm[str(row['rxcui'])].append(
            {
                'condition_system': row['condition_system'],
                'condition_code': row['condition_code'],
                'relationship': row['relationship'],
                'source_attribution': row['source_attribution'] or NLM_ATTRIBUTION,
                'terms': terms_by_condition.get(key, []),
            }
        )
    return relationships_by_rxnorm


def _official_matches(label, rxnorm_by_product, relationships_by_rxnorm):
    text = getattr(label, 'indications_and_usage', None) or ''
    text_lower = re.sub(r'\s+', ' ', text.lower())
    product_ndc, package_ndc = _label_ndcs(label)
    rxnorm_ids = _rxnorm_ids_for_label(product_ndc, rxnorm_by_product)
    now = datetime.datetime.utcnow()
    emitted = set()
    for rxcui in rxnorm_ids:
        for item in relationships_by_rxnorm.get(str(rxcui), []):
            base_key = (rxcui, item['condition_system'], item['condition_code'])
            if base_key not in emitted:
                emitted.add(base_key)
                evidence_id = _hash_id(getattr(label, 'set_id', None), getattr(label, 'id', None), *base_key)
                yield {
                    'evidence_id': evidence_id,
                    'set_id': getattr(label, 'set_id', None),
                    'label_id': getattr(label, 'id', None),
                    'product_ndc': product_ndc,
                    'package_ndc': package_ndc,
                    'rxnorm_ids': rxnorm_ids,
                    'condition_system': item['condition_system'],
                    'condition_code': item['condition_code'],
                    'evidence_text': f"RxNorm {rxcui} {item['relationship']} {item['condition_system']}:{item['condition_code']} via clinical terminology relationship.",
                    'evidence_source': 'clinical_rxnorm_relationship',
                    'confidence': 0.85,
                    'source_attribution': item['source_attribution'],
                    'imported_at': now,
                }
            for term in item['terms']:
                matched_term = term['term']
                if not _term_matches(text_lower, matched_term):
                    continue
                term_key = (*base_key, matched_term)
                if term_key in emitted:
                    continue
                emitted.add(term_key)
                evidence_id = _hash_id(getattr(label, 'set_id', None), getattr(label, 'id', None), *term_key)
                yield {
                    'evidence_id': evidence_id,
                    'set_id': getattr(label, 'set_id', None),
                    'label_id': getattr(label, 'id', None),
                    'product_ndc': product_ndc,
                    'package_ndc': package_ndc,
                    'rxnorm_ids': rxnorm_ids,
                    'condition_system': item['condition_system'],
                    'condition_code': item['condition_code'],
                    'evidence_text': _text_excerpt(text, matched_term),
                    'evidence_source': 'clinical_synonym_label_match',
                    'confidence': 0.9 if term['term_type'] == 'preferred' else 0.8,
                    'source_attribution': item['source_attribution'],
                    'imported_at': now,
                }


async def _create_indexes(schema, evidence_cls, import_date):
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
        f"CREATE INDEX idx_drug_condition_evidence_rxnorm_{import_date} "
        f"ON {schema}.{evidence_cls.__tablename__} USING GIN(rxnorm_ids);"
    )


async def _publish(schema, import_date):
    await db.status(f"DROP TABLE IF EXISTS {schema}.drug_condition_evidence;")
    await db.status(
        f"ALTER TABLE IF EXISTS {schema}.drug_condition_evidence_{import_date} RENAME TO drug_condition_evidence;"
    )

    for prefix in [
        'idx_drug_condition_evidence_id',
        'idx_drug_condition_evidence_set_id',
        'idx_drug_condition_evidence_condition',
        'idx_drug_condition_evidence_product_ndc',
        'idx_drug_condition_evidence_rxnorm',
    ]:
        await db.status(f"ALTER INDEX IF EXISTS {schema}.{prefix}_{import_date} RENAME TO {prefix};")


async def import_drug_indications(test_mode=False, import_id=None, run_id=None):
    start = datetime.datetime.now()
    import_date = _import_date(import_id)
    schema = _schema()
    batch_size = int(os.getenv('HLTHPRT_DRUG_INDICATIONS_BATCH_SIZE', '1000'))
    test_limit = int(os.getenv('HLTHPRT_DRUG_INDICATIONS_TEST_LIMIT', '5000'))
    await mark_control_run(
        run_id,
        status="running",
        phase_detail="drug indications scanning labels",
        progress_message="scanning labels",
        progress={"unit": "labels", "done": 0, "total": test_limit if test_mode else None, "pct": 0, "message": "scanning labels"},
    )

    await db.status(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    evidence_cls = make_class(DrugConditionEvidence, import_date)
    await db.status(f"DROP TABLE IF EXISTS {schema}.{evidence_cls.__tablename__};")
    await evidence_cls.__table__.gino.create()

    rxnorm_by_product = {}
    async with db.transaction():
        async for product in Product.query.gino.iterate():
            if getattr(product, 'product_ndc', None):
                rxnorm_by_product[product.product_ndc] = list(getattr(product, 'rxnorm_ids', None) or [])
    relationships_by_rxnorm = await _load_official_condition_context(test_mode=test_mode)

    evidence_batch = []
    scanned = 0
    matched = 0
    async with db.transaction():
        async for label in Label.query.gino.iterate():
            scanned += 1
            for row in _official_matches(label, rxnorm_by_product, relationships_by_rxnorm):
                evidence_batch.append(row)
                matched += 1
            if len(evidence_batch) >= batch_size:
                await push_objects(evidence_batch, evidence_cls)
                evidence_batch = []
                enqueue_live_progress(
                    run_id=run_id,
                    importer="drug-indications",
                    status="running",
                    phase="drug indications scanning labels",
                    unit="labels",
                    done=scanned,
                    total=test_limit if test_mode else None,
                    message=f"scanned {scanned} labels; matched {matched}",
                )
            if test_mode and scanned >= test_limit:
                break

    await push_objects(evidence_batch, evidence_cls)
    await _create_indexes(schema, evidence_cls, import_date)

    min_rows = int(os.getenv('HLTHPRT_DRUG_INDICATIONS_MIN_ROWS', '0' if test_mode else '100'))
    allow_empty = os.getenv('HLTHPRT_DRUG_INDICATIONS_ALLOW_EMPTY', '').lower() in {'1', 'true', 'yes'}
    evidence_count = await db.func.count(evidence_cls.evidence_id).gino.scalar()
    if evidence_count < min_rows and not allow_empty:
        raise RuntimeError(f"Drug indication stage has {evidence_count} rows, below minimum {min_rows}.")

    publish_test_mode = os.getenv('HLTHPRT_DRUG_INDICATIONS_PUBLISH_TEST_MODE', '').lower() in {'1', 'true', 'yes'}
    published = False
    if not test_mode or publish_test_mode:
        async with db.transaction():
            await _publish(schema, import_date)
        published = True

    result = {
        'import_id': import_date,
        'stage_table': evidence_cls.__tablename__,
        'published': published,
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
