import os
from typing import TYPE_CHECKING

from sqlalchemy import (
    DATE,
    JSON,
    TEXT,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    String,
)
from sqlalchemy.dialects.postgresql import ARRAY

from db.connection import db
from db.json_mixin import JSONOutputMixin


class Product(db.Model, JSONOutputMixin):
    __tablename__ = 'product'
    __table_args__ = (
        {'schema': os.getenv('DB_SCHEMA') or 'rx_data', 'extend_existing': True},
    )
    product_ndc = Column(String)
    generic_name = Column(String)
    labeler_name = Column(String)
    brand_name = Column(String)
    active_ingredients = Column(ARRAY(JSON))
    finished = Column(Boolean)
    listing_expiration_date = Column(DATE)
    openfda = Column(JSON)
    marketing_category = Column(String)
    is_otc = Column(Boolean)
    dosage_form = Column(String)
    short_dosage_form = Column(String)
    spl_id = Column(String)
    product_type = Column(String)
    route = Column(ARRAY(String))
    marketing_start_date = Column(DATE)
    marketing_end_date = Column(DATE)
    product_id = Column(String, primary_key=True)
    application_number = Column(String)
    brand_name_base = Column(String)
    pharm_class = Column(ARRAY(String))
    dea_schedule = Column(String)
    rxnorm_ids = Column(ARRAY(String))


class Package(db.Model, JSONOutputMixin):
    __tablename__ = 'package'
    __table_args__ = (
        {'schema': os.getenv('DB_SCHEMA') or 'rx_data', 'extend_existing': True},
    )
    package_ndc = Column(String, primary_key=True)
    ndc11 = Column(String)
    product_ndc = Column(String)
    description = Column(TEXT)
    size = Column(BigInteger)
    size_extra = Column(String)
    packages_number = Column(BigInteger)
    package_format = Column(String)
    marketing_start_date = Column(DATE)
    sample = Column(Boolean)


class Label(db.Model, JSONOutputMixin):
    from db.drug_snapshot_runtime.label import Label as _ArchiveLabel

    __tablename__ = "label"
    __table__ = _ArchiveLabel.__table__.to_metadata(db.Model.metadata)
    if TYPE_CHECKING:
        id = _ArchiveLabel.id
        set_id = _ArchiveLabel.set_id


class DrugConditionEvidence(db.Model, JSONOutputMixin):
    __tablename__ = 'drug_condition_evidence'
    __table_args__ = (
        {'schema': os.getenv('DB_SCHEMA') or 'rx_data', 'extend_existing': True},
    )
    evidence_id = Column(String, primary_key=True)
    set_id = Column(String)
    label_id = Column(String)
    product_ndc = Column(ARRAY(String))
    package_ndc = Column(ARRAY(String))
    rxnorm_ids = Column(ARRAY(String))
    condition_system = Column(String)
    condition_code = Column(String)
    evidence_text = Column(TEXT)
    evidence_source = Column(String)
    confidence = Column(Float)
    source_attribution = Column(TEXT)
    imported_at = Column(DateTime)
