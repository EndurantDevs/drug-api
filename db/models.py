import os
from typing import TYPE_CHECKING

from sqlalchemy import (
    TEXT,
    Column,
    DateTime,
    Float,
    String,
)
from sqlalchemy.dialects.postgresql import ARRAY

from db.connection import db
from db.json_mixin import JSONOutputMixin


class Product(db.Model, JSONOutputMixin):
    from db.drug_snapshot_runtime.ndc import Product as _ArchiveProduct

    __tablename__ = "product"
    __table__ = _ArchiveProduct.__table__.to_metadata(db.Model.metadata)
    if TYPE_CHECKING:
        product_id = _ArchiveProduct.product_id
        product_ndc = _ArchiveProduct.product_ndc
        generic_name = _ArchiveProduct.generic_name
        labeler_name = _ArchiveProduct.labeler_name
        brand_name = _ArchiveProduct.brand_name
        active_ingredients = _ArchiveProduct.active_ingredients
        finished = _ArchiveProduct.finished
        listing_expiration_date = _ArchiveProduct.listing_expiration_date
        openfda = _ArchiveProduct.openfda
        marketing_category = _ArchiveProduct.marketing_category
        is_otc = _ArchiveProduct.is_otc
        dosage_form = _ArchiveProduct.dosage_form
        short_dosage_form = _ArchiveProduct.short_dosage_form
        spl_id = _ArchiveProduct.spl_id
        product_type = _ArchiveProduct.product_type
        route = _ArchiveProduct.route
        marketing_start_date = _ArchiveProduct.marketing_start_date
        marketing_end_date = _ArchiveProduct.marketing_end_date
        application_number = _ArchiveProduct.application_number
        brand_name_base = _ArchiveProduct.brand_name_base
        pharm_class = _ArchiveProduct.pharm_class
        dea_schedule = _ArchiveProduct.dea_schedule
        rxnorm_ids = _ArchiveProduct.rxnorm_ids


class Package(db.Model, JSONOutputMixin):
    from db.drug_snapshot_runtime.ndc import Package as _ArchivePackage

    __tablename__ = "package"
    __table__ = _ArchivePackage.__table__.to_metadata(db.Model.metadata)
    if TYPE_CHECKING:
        package_ndc = _ArchivePackage.package_ndc
        ndc11 = _ArchivePackage.ndc11
        product_ndc = _ArchivePackage.product_ndc
        description = _ArchivePackage.description
        size = _ArchivePackage.size
        size_extra = _ArchivePackage.size_extra
        packages_number = _ArchivePackage.packages_number
        package_format = _ArchivePackage.package_format
        marketing_start_date = _ArchivePackage.marketing_start_date
        sample = _ArchivePackage.sample


class Label(db.Model, JSONOutputMixin):
    from db.drug_snapshot_runtime.label import Label as _ArchiveLabel

    __tablename__ = "label"
    __table__ = _ArchiveLabel.__table__.to_metadata(db.Model.metadata)
    if TYPE_CHECKING:
        id = _ArchiveLabel.id
        set_id = _ArchiveLabel.set_id


class DrugConditionEvidence(db.Model, JSONOutputMixin):
    __tablename__ = "drug_condition_evidence"
    __table_args__ = ({"schema": os.getenv("DB_SCHEMA") or "rx_data", "extend_existing": True},)
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
