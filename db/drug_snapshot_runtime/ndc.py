"""Canonical product/package declarations for application and archive runtimes."""

import os

from sqlalchemy import DATE, JSON, TEXT, BigInteger, Boolean, Column, String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import DeclarativeBase


class NdcMetadataBase(DeclarativeBase):
    """Independent metadata without application startup or database connections."""


class Product(NdcMetadataBase):
    __tablename__ = "product"
    __table_args__ = ({"schema": os.getenv("DB_SCHEMA") or "rx_data", "extend_existing": True},)
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

    __my_additional_indexes__ = (
        {
            "name": "product_ndc",
            "staging_name": "product_idx_product_ndc",
            "index_elements": ["product_ndc"],
            "using": "gin",
        },
        {
            "name": "brand_trgm_idx",
            "staging_name": "product_idx_brand_trgm_idx",
            "index_elements": ["brand_name gin_trgm_ops"],
            "using": "gin",
        },
        {
            "name": "generic_trgm_idx",
            "staging_name": "product_idx_generic_trgm_idx",
            "index_elements": ["generic_name gin_trgm_ops"],
            "using": "gin",
        },
        {"name": "rxnorm", "staging_name": "product_rxnorm_idx", "index_elements": ["rxnorm_ids"], "using": "gin"},
    )


class Package(NdcMetadataBase):
    __tablename__ = "package"
    __table_args__ = ({"schema": os.getenv("DB_SCHEMA") or "rx_data", "extend_existing": True},)
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

    __my_additional_indexes__ = (
        {
            "name": "product_ndc",
            "staging_name": "package_idx_product_ndc",
            "index_elements": ["product_ndc"],
            "using": "gin",
        },
    )


# Product identifiers, not product NDCs, are unique. Multiple product records may
# legitimately share a product NDC; the package edge is an indexed existence test.
RELATIONSHIPS = ((Package, "product_ndc", Product, "product_ndc", False, False),)
