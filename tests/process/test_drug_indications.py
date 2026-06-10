import importlib
import types

drug_indications = importlib.import_module("process.drug_indications")


def test_official_matches_uses_rxnorm_relationship_and_synonym_terms():
    label = types.SimpleNamespace(
        set_id="set-1",
        id="label-1",
        product_ndc=["0001-0001"],
        package_ndc=["0001-0001-01"],
        indications_and_usage="This medication is indicated for menorrhagia in adults.",
    )
    rxnorm_by_product = {"0001-0001": ["12345"]}
    relationships_by_rxnorm = {
        "12345": [
            {
                "condition_system": "MESH",
                "condition_code": "D008595",
                "relationship": "may_treat",
                "source_attribution": drug_indications.NLM_ATTRIBUTION,
                "terms": [{"term": "menorrhagia", "term_type": "preferred"}],
            }
        ]
    }

    rows = list(drug_indications._official_matches(label, rxnorm_by_product, relationships_by_rxnorm))

    assert {row["evidence_source"] for row in rows} == {
        "clinical_rxnorm_relationship",
        "clinical_synonym_label_match",
    }
    assert all(row["condition_system"] == "MESH" for row in rows)
    assert all(row["condition_code"] == "D008595" for row in rows)


def test_condition_lexicon_removed():
    assert not hasattr(drug_indications, "CONDITION_LEXICON")
