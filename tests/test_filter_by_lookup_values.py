from __future__ import annotations

import pandas as pd
import pytest
from pydantic import ValidationError

from blocks.transform import FilterByLookupValues


def test_filter_by_lookup_values_allows_null_filter_params_when_filter_column_is_missing() -> None:
    data_df = pd.DataFrame(
        {
            "sample_id": ["A", "B", "C"],
            "value": [1.0, 2.0, 3.0],
        }
    )
    lookup_df = pd.DataFrame(
        {
            "lookup_id": ["A", "C"],
            "coverage": [0.6, 0.8],
        }
    )

    params = FilterByLookupValues.Params.model_validate(
        {
            "data_key": "sample_id",
            "lookup_key": "lookup_id",
            "lookup_filter_column": None,
            "lookup_filter_operator": None,
            "lookup_filter_value": None,
            "keep_matches": True,
        }
    )

    out = FilterByLookupValues().execute([data_df, lookup_df], params)

    assert out.data["sample_id"].tolist() == ["A", "C"]
    assert out.metadata["lookup_value_count"] == 2


def test_filter_by_lookup_values_requires_operator_when_filter_column_is_set() -> None:
    with pytest.raises(ValidationError, match="lookup_filter_operator is required"):
        FilterByLookupValues.Params.model_validate(
            {
                "data_key": "sample_id",
                "lookup_key": "lookup_id",
                "lookup_filter_column": "coverage",
                "lookup_filter_operator": None,
                "lookup_filter_value": 0.5,
                "keep_matches": True,
            }
        )


def test_filter_by_lookup_values_applies_lookup_filter_before_matching() -> None:
    data_df = pd.DataFrame(
        {
            "sample_id": ["A", "B", "C"],
            "value": [1.0, 2.0, 3.0],
        }
    )
    lookup_df = pd.DataFrame(
        {
            "lookup_id": ["A", "C"],
            "coverage": [0.6, 0.4],
        }
    )

    params = FilterByLookupValues.Params.model_validate(
        {
            "data_key": "sample_id",
            "lookup_key": "lookup_id",
            "lookup_filter_column": "coverage",
            "lookup_filter_operator": "gte",
            "lookup_filter_value": 0.5,
            "keep_matches": True,
        }
    )

    out = FilterByLookupValues().execute([data_df, lookup_df], params)

    assert out.data["sample_id"].tolist() == ["A"]
    assert out.metadata["lookup_value_count"] == 1
