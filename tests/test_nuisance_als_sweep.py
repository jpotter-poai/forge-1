from __future__ import annotations

import numpy as np
import pandas as pd

from blocks.factorization import NuisanceALSSweep


def test_nuisance_als_sweep_uses_generic_group_step_keys() -> None:
    matrix = pd.DataFrame(
        [
            [1.0, 2.0, 3.0, np.nan],
            [4.0, 5.0, np.nan, 7.0],
        ],
        index=["A1", "A2"],
        columns=[
            "PrefixA__Step1",
            "PrefixA__Step2",
            "PrefixB__Step1",
            "PrefixB__Step2",
        ],
    )
    weights = pd.DataFrame(1.0, index=matrix.index, columns=matrix.columns)

    out = NuisanceALSSweep().execute(
        [matrix, weights],
        NuisanceALSSweep.Params(
            k_values="1",
            lambda_values="1.0",
            n_repeats=1,
            n_iters=1,
            holdout_frac=0.5,
            column_separator="__",
            group_prefix="Prefix",
            step_prefix="Step",
            holdout_strategy="group_block",
        ),
    )

    assert out.data.shape[0] == 1
    assert out.data.loc[0, "group_prefix"] == "Prefix"
    assert out.data.loc[0, "step_prefix"] == "Step"
    assert out.metadata["holdout_strategy"] == "group_block"
    assert out.metadata["group_prefix"] == "Prefix"
    assert out.metadata["step_prefix"] == "Step"
    assert {"group_prefix", "step_prefix", "holdout_strategy"} <= set(out.metadata)
