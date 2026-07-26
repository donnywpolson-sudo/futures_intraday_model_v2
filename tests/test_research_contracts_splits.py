from __future__ import annotations

import numpy as np
import pytest

from futures_rebuild.research import (
    ResearchContractError,
    SessionWindow,
    SyntheticOnlyPermit,
    TemporalSamples,
    assert_disjoint_partitions,
    make_synthetic_permit,
    nested_chronological_splits,
    purge_and_post_embargo_indices,
    require_synthetic_permit,
)


def test_synthetic_permit_binds_exact_float64_fixture() -> None:
    fixture = np.asarray([[1.0, 2.0], [3.0, 4.0]], dtype=np.float64)
    permit = make_synthetic_permit(fixture, generator_id="futures-oracle", seed=17)
    require_synthetic_permit(permit, fixture)
    changed = fixture.copy()
    changed[0, 0] = 9.0
    with pytest.raises(ResearchContractError, match="exact fixture"):
        require_synthetic_permit(permit, changed)


def test_synthetic_permit_rejects_history_sealing_bad_dtype_and_nan() -> None:
    fixture = np.asarray([1.0, 2.0], dtype=np.float64)
    with pytest.raises(ResearchContractError, match="SYNTHETIC"):
        make_synthetic_permit(
            fixture, generator_id="bad", seed=0, source_kind="REAL_HISTORY"
        )
    forged = SyntheticOnlyPermit(
        "MECHANICS_ONLY", "SYNTHETIC", "bad", 0, "0" * 64, False, True
    )
    with pytest.raises(ResearchContractError, match="cannot authorize"):
        require_synthetic_permit(forged)
    with pytest.raises(ResearchContractError, match="float64"):
        make_synthetic_permit(
            np.asarray([1.0], dtype=np.float32), generator_id="dtype", seed=0
        )
    with pytest.raises(ResearchContractError, match="NaN"):
        make_synthetic_permit(
            np.asarray([np.nan], dtype=np.float64), generator_id="nan", seed=0
        )


def test_fit_audit_overlap_fails_closed() -> None:
    with pytest.raises(ResearchContractError, match="overlap"):
        assert_disjoint_partitions(
            np.asarray([0, 1], dtype=np.int64),
            np.asarray([1, 2], dtype=np.int64),
        )


def test_half_open_purge_and_post_embargo_oracle() -> None:
    samples = TemporalSamples(
        np.asarray([0, 2, 4, 6, 8], dtype=np.int64),
        np.asarray([0, 2, 4, 6, 8], dtype=np.int64),
        np.asarray([2, 5, 6, 9, 10], dtype=np.int64),
        np.asarray([2, 5, 6, 9, 10], dtype=np.int64),
    )
    actual = purge_and_post_embargo_indices(
        samples,
        np.asarray([0, 1, 3, 4], dtype=np.int64),
        np.asarray([2], dtype=np.int64),
        post_embargo_sessions=2,
    )
    np.testing.assert_array_equal(actual, np.asarray([0, 4], dtype=np.int64))


def test_nested_chronological_split_oracle() -> None:
    decision = np.arange(10, dtype=np.int64)
    samples = TemporalSamples(decision, decision, decision + 2, decision + 2)
    folds = nested_chronological_splits(
        samples,
        (SessionWindow(7, 9),),
        ((SessionWindow(4, 5),),),
        session_embargo=1,
        minimum_fit_samples=2,
        minimum_audit_samples=1,
    )
    np.testing.assert_array_equal(folds[0].fit_indices, np.arange(5, dtype=np.int64))
    np.testing.assert_array_equal(folds[0].audit_indices, np.asarray([7, 8], dtype=np.int64))
    np.testing.assert_array_equal(
        folds[0].inner_folds[0].fit_indices, np.asarray([0, 1], dtype=np.int64)
    )
    np.testing.assert_array_equal(
        folds[0].inner_folds[0].audit_indices, np.asarray([4], dtype=np.int64)
    )


def test_split_scalar_bool_and_inner_outer_leakage_fail_closed() -> None:
    decision = np.arange(10, dtype=np.int64)
    samples = TemporalSamples(decision, decision, decision + 1, decision + 1)
    with pytest.raises(ResearchContractError, match="explicit integer"):
        nested_chronological_splits(
            samples,
            (SessionWindow(7, 9),),
            ((SessionWindow(4, 5),),),
            session_embargo=True,
            minimum_fit_samples=1,
            minimum_audit_samples=1,
        )
    with pytest.raises(ResearchContractError, match="outer embargo"):
        nested_chronological_splits(
            samples,
            (SessionWindow(7, 9),),
            ((SessionWindow(6, 7),),),
            session_embargo=1,
            minimum_fit_samples=1,
            minimum_audit_samples=1,
        )
