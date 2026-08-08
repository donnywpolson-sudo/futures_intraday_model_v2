from futures_rebuild.tier1_bracket_model import DirectionalTrainingRow, fit_directional_ridge, predict_fold


def _row(value: float) -> DirectionalTrainingRow:
    return DirectionalTrainingRow((value, value / 10, 0.0, 1.0), value, -value)


def test_two_target_ridge_and_threshold_use_training_rows_only() -> None:
    training = [_row(1.0), _row(2.0), _row(3.0), _row(4.0), _row(5.0)]
    model = fit_directional_ridge(training_rows=training)
    ordinary = predict_fold(model=model, training_rows=training, test_rows=[_row(3.0)])
    extreme_test = predict_fold(model=model, training_rows=training, test_rows=[_row(99999.0)])

    assert ordinary[0].neutral_threshold == extreme_test[0].neutral_threshold
    assert ordinary[0].long_prediction_net_r > ordinary[0].short_prediction_net_r
    assert extreme_test[0].selected_direction == "long"
