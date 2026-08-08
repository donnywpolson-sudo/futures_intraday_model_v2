from futures_rebuild.high_risk import CONFIRMATION_SCHEMA, confirmation_required, main


def test_confirmation_summary_has_no_token_or_execution_path() -> None:
    result = confirmation_required(
        "provider read",
        scope={"requests": "1"},
        outputs=("reports/provider.json",),
    )
    assert result["schema_version"] == CONFIRMATION_SCHEMA
    assert result["status"] == "CONFIRMATION_REQUIRED"
    assert "approval" not in result
    assert result["outputs"] == ["reports/provider.json"]


def test_prepare_cli_rejects_malformed_scope() -> None:
    try:
        main(["--operation", "provider read", "--scope", "requests"])
    except SystemExit as exc:
        assert str(exc) == "--scope values must be KEY=VALUE"
    else:
        raise AssertionError("malformed scope must fail")
