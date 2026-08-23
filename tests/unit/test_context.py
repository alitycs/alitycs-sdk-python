import alitycs.context as context_module
from alitycs.context import _safe, collect_context


def test_collect_context_reports_python_sdk_fields():
    context = collect_context("9.9.9").to_dict()
    assert context["sdkVersion"] == "9.9.9"
    assert context["sdkLanguage"] == "python"


def test_collect_context_includes_process_metadata():
    payload = collect_context("1.0.0").to_dict()
    assert payload.get("osName")
    assert payload.get("pythonVersion")


def test_locale_is_bcp47_style(monkeypatch):
    import locale

    monkeypatch.setattr(locale, "getlocale", lambda: ("en_US", "UTF-8"), raising=False)
    assert collect_context("1.0.0").locale == "en-US"


def test_locale_none_when_unset(monkeypatch):
    import locale

    monkeypatch.setattr(locale, "getlocale", lambda: (None, None), raising=False)
    assert collect_context("1.0.0").locale is None


def test_locale_survives_probe_failures(monkeypatch):
    def explode():
        raise RuntimeError("no locale for you")

    monkeypatch.setattr(context_module, "_get_locale", explode)
    context = collect_context("1.0.0")
    assert context.locale is None
    assert context.sdk_language == "python"  # everything else still collected


def test_timezone_is_reported():
    from alitycs.context import _get_timezone

    assert isinstance(_get_timezone(), str)


def test_safe_returns_none_when_getter_raises():
    def explode():
        raise ValueError("boom")

    assert _safe(explode) is None


def test_safe_passes_through_values():
    assert _safe(lambda: "value") == "value"
    assert _safe(lambda: None) is None
