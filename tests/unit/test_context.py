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

    value = _get_timezone()
    assert isinstance(value, str)
    assert value


def test_timezone_is_iana_style():
    """Abbreviations like "EST" are ambiguous; a resolvable system must report its
    IANA identifier (the abbreviation remains the documented fallback)."""
    from alitycs.context import _get_timezone

    assert "/" in _get_timezone()


def test_local_iana_key_resolves_from_tz_env(monkeypatch):
    monkeypatch.setenv("TZ", ":America/Chicago")
    assert context_module._local_iana_key() == "America/Chicago"


def test_local_iana_key_resolves_etc_localtime_target(monkeypatch):
    import os

    monkeypatch.delenv("TZ", raising=False)
    real_realpath = os.path.realpath
    monkeypatch.setattr(
        os.path,
        "realpath",
        lambda path: "/usr/share/zoneinfo/Europe/Berlin" if path == "/etc/localtime" else real_realpath(path),
    )
    assert context_module._local_iana_key() == "Europe/Berlin"


def test_local_iana_key_returns_none_when_unresolvable(monkeypatch):
    import os

    monkeypatch.delenv("TZ", raising=False)

    def explode(path):
        raise OSError("no /etc/localtime here")

    monkeypatch.setattr(os.path, "realpath", explode)
    assert context_module._local_iana_key() is None


def test_timezone_falls_back_to_abbreviation(monkeypatch):
    """Where no IANA key can be resolved (notably Windows), the abbreviation is kept."""

    class StubLocal:
        def tzname(self):
            return "EST"

    class StubAware:
        def astimezone(self):
            return StubLocal()

    class StubDateTime:
        @staticmethod
        def now(tz=None):  # noqa: ARG004 - mirrors datetime.now(timezone.utc)
            return StubAware()

    monkeypatch.setattr(context_module, "_local_iana_key", lambda: None)
    monkeypatch.setattr(context_module, "datetime", StubDateTime)
    assert context_module._get_timezone() == "EST"


def test_timezone_survives_probe_failures(monkeypatch):
    def explode():
        raise RuntimeError("no timezone for you")

    monkeypatch.setattr(context_module, "_get_timezone", explode)
    context = collect_context("1.0.0")
    assert context.timezone is None  # _safe() swallowed the failure
    assert context.sdk_language == "python"  # everything else still collected


def test_safe_returns_none_when_getter_raises():
    def explode():
        raise ValueError("boom")

    assert _safe(explode) is None


def test_safe_passes_through_values():
    assert _safe(lambda: "value") == "value"
    assert _safe(lambda: None) is None
