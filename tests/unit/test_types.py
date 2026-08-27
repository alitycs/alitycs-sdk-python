import pytest

from alitycs.types import (
    AnalyticsEvent,
    BatchPayload,
    EventContext,
    EventType,
    RevenueError,
    RevenuePayload,
)


def make_context(**overrides) -> EventContext:
    fields = {"sdk_version": "1.0.0", "sdk_language": "python"}
    fields.update(overrides)
    return EventContext(**fields)


def make_event(**overrides) -> AnalyticsEvent:
    fields = {
        "event_id": "evt_1",
        "event": "signup",
        "event_type": EventType.TRACK,
        "anonymous_id": "anon_1",
        "session_id": "sess_1",
        "timestamp": 1_700_000_000_000,
        "properties": {"plan": "pro"},
        "context": make_context(),
    }
    fields.update(overrides)
    return AnalyticsEvent(**fields)


class TestEventType:
    def test_values_match_the_wire_contract(self):
        assert EventType.TRACK.value == "track"
        assert EventType.IDENTIFY.value == "identify"
        assert EventType.PAGE.value == "page"
        assert EventType.ERROR.value == "error"


class TestEventContext:
    def test_required_fields_and_camel_case(self):
        payload = make_context().to_dict()
        assert payload == {"sdkVersion": "1.0.0", "sdkLanguage": "python"}

    def test_optional_fields_are_omitted_when_unset(self):
        payload = make_context(locale="en-US", timezone="UTC", os_name="Darwin").to_dict()
        assert payload == {
            "sdkVersion": "1.0.0",
            "sdkLanguage": "python",
            "locale": "en-US",
            "timezone": "UTC",
            "osName": "Darwin",
        }

    def test_browser_fields_serialize_when_present(self):
        context = make_context(
            user_agent="ua",
            url="https://example.com/x",
            referrer="https://example.com/",
            screen={"width": "1440", "height": "900"},
            utm_source="news",
            utm_medium="cpc",
            utm_campaign="launch",
            utm_content="a",
            utm_term="alytics",
        )
        payload = context.to_dict()
        assert payload["userAgent"] == "ua"
        assert payload["url"] == "https://example.com/x"
        assert payload["referrer"] == "https://example.com/"
        assert payload["screen"] == {"width": "1440", "height": "900"}
        assert payload["utmSource"] == "news"
        assert payload["utmMedium"] == "cpc"
        assert payload["utmCampaign"] == "launch"
        assert payload["utmContent"] == "a"
        assert payload["utmTerm"] == "alytics"


class TestRevenuePayloadTransaction:
    def test_happy_path(self):
        payload = RevenuePayload.transaction(fact_id="fact-1", amount="19.99", currency="USD")
        assert payload.to_dict() == {
            "version": 1,
            "kind": "transaction",
            "factId": "fact-1",
            "amount": "19.99",
            "currency": "USD",
        }

    def test_customer_id_is_included_when_given(self):
        payload = RevenuePayload.transaction(fact_id="f", amount="5.00", currency="EUR", customer_id="cus_1")
        assert payload.to_dict()["customerId"] == "cus_1"

    def test_amount_is_required(self):
        with pytest.raises(RevenueError, match="amount"):
            RevenuePayload(kind="transaction", fact_id="f", currency="USD")

    def test_negative_amounts_are_allowed_for_transactions(self):
        payload = RevenuePayload.transaction(fact_id="f", amount="-3.20", currency="USD")
        assert payload.amount == "-3.20"


class TestRevenuePayloadMrrSnapshot:
    def test_happy_path(self):
        payload = RevenuePayload.mrr_snapshot(
            fact_id="f", subscription_id="sub", customer_id="cus", mrr_amount="120.00", currency="USD"
        )
        data = payload.to_dict()
        assert data["kind"] == "mrr_snapshot"
        assert data["subscriptionId"] == "sub"
        assert data["mrrAmount"] == "120.00"

    def test_requires_subscription_customer_and_amount(self):
        with pytest.raises(RevenueError, match="subscriptionId"):
            RevenuePayload(kind="mrr_snapshot", fact_id="f", currency="USD")

    def test_rejects_negative_mrr(self):
        with pytest.raises(RevenueError, match="non-negative"):
            RevenuePayload(
                kind="mrr_snapshot",
                fact_id="f",
                subscription_id="sub",
                customer_id="cus",
                mrr_amount="-1.00",
                currency="USD",
            )


class TestRevenuePayloadMrrBaseline:
    def test_happy_path(self):
        payload = RevenuePayload.mrr_baseline_complete(fact_id="f", currency="USD", expected_active_subscriptions=7)
        assert payload.to_dict() == {
            "version": 1,
            "kind": "mrr_baseline_complete",
            "factId": "f",
            "currency": "USD",
            "expectedActiveSubscriptions": 7,
        }

    def test_expected_subscriptions_is_required(self):
        with pytest.raises(RevenueError, match="expectedActiveSubscriptions"):
            RevenuePayload(kind="mrr_baseline_complete", fact_id="f", currency="USD")

    @pytest.mark.parametrize("value", [-1, True, 1.5])
    def test_expected_subscriptions_must_be_a_non_negative_integer(self, value):
        with pytest.raises(RevenueError, match="non-negative integer"):
            RevenuePayload(kind="mrr_baseline_complete", fact_id="f", currency="USD", expected_active_subscriptions=value)


class TestRevenueValidationShared:
    def test_unknown_kind_rejected(self):
        with pytest.raises(RevenueError, match="Unknown revenue kind"):
            RevenuePayload(kind="refund", fact_id="f", amount="1.00", currency="USD")

    def test_version_is_pinned_to_one(self):
        with pytest.raises(RevenueError, match="version"):
            RevenuePayload(kind="transaction", fact_id="f", amount="1.00", currency="USD", version=2)

    @pytest.mark.parametrize("fact_id", ["", "   ", None, 42])
    def test_fact_id_must_be_a_non_empty_string(self, fact_id):
        with pytest.raises(RevenueError, match="factId"):
            RevenuePayload.transaction(fact_id=fact_id, amount="1.00", currency="USD")

    def test_fact_id_capped_at_200_characters(self):
        with pytest.raises(RevenueError, match="200"):
            RevenuePayload.transaction(fact_id="x" * 201, amount="1.00", currency="USD")

    @pytest.mark.parametrize("currency", ["usd", "US", "USDD", "", None, 840])
    def test_currency_must_be_three_uppercase_letters(self, currency):
        with pytest.raises(RevenueError, match="currency"):
            RevenuePayload.transaction(fact_id="f", amount="1.00", currency=currency)

    @pytest.mark.parametrize("amount", ["1.0000000001", "01.5", "+1.0", "1e5", "nan", 1.0])
    def test_amounts_must_be_plain_decimals_with_at_most_nine_fraction_digits(self, amount):
        with pytest.raises(RevenueError, match="decimal"):
            RevenuePayload.transaction(fact_id="f", amount=amount, currency="USD")

    def test_precision_capped_at_38_digits(self):
        with pytest.raises(RevenueError, match="38 digits"):
            RevenuePayload.transaction(fact_id="f", amount="12345678901234567890123456789012345678.9", currency="USD")

    def test_precision_exactly_38_digits_passes(self):
        payload = RevenuePayload.transaction(fact_id="f", amount="12345678901234567890123456789012345678", currency="USD")
        assert payload.amount.endswith("678")

    # Per-kind exclusivity mirrors the server: foreign fields are rejected.

    def test_transaction_rejects_recurring_only_fields(self):
        with pytest.raises(RevenueError, match="another revenue kind"):
            RevenuePayload(
                kind="transaction", fact_id="f", amount="1.00", currency="USD",
                subscription_id="sub_1",
            )
        with pytest.raises(RevenueError, match="another revenue kind"):
            RevenuePayload(
                kind="transaction", fact_id="f", amount="1.00", currency="USD",
                mrr_amount="5.00",
            )
        with pytest.raises(RevenueError, match="another revenue kind"):
            RevenuePayload(
                kind="transaction", fact_id="f", amount="1.00", currency="USD",
                expected_active_subscriptions=3,
            )

    def test_mrr_snapshot_rejects_transaction_or_baseline_fields(self):
        for extra in (
            {"amount": "1.00"},
            {"expected_active_subscriptions": 3},
        ):
            with pytest.raises(RevenueError, match="another revenue kind"):
                RevenuePayload(
                    kind="mrr_snapshot", fact_id="f", currency="USD",
                    subscription_id="s", customer_id="c", mrr_amount="5.00",
                    **extra,
                )

    def test_mrr_baseline_complete_rejects_amount_and_identity_fields(self):
        for extra in (
            {"amount": "1.00"},
            {"mrr_amount": "5.00"},
            {"subscription_id": "s"},
            {"customer_id": "c"},
        ):
            with pytest.raises(RevenueError, match="another revenue kind"):
                RevenuePayload(
                    kind="mrr_baseline_complete", fact_id="f", currency="USD",
                    expected_active_subscriptions=2,
                    **extra,
                )


class TestAnalyticsEvent:
    def test_to_dict_uses_wire_names_and_omits_absent_user_and_revenue(self):
        event = make_event()
        payload = event.to_dict()
        assert payload["eventId"] == "evt_1"
        assert payload["eventType"] == "track"
        assert "userId" not in payload
        assert "revenue" not in payload
        assert payload["anonymousId"] == "anon_1"
        assert payload["sessionId"] == "sess_1"
        assert payload["properties"] == {"plan": "pro"}
        assert payload["context"]["sdkLanguage"] == "python"

    def test_user_id_and_revenue_serialize_when_present(self):
        revenue = RevenuePayload.transaction(fact_id="f", amount="1.00", currency="USD")
        event = make_event(user_id="usr_1", revenue=revenue, event_type=EventType.IDENTIFY)
        payload = event.to_dict()
        assert payload["userId"] == "usr_1"
        assert payload["revenue"]["kind"] == "transaction"

    def test_properties_dict_is_copied_not_shared(self):
        properties = {"k": "v"}
        event = make_event(properties=properties)
        properties["k"] = "changed"
        assert event.to_dict()["properties"] == {"k": "v"}


class TestBatchPayload:
    def test_to_dict_wraps_events(self):
        batch = BatchPayload(batch_id="batch_1", sent_at=123, events=[make_event()])
        payload = batch.to_dict()
        assert payload["batchId"] == "batch_1"
        assert payload["sentAt"] == 123
        assert len(payload["events"]) == 1
