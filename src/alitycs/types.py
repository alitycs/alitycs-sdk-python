"""Wire-contract types: events, revenue payloads, batches (schema v0.4.0).

Field names are snake_case in Python and serialized to the schema's camelCase by
each type's :meth:`to_dict`. Optional fields are omitted from the JSON payload,
matching ``explicitNulls = false`` in alitycs-sdk-jvm.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
_DECIMAL_RE = re.compile(r"^-?(?:0|[1-9]\d*)(?:\.\d{1,9})?$")
_MAX_FACT_ID_LENGTH = 200
_MAX_DECIMAL_PRECISION = 38


class EventType(str, Enum):
    TRACK = "track"
    IDENTIFY = "identify"
    PAGE = "page"
    ERROR = "error"


class RevenueError(ValueError):
    """Raised when a revenue payload violates its variant's validation rules."""


@dataclass(frozen=True)
class EventContext:
    """Environment metadata attached to every event.

    ``sdk_version`` and ``sdk_language`` are required by the wire contract; servers
    leave the browser-only fields (``user_agent``, ``url``, ``referrer``, ``screen``,
    UTM parameters) unset rather than fabricating them. Extra keys beyond the schema's
    are permitted (``additionalProperties: true``).
    """

    sdk_version: str
    sdk_language: str
    locale: Optional[str] = None
    timezone: Optional[str] = None
    user_agent: Optional[str] = None
    url: Optional[str] = None
    referrer: Optional[str] = None
    screen: Optional[Dict[str, str]] = None
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_content: Optional[str] = None
    utm_term: Optional[str] = None
    os_name: Optional[str] = None
    os_version: Optional[str] = None
    python_version: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "sdkVersion": self.sdk_version,
            "sdkLanguage": self.sdk_language,
        }
        optional = {
            "locale": self.locale,
            "timezone": self.timezone,
            "userAgent": self.user_agent,
            "url": self.url,
            "referrer": self.referrer,
            "screen": self.screen,
            "utmSource": self.utm_source,
            "utmMedium": self.utm_medium,
            "utmCampaign": self.utm_campaign,
            "utmContent": self.utm_content,
            "utmTerm": self.utm_term,
            "osName": self.os_name,
            "osVersion": self.os_version,
            "pythonVersion": self.python_version,
        }
        for key, value in optional.items():
            if value is not None:
                payload[key] = value
        return payload


@dataclass(frozen=True)
class RevenuePayload:
    """Server-side trusted revenue ingestion payload.

    Build one through the variant constructors — :meth:`transaction`,
    :meth:`mrr_snapshot`, or :meth:`mrr_baseline_complete` — which mirror
    ``RevenuePayload`` in alitycs-sdk-jvm. Validation rules are shared across SDKs:
    ISO-3166-style uppercase currency, non-exponent decimal strings with at most nine
    fraction digits and 38 digits of precision, non-negative MRR amounts.
    """

    kind: str
    fact_id: str
    currency: str
    amount: Optional[str] = None
    customer_id: Optional[str] = None
    subscription_id: Optional[str] = None
    mrr_amount: Optional[str] = None
    expected_active_subscriptions: Optional[int] = None
    version: int = 1

    @classmethod
    def transaction(
        cls,
        fact_id: str,
        amount: str,
        currency: str,
        customer_id: Optional[str] = None,
    ) -> "RevenuePayload":
        return cls(kind="transaction", fact_id=fact_id, amount=amount, currency=currency, customer_id=customer_id)

    @classmethod
    def mrr_snapshot(
        cls,
        fact_id: str,
        subscription_id: str,
        customer_id: str,
        mrr_amount: str,
        currency: str,
    ) -> "RevenuePayload":
        return cls(
            kind="mrr_snapshot",
            fact_id=fact_id,
            currency=currency,
            customer_id=customer_id,
            subscription_id=subscription_id,
            mrr_amount=mrr_amount,
        )

    @classmethod
    def mrr_baseline_complete(
        cls,
        fact_id: str,
        currency: str,
        expected_active_subscriptions: int,
    ) -> "RevenuePayload":
        return cls(
            kind="mrr_baseline_complete",
            fact_id=fact_id,
            currency=currency,
            expected_active_subscriptions=expected_active_subscriptions,
        )

    def __post_init__(self) -> None:
        if not isinstance(self.version, int) or self.version != 1:
            raise RevenueError("Revenue payload version must be 1")
        if self.kind not in ("transaction", "mrr_snapshot", "mrr_baseline_complete"):
            raise RevenueError(f"Unknown revenue kind: {self.kind!r}")
        if not isinstance(self.fact_id, str) or not self.fact_id.strip():
            raise RevenueError("Revenue factId must be a non-empty string")
        if len(self.fact_id) > _MAX_FACT_ID_LENGTH:
            raise RevenueError("Revenue factId must be between 1 and 200 characters")
        if not isinstance(self.currency, str) or not _CURRENCY_RE.match(self.currency):
            raise RevenueError("Revenue currency must be a three-letter uppercase code")

        decimal_value = self.amount if self.kind == "transaction" else self.mrr_amount
        required: Dict[str, Any] = {}
        if self.kind == "transaction":
            required["amount"] = self.amount
        elif self.kind == "mrr_snapshot":
            required = {
                "subscriptionId": self.subscription_id,
                "customerId": self.customer_id,
                "mrrAmount": self.mrr_amount,
            }
        else:
            required["expectedActiveSubscriptions"] = self.expected_active_subscriptions
        for name, value in required.items():
            if value is None or (isinstance(value, str) and not value.strip()):
                raise RevenueError(f"{self.kind} requires {name}")

        if decimal_value is not None:
            _validate_decimal(decimal_value)
            if self.kind == "mrr_snapshot" and decimal_value.startswith("-"):
                raise RevenueError("MRR snapshot amount must be non-negative")
        subscriptions = self.expected_active_subscriptions
        if subscriptions is not None and (
            not isinstance(subscriptions, int) or isinstance(subscriptions, bool) or subscriptions < 0
        ):
            raise RevenueError("Expected active subscriptions must be a non-negative integer")

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"version": self.version, "kind": self.kind, "factId": self.fact_id}
        optional = {
            "amount": self.amount,
            "currency": self.currency,
            "customerId": self.customer_id,
            "subscriptionId": self.subscription_id,
            "mrrAmount": self.mrr_amount,
            "expectedActiveSubscriptions": self.expected_active_subscriptions,
        }
        for key, value in optional.items():
            if value is not None:
                payload[key] = value
        return payload


def _validate_decimal(value: str) -> None:
    if not isinstance(value, str) or not _DECIMAL_RE.match(value):
        raise RevenueError(
            "Revenue amounts must be non-exponent decimal strings with at most 9 fraction digits"
        )
    digits = value.lstrip("-").replace(".", "").lstrip("0")
    if len(digits) > _MAX_DECIMAL_PRECISION:
        raise RevenueError("Revenue amounts must not exceed 38 digits of precision")


@dataclass(frozen=True)
class AnalyticsEvent:
    """One event as it appears inside a batch on the wire."""

    event_id: str
    event: str
    event_type: EventType
    anonymous_id: str
    session_id: str
    timestamp: int
    properties: Dict[str, str]
    context: EventContext
    user_id: Optional[str] = None
    revenue: Optional[RevenuePayload] = None

    def __post_init__(self) -> None:
        # Copy defensively: the caller may mutate the mapping it handed over after the
        # event is queued, and a frozen dataclass would otherwise alias that dict.
        object.__setattr__(self, "properties", dict(self.properties))

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "eventId": self.event_id,
            "event": self.event,
            "eventType": self.event_type.value,
        }
        if self.user_id is not None:
            payload["userId"] = self.user_id
        payload["anonymousId"] = self.anonymous_id
        payload["sessionId"] = self.session_id
        payload["timestamp"] = self.timestamp
        payload["properties"] = dict(self.properties)
        if self.revenue is not None:
            payload["revenue"] = self.revenue.to_dict()
        payload["context"] = self.context.to_dict()
        return payload


@dataclass(frozen=True)
class BatchPayload:
    """Envelope POSTed to the ingest endpoint; retried verbatim on failure so the
    ``batchId`` stays stable for server-side dedup."""

    batch_id: str
    sent_at: int
    events: List[AnalyticsEvent]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batchId": self.batch_id,
            "sentAt": self.sent_at,
            "events": [event.to_dict() for event in self.events],
        }
