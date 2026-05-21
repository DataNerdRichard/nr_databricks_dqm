"""
Send custom events to New Relic via the Events API.
Used to report Databricks data quality check results.
"""

import json
import os
import time
from typing import Any

import requests

NR_EVENTS_ENDPOINT_US = "https://insights-collector.newrelic.com/v1/accounts/{account_id}/events"
NR_EVENTS_ENDPOINT_EU = "https://insights-collector.eu01.nr-data.net/v1/accounts/{account_id}/events"


class NREventsClient:
    def __init__(self, data_center: str = "US"):
        self.account_id = os.environ.get("NEW_RELIC_ACCOUNT_ID")
        self.license_key = os.environ.get("NEW_RELIC_LICENSE_KEY")

        if not self.account_id or not self.license_key:
            raise ValueError(
                "NEW_RELIC_ACCOUNT_ID and NEW_RELIC_LICENSE_KEY must be set in ~/.zshrc"
            )

        template = NR_EVENTS_ENDPOINT_EU if data_center == "EU" else NR_EVENTS_ENDPOINT_US
        self.endpoint = template.format(account_id=self.account_id)

    def send(self, event_type: str, attributes: dict[str, Any]) -> None:
        """Send a single custom event to New Relic."""
        payload = {"eventType": event_type, "timestamp": int(time.time()), **attributes}
        response = requests.post(
            self.endpoint,
            data=json.dumps(payload),
            headers={
                "Content-Type": "application/json",
                "Api-Key": self.license_key,
            },
            timeout=10,
        )
        response.raise_for_status()

    def send_batch(self, event_type: str, rows: list[dict[str, Any]]) -> None:
        """Send a batch of events (max 2000 per call)."""
        ts = int(time.time())
        payload = [{"eventType": event_type, "timestamp": ts, **r} for r in rows]
        for i in range(0, len(payload), 2000):
            response = requests.post(
                self.endpoint,
                data=json.dumps(payload[i : i + 2000]),
                headers={
                    "Content-Type": "application/json",
                    "Api-Key": self.license_key,
                },
                timeout=30,
            )
            response.raise_for_status()
