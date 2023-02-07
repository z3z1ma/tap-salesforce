"""Tests standard tap features using the built-in SDK tests library."""
from singer_sdk.testing import get_standard_tap_tests

from tap_salesforce.tap import TapSalesforce

BASE_CONFIG = {
    "api_type": "BULK",
    "start_date": "2021-01-01T00:00:00Z",
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapSalesforce, config=BASE_CONFIG)
    for test in tests:
        test()


if __name__ == "__main__":
    # Run a sync
    TapSalesforce(config=BASE_CONFIG).sync_all()
