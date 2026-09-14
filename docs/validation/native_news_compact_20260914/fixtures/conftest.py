import sys
import pytest


def pytest_sessionstart(session):
    if getattr(sys, '_native_news_reproduction_external_actions_blocked', False) is not True:
        raise pytest.UsageError('portable reproduction requires its isolated sitecustomize guard')
