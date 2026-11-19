"""Shared test fixtures. One local Spark session for the whole test run."""

import pytest


@pytest.fixture(scope="session")
def spark():
    from common.spark import get_spark

    session = get_spark("tests")
    yield session
    session.stop()
