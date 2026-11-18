import pytest
from pureib import backend
from pureib.testing import FakeFabric


@pytest.fixture
def fabric():
    """Route all verbs calls through the in-process fabric for the duration of a test."""
    fab = FakeFabric()
    backend.set_provider(fab)
    try:
        yield fab
    finally:
        backend.set_provider(None)
