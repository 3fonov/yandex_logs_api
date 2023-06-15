import logging
import os
from typing import Any, Callable

import pytest
from dotenv import load_dotenv

from yandex_logs_api import LogsAPI

load_dotenv()


@pytest.fixture(scope="module")
def vcr_config() -> dict[str, Any]:
    return {"record_mode": "new_episodes", "filter_headers": ["authorization"]}


@pytest.fixture
def logs_api() -> LogsAPI:
    token = os.environ.get("TOKEN")
    assert token, "Set token into ENVVAR TOKEN"

    counter_id = os.environ.get("COUNTER_ID")
    assert counter_id, "Set counter_id into ENVVAR COUNTER_ID"

    return LogsAPI(int(counter_id), token)


@pytest.fixture
def assert_logs_api_initialized() -> Callable[..., None]:
    def func(logs_api_obj: LogsAPI) -> None:
        assert logs_api_obj.counter_id == int(os.environ.get("COUNTER_ID", "0"))
        assert logs_api_obj.token == os.environ.get("TOKEN")
        assert logs_api_obj.api_url == f"{logs_api_obj.HOST}{logs_api_obj.counter_id}/"

    return func
