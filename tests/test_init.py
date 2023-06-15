import logging
import pytest
from datetime import date
from typing import Callable
from yandex_logs_api.interfaces import LogRequestSource, LogRequestStatus
from yandex_logs_api.logs_api import LogsAPI


def test_instance_create(
    logs_api: LogsAPI,
    assert_logs_api_initialized: Callable[..., None],
) -> None:
    assert_logs_api_initialized(logs_api)


@pytest.mark.vcr
@pytest.mark.asyncio
async def test_get_estimation(logs_api: LogsAPI) -> None:
    logs_api.create_request(
        date(2023, 1, 1),
        date(2023, 1, 1),
        LogRequestSource.VISITS,
        ["ym:s:pageViews"],
    )
    estimation = await logs_api.get_estimation()

    assert estimation
    assert estimation.possible


@pytest.mark.vcr
@pytest.mark.asyncio
async def test_create_requests(logs_api: LogsAPI) -> None:
    logs_api.create_request(
        date(2023, 1, 1),
        date(2023, 1, 1),
        LogRequestSource.VISITS,
        ["ym:s:pageViews"],
    )

    await logs_api.create_api_requests()

    assert len(logs_api.requests) == 1


@pytest.mark.vcr
@pytest.mark.asyncio
async def test_put_requests(logs_api: LogsAPI) -> None:
    logs_api.create_request(
        date(2023, 1, 1),
        date(2023, 1, 1),
        LogRequestSource.VISITS,
        ["ym:s:pageViews"],
    )

    await logs_api.create_api_requests()
    requests = [request async for request in logs_api.process_requests()]

    assert len(requests) == 1
    assert requests[0].request_id
    assert requests[0].status == LogRequestStatus.PROCESSED


@pytest.mark.vcr
@pytest.mark.asyncio
async def test_download(logs_api: LogsAPI) -> None:
    data = [
        row
        async for row in logs_api.download_report(
            date(2023, 1, 1),
            date(2023, 1, 1),
            LogRequestSource.VISITS,
            ["ym:s:pageViews"],
        )
    ]
    await logs_api.clean_report()
    await logs_api.session.close()
    assert len(data) > 0
