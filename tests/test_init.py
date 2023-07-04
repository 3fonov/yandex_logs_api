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
    assert requests[0]
    assert requests[0].request_id
    assert requests[0].status == LogRequestStatus.PROCESSED


# @pytest.mark.vcr
@pytest.mark.asyncio
async def test_download(logs_api: LogsAPI) -> None:
    data = [
        row
        async for row in logs_api.download_report(
            date(2023, 3, 1),
            date(2023, 3, 10),
            LogRequestSource.HITS,
            [
                "ym:pv:watchID",
                "ym:pv:counterID",
                "ym:pv:dateTime",
                "ym:pv:title",
                "ym:pv:URL",
                "ym:pv:referer",
                "ym:pv:browser",
                "ym:pv:UTMCampaign",
                "ym:pv:UTMContent",
                "ym:pv:UTMMedium",
                "ym:pv:UTMSource",
                "ym:pv:deviceCategory",
                "ym:pv:operatingSystem",
                "ym:pv:regionCity",
                "ym:pv:regionCountry",
                "ym:pv:lastTrafficSource",
                "ym:pv:lastSearchEngineRoot",
                "ym:pv:lastSearchEngine",
                "ym:pv:lastAdvEngine",
                "ym:pv:params",
                "ym:pv:clientID",
                "ym:pv:goalsID",
                "ym:pv:date",
                "ym:pv:GCLID",
                "ym:pv:regionCityID",
                "ym:pv:regionCountryID",
                "ym:pv:isPageView",
                "ym:pv:parsedParamsKey1",
                "ym:pv:parsedParamsKey2",
                "ym:pv:parsedParamsKey3",
                "ym:pv:parsedParamsKey4",
                "ym:pv:parsedParamsKey5",
                "ym:pv:parsedParamsKey6",
                "ym:pv:parsedParamsKey7",
                "ym:pv:parsedParamsKey8",
                "ym:pv:parsedParamsKey9",
                "ym:pv:parsedParamsKey10",
            ],
        )
    ]
    await logs_api.clean_report()
    assert len(data) > 0
