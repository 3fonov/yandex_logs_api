import asyncio
import logging
from datetime import date, timedelta
from typing import Any, AsyncGenerator

import aiohttp
from tenacity import retry, retry_if_result, stop_after_attempt, wait_exponential

from yandex_logs_api.fields import MetrikaFields
from yandex_logs_api.interfaces import (
    CancelRequestEndpoint,
    CleanRequestEndpoint,
    DownloadRequestEndpoint,
    EvaluateEndpoint,
    LogEndpoint,
    LogRequest,
    LogRequestEndpoint,
    LogRequestEvaluation,
    LogRequestsEndpoint,
    LogRequestSource,
    LogRequestStatus,
)
from yandex_logs_api.utils import get_day_intervals


class LogsAPI:
    HOST = "https://api-metrika.yandex.ru/management/v1/counter/"

    request: LogRequest
    requests: set[LogRequest]
    _session: aiohttp.ClientSession | None = None

    def __init__(
        self: "LogsAPI", counter_id: int, token: str, logger: logging.Logger | None
    ) -> None:
        self.counter_id = counter_id
        self.token = token
        self.api_url = f"{self.HOST}{counter_id}/"
        self.bytes_loaded = 0
        self.rows_loaded = 0
        self.requests = set()
        if logger:
            self.logger = logger
        else:
            self.setup_logging()
        self.logger.info("Initialized CID: %s" % self.counter_id)

    @property
    def session(self: "LogsAPI") -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            headers = {"Authorization": "OAuth " + self.token}
            self._session = aiohttp.ClientSession(headers=headers)
        return self._session

    def setup_logging(self: "LogsAPI") -> None:
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        if not self.logger.hasHandlers():
            # define handler and formatter
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s %(levelname)s.%(name)s: %(message)s",
            )

            # add formatter to handler
            handler.setFormatter(formatter)

            # add handler to self.logger
            self.logger.addHandler(handler)

    async def clean_up(self: "LogsAPI") -> None:
        requests = await LogRequestsEndpoint(self.session, self.api_url)()
        for request in requests:
            if request.status == LogRequestStatus.PROCESSED:
                await CleanRequestEndpoint(self.session, self.api_url, request)()
            if request.status == LogRequestStatus.CREATED:
                await CancelRequestEndpoint(self.session, self.api_url, request)()

    async def download_report(
        self: "LogsAPI",
        date_start: date,
        date_end: date,
        source: LogRequestSource,
        fields: MetrikaFields,
    ) -> AsyncGenerator[dict[str, Any | str], None]:
        self.logger.debug(
            "Downloading  %s report from %s to %s" % (source, date_start, date_end),
        )
        self.create_request(date_start, date_end, source, fields)
        await self.create_api_requests()
        async for loaded_request in self.process_requests():
            if not loaded_request:
                continue
            async for request_data, bytes_loaded in DownloadRequestEndpoint(
                self.session,
                self.api_url,
                loaded_request,
            )():
                self.bytes_loaded += bytes_loaded or 0
                self.rows_loaded += len(request_data)
                yield request_data
            await CleanRequestEndpoint(
                self.session,
                self.api_url,
                loaded_request,
            )()
        self.logger.info(
            "Downloaded report",
        )

    async def clean_report(self: "LogsAPI") -> None:
        if self._session:
            await self._session.close()

        self.requests = set()
        self.bytes_loaded = 0
        self.rows_loaded = 0

    def create_request(
        self: "LogsAPI",
        date_start: date,
        date_end: date,
        source: LogRequestSource,
        fields: MetrikaFields,
    ) -> None:
        if date_start > date_end:
            raise RuntimeError("Start date cannot be after end date")
        if date_end >= date.today() - timedelta(days=1):
            raise RuntimeError("End date must by a day before yesterday")

        self.request = LogRequest(
            date1=date_start.isoformat(),
            date2=date_end.isoformat(),
            source=source,
            fields=fields,
        )

    async def get_estimation(self: "LogsAPI") -> LogRequestEvaluation:
        if not self.request:
            raise RuntimeError("request not set")
        data, bytes_loaded = await EvaluateEndpoint(
            self.session,
            self.api_url,
            self.request,
        )()
        self.bytes_loaded += bytes_loaded or 0
        return LogRequestEvaluation(**data["log_request_evaluation"])

    async def create_api_requests(self: "LogsAPI") -> None:
        estimation = await self.get_estimation()
        if estimation.max_possible_day_quantity == 0:
            raise RuntimeError(
                "Logs API can't load data: max_possible_day_quantity = 0",
            )

        if estimation.possible:
            self.logger.info("Estimated as possible")
            self.requests.add(self.request)
            return
        self.logger.info("Estimated as possible but need to be chunked")
        for date_start, date_end in get_day_intervals(
            date_start=self.request.date_start,
            date_end=self.request.date_end,
            day_quantity=estimation.max_possible_day_quantity,
        ):
            request = LogRequest(
                date1=date_start.isoformat(),
                date2=date_end.isoformat(),
                source=self.request.source,
                fields=self.request.fields,
            )
            self.logger.info("Creating request from %s to %s" % (date_start, date_end))
            self.requests.add(request)

    async def process_requests(
        self: "LogsAPI",
    ) -> AsyncGenerator[LogRequest | None, Any]:
        for request in self.requests:
            yield await self.process_request(request)

    @retry(
        retry=retry_if_result(lambda value: value is None),
        stop=stop_after_attempt(100),
        wait=wait_exponential(multiplier=1, min=4, max=180),
    )
    async def process_request(
        self: "LogsAPI", request: LogRequest
    ) -> LogRequest | None:
        data = await self.get_request_data(request)
        current_request = LogRequest(**data)
        request.update(current_request)

        if request.status == LogRequestStatus.PROCESSED:
            self.logger.info("Request %s: %s" % (request.request_id, request.status))
            return request
        if request.status in (
            LogRequestStatus.NEW,
            LogRequestStatus.CREATED,
            LogRequestStatus.AWAITING_RETRY,
        ):
            self.logger.info("Request %s: %s" % (request.request_id, request.status))
            return None
        raise RuntimeError(f"Wrong status {request.status}")

    async def get_request_data(self: "LogsAPI", request: LogRequest) -> dict[str, Any]:
        if request.request_id:
            data, bytes_loaded = await LogRequestEndpoint(
                self.session,
                self.api_url,
                request,
            )()

        else:
            data, bytes_loaded = await LogEndpoint(
                self.session,
                self.api_url,
                request,
            )()
        self.bytes_loaded += bytes_loaded or 0
        if "log_request" not in data:
            raise RuntimeError(f"log_request not found in response {data}")
        return data["log_request"]
