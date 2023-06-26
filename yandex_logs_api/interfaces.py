import logging
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any, AsyncGenerator, Iterator, List, Optional, Tuple, TypedDict

import aiohttp

from yandex_logs_api.fields import MetrikaFields
from yandex_logs_api.utils import clean_field_name, fix_value

logger = logging.getLogger(__name__)


class LogRequestSource(str, Enum):
    VISITS = "visits"
    HITS = "hits"


class HTTPMethod(Enum):
    GET = 0
    POST = 1


class APIParams(TypedDict, total=False):
    date1: str
    date2: str
    source: str
    fields: str


class LogRequestStatus(str, Enum):
    NEW = "new"
    PROCESSED = "processed"
    CANCELED = "canceled"
    PROCESSING_FAILED = "processing_failed"
    CREATED = "created"
    CLEANED_BY_USER = "cleaned_by_user"
    CLEANED_AUTOMATICALLY_AS_TOO_OLD = "cleaned_automatically_as_too_old"


@dataclass
class LogRequestPart:
    part_number: int
    size: int

    def __iter__(self: "LogRequestPart") -> Iterator[int]:
        return iter((self.part_number, self.size))


@dataclass
class LogRequest:  # noqa
    date1: str
    date2: str
    source: LogRequestSource
    fields: MetrikaFields
    status: LogRequestStatus = LogRequestStatus.NEW
    attribution: Optional[str] = None
    request_id: Optional[int] = None
    counter_id: Optional[int] = None
    size: Optional[int] = None
    parts: Optional[List[LogRequestPart]] = None

    def __post_init__(self: "LogRequest") -> None:
        if self.parts:
            self.parts = [LogRequestPart(**part) for part in self.parts]

    def __hash__(self: "LogRequest") -> int:
        return hash(self.date1 + self.date2 + self.source + str(self.fields))

    def update(self: "LogRequest", value: "LogRequest") -> None:
        self.request_id = value.request_id
        self.counter_id = value.counter_id
        self.attribution = value.attribution
        self.parts = value.parts
        self.status = LogRequestStatus[value.status.upper()]

    @property
    def date_start(self: "LogRequest") -> date:
        return date.fromisoformat(self.date1)

    @date_start.setter
    def date_start(self: "LogRequest", value: date) -> None:
        self.date1 = value.isoformat()

    @property
    def date_end(self: "LogRequest") -> date:
        return date.fromisoformat(self.date2)

    @date_end.setter
    def date_end(self: "LogRequest", value: date) -> None:
        self.date2 = value.isoformat()

    def get_api_params(self: "LogRequest") -> APIParams:
        return APIParams(
            date1=self.date_start.isoformat(),
            date2=self.date_end.isoformat(),
            source=self.source.value,
            fields=",".join(self.fields),
        )


@dataclass
class LogRequestEvaluation:
    possible: bool
    max_possible_day_quantity: int
    expected_size: int
    log_request_sum_max_size: int
    log_request_sum_size: int


@dataclass
class EvaluateEndpoint:
    session: aiohttp.ClientSession
    api_url: str
    request: LogRequest

    async def __call__(
        self: "EvaluateEndpoint",
    ) -> Tuple[dict[str, Any], Optional[int]]:
        async with self.session.get(
            f"{self.api_url}logrequests/evaluate",
            params=self.request.get_api_params(),
        ) as response:
            response.raise_for_status()
            return await response.json(), response.content_length


@dataclass
class LogEndpoint:
    session: aiohttp.ClientSession
    api_url: str
    request: LogRequest

    async def __call__(self: "LogEndpoint") -> Tuple[dict[str, Any], Optional[int]]:
        async with self.session.post(
            f"{self.api_url}logrequests",
            params=self.request.get_api_params(),
        ) as response:
            response.raise_for_status()
            return await response.json(), response.content_length


@dataclass
class LogRequestEndpoint:
    session: aiohttp.ClientSession
    api_url: str
    request: LogRequest

    async def __call__(
        self: "LogRequestEndpoint",
    ) -> Tuple[dict[str, Any], Optional[int]]:
        async with self.session.get(
            f"{self.api_url}logrequest/{self.request.request_id}",
        ) as response:
            response.raise_for_status()
            return await response.json(), response.content_length


@dataclass
class CleanRequestEndpoint:
    session: aiohttp.ClientSession
    api_url: str
    request: LogRequest

    async def __call__(
        self: "CleanRequestEndpoint",
    ) -> dict[str, Any]:
        async with self.session.post(
            f"{self.api_url}logrequest/{self.request.request_id}/clean",
        ) as response:
            response.raise_for_status()
            logger.info("Cleaning request  %s", (self.request.request_id))
            return await response.json()


@dataclass
class DownloadRequestEndpoint:
    session: aiohttp.ClientSession
    api_url: str
    request: LogRequest

    async def __call__(
        self: "DownloadRequestEndpoint",
    ) -> Tuple[AsyncGenerator[list[dict[str, Any | str]], None], Optional[int]]:
        base_url = f"{self.api_url}logrequest/{self.request.request_id}/part/"
        if not self.request.parts:
            return
        for part in self.request.parts:
            url = f"{base_url}{part.part_number}/download"
            logger.info(
                "Downloaded part %s of %s of %s"
                % (
                    part.part_number + 1,
                    len(self.request.parts),
                    self.request.request_id,
                ),
            )
            async with self.session.get(url) as response:
                response.raise_for_status()
                response_text = await response.text()
                response_size: int = response.content_length

            cleaned_text = self.clean_text(response_text)

            if len(cleaned_text) < 2:
                yield [], response_size

            headers_data = [clean_field_name(h) for h in cleaned_text[0].split("\t")]

            yield [
                {headers_data[i]: fix_value(v) for i, v in enumerate(row.split("\t"))}
                for row in cleaned_text[1:]
            ], response_size

    def clean_text(self: "DownloadRequestEndpoint", response_text: str) -> list[str]:
        text_lines = response_text.split("\n")

        headers_num = len(text_lines[0].split("\t"))

        return [line for line in text_lines if len(line.split("\t")) == headers_num]
