import ast
import datetime
import json
import logging
from urllib.parse import urlencode
import re
import aiohttp
from tenacity import (
    stop_after_attempt,
    wait_fixed,
    retry_if_exception,
    before_log,
    retry,
)

from mh_connectors.base.structure import Structure

logger = logging.getLogger("logs_api")

HOST = "https://api-metrika.yandex.ru/management/v1/counter/"
DATE_FORMAT = "%Y-%m-%d"


async def get_requests(token, counter_id):
    """Returns estimation of Logs API (whether it's
    possible to load data and max period in days)"""

    headers = {"Authorization": "OAuth " + token}

    url = "{host}{counter_id}/logrequests/".format(host=HOST, counter_id=counter_id)
    async with aiohttp.ClientSession() as session:
        response = await session.get(url, headers=headers)
        text = await response.text()

    if response.status != 200:
        raise ValueError(text)
    else:
        return json.loads(text)["requests"]


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(30),
    retry=retry_if_exception(ValueError),  # noqa
    before=before_log(logging.getLogger(), logging.INFO),
)
async def create_task(api_request):
    """Creates a Logs API task to generate data"""
    url_params = urlencode(
        [
            ("date1", api_request.date1_str),
            ("date2", api_request.date2_str),
            ("source", api_request.user_request.source),
            (
                "fields",
                ",".join(
                    sorted(
                        api_request.user_request.get_fields(), key=lambda s: s.lower()
                    )
                ),
            ),
        ]
    )

    url = (
        "{host}{counter_id}/logrequests?".format(
            host=HOST, counter_id=api_request.user_request.counter_id
        )
        + url_params
    )

    headers = {"Authorization": "OAuth " + api_request.user_request.token}

    async with aiohttp.ClientSession() as session:
        response = await session.post(url, headers=headers)
        text = await response.text()

    if response.status != 200:
        raise ValueError(text)

    response_object = json.loads(text)
    logger.debug(json.dumps(response_object["log_request"], indent=2))
    api_request.status = response_object["log_request"]["status"]
    api_request.request_id = response_object["log_request"]["request_id"]


async def update_status(api_request):
    """Returns current tasks\'s status"""
    url = "{host}{counter_id}/logrequest/{request_id}".format(
        request_id=api_request.request_id,
        counter_id=api_request.user_request.counter_id,
        host=HOST,
    )

    headers = {"Authorization": "OAuth " + api_request.user_request.token}

    async with aiohttp.ClientSession() as session:
        response = await session.get(url, headers=headers)
        text = await response.text()

    if response.status != 200:
        raise ValueError(text)
    response_object = json.loads(text)

    status = response_object["log_request"]["status"]
    api_request.status = status
    if status == "processed":
        size = len(response_object["log_request"]["parts"])
        api_request.size = size
    return api_request


def fix_value(v):
    if len(v) > 2 and (
        v[0] == "["
        or v[1] == "["
        or ((v[0] == "{" or v[1] == "{") and ('"' in v or "'" in v))
    ):
        v = v.replace("[\\'", "['").replace("\\']", "']").replace("\\',\\'", "','")
        if v[0] == '"':
            v = v.replace('""', "'")
        try:
            return ast.literal_eval(v)
        except ValueError:
            return v

    v = v.replace("\\'", "'").replace("'", '"')
    return v


async def load_data(api_request, part) -> list:
    url = "{host}{counter_id}/logrequest/{request_id}/part/{part}/download"
    url = url.format(
        host=HOST,
        counter_id=api_request.user_request.counter_id,
        request_id=api_request.request_id,
        part=part,
    )

    headers = {"Authorization": "OAuth " + api_request.user_request.token}
    async with aiohttp.ClientSession() as session:
        response = await session.get(url, headers=headers)
        text = await response.text()

    if response.status != 200:
        raise ValueError(text)

    text_lines = text.split("\n")

    headers_num = len(text_lines[0].split("\t"))

    splitted_text_filtered = [
        line for line in text_lines if len(line.split("\t")) == headers_num
    ]

    num_filtered = len(text_lines) - len(splitted_text_filtered)
    if num_filtered > 1:
        logger.warning("%d rows were filtered out" % num_filtered)

    if len(splitted_text_filtered) < 2:
        logger.warning("No data")
        return []
    headers_data = [clean_field_name(h) for h in splitted_text_filtered[0].split("\t")]

    data = [
        {
            headers_data[i]: fix_value(v)
            for i, v in enumerate(row.split("\t"))
            if v != "[]" and v != "[\\'\\']" and v != "\\'\\'"
        }
        for row in splitted_text_filtered[1:]
    ]
    return data


async def clean_data(api_request):
    """Cleans generated data on server"""
    url = "{host}{counter_id}/logrequest/{request_id}/clean"
    url = url.format(
        host=HOST,
        counter_id=api_request.download.counter_id,
        request_id=api_request.request_id,
    )

    headers = {"Authorization": "OAuth " + api_request.download.token}
    async with aiohttp.ClientSession() as session:
        r = await session.post(url, headers=headers)
        if r.status != 200:
            raise ValueError(await r.text())


async def cancel_data(api_request):
    """Cleans generated data on server"""
    url = "{host}{counter_id}/logrequest/{request_id}/cancel"
    url = url.format(
        host=HOST,
        counter_id=api_request.download.counter_id,
        request_id=api_request.request_id,
    )

    headers = {"Authorization": "OAuth " + api_request.download.token}
    async with aiohttp.ClientSession() as session:
        r = await session.post(url, headers=headers)
        if r.status != 200:
            raise ValueError(await r.text())
