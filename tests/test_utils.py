from datetime import date
from typing import List
from yandex_logs_api.utils import get_day_intervals
import pytest


@pytest.mark.parametrize(
    "date_start,date_end,count,expected",
    [
        (
            date(2023, 1, 1),
            date(2023, 1, 10),
            5,
            [
                (date(2023, 1, 1), date(2023, 1, 5)),
                (date(2023, 1, 6), date(2023, 1, 10)),
            ],
        ),
        (
            date(2023, 1, 1),
            date(2023, 1, 10),
            9,
            [
                (date(2023, 1, 1), date(2023, 1, 9)),
                (date(2023, 1, 10), date(2023, 1, 10)),
            ],
        ),
        (
            date(2023, 1, 1),
            date(2023, 1, 10),
            12,
            [
                (date(2023, 1, 1), date(2023, 1, 10)),
            ],
        ),
        (
            date(2023, 1, 1),
            date(2023, 1, 3),
            1,
            [
                (date(2023, 1, 1), date(2023, 1, 1)),
                (date(2023, 1, 2), date(2023, 1, 2)),
                (date(2023, 1, 3), date(2023, 1, 3)),
            ],
        ),
    ],
)
def test_date_iterator(
    date_start: date,
    date_end: date,
    count: int,
    expected: list[tuple[date, date]],
) -> None:
    result = list(get_day_intervals(date_start, date_end, count))

    assert result == expected
