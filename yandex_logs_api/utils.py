import ast
from datetime import date, timedelta
import re
from typing import Any, Generator


def get_day_intervals(
    date_start: date,
    date_end: date,
    day_quantity: int,
) -> Generator[tuple[date, date], None, None]:
    new_date_start = date_start
    new_date_end = new_date_start + timedelta(days=day_quantity - 1)
    while new_date_end < date_end:
        yield new_date_start, new_date_end
        new_date_start = new_date_end + timedelta(days=1)
        new_date_end = new_date_start + timedelta(days=day_quantity - 1)

    yield new_date_start, date_end


def clean_field_name(field_name: str) -> str:
    fixes = [
        ("ym:pv:", ""),
        ("ym:s:", ""),
        ("GCLID", "Gclid"),
        ("ID", "Id"),
        ("UTC", "Utc"),
        ("URL", "Url"),
        ("UTM", "Utm"),
    ]
    for f in fixes:
        field_name = field_name.replace(*f)

    return re.sub(r"(?<!^)(?=[A-Z])", "_", field_name).lower()


def fix_value(v: str) -> Any | str:
    if v in {"[]", "[\\'\\']"}:
        return []
    if v == "\\'\\'":
        return ""
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
        except SyntaxError:
            return v

    return v.replace("\\'", "'").replace("'", '"')
