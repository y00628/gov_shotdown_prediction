import re
from html.parser import HTMLParser
from typing import List, Tuple
from urllib.error import URLError
from urllib.request import Request, urlopen

import pandas as pd


class SimpleHTMLTableParser(HTMLParser):
    """Very small HTML table parser that does not rely on lxml/bs4."""

    def __init__(self):
        super().__init__()
        self.tables: List[Tuple[List[str], List[List[str]]]] = []
        self._in_table = False
        self._current_header = None
        self._current_rows = None
        self._current_row = None
        self._current_cell = None

    def handle_starttag(self, tag, attrs):
        if tag == "table" and not self._in_table:
            self._in_table = True
            self._current_header = None
            self._current_rows = []
        elif self._in_table and tag == "tr":
            self._current_row = []
        elif self._in_table and tag in ("td", "th"):
            self._current_cell = []
        elif self._in_table and tag == "br" and self._current_cell is not None:
            self._current_cell.append("\n")

    def handle_endtag(self, tag):
        if tag == "table" and self._in_table:
            self.tables.append((self._current_header or [], self._current_rows or []))
            self._in_table = False
            self._current_header = None
            self._current_rows = None
        elif self._in_table and tag in ("td", "th") and self._current_cell is not None:
            text = "".join(self._current_cell).strip()
            self._current_row.append(text)
            self._current_cell = None
        elif self._in_table and tag == "tr" and self._current_row is not None:
            if self._current_header is None:
                self._current_header = self._current_row
            else:
                self._current_rows.append(self._current_row)
            self._current_row = None

    def handle_data(self, data):
        if self._in_table and self._current_cell is not None:
            self._current_cell.append(data)


def normalize_header(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value.replace("*", " ")).strip()
    return cleaned


def _normalize_row(row, width):
    padded = row + [""] * max(0, width - len(row))
    return padded[:width]


def _scrape_tables_without_optional_dependencies(url: str) -> List[pd.DataFrame]:
    req = Request(url, headers={"User-Agent": "gov-shutdown-parser/1.0"})
    try:
        with urlopen(req) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
    except URLError as exc:
        raise RuntimeError(f"Unable to fetch {url}: {exc}") from exc

    parser = SimpleHTMLTableParser()
    parser.feed(html)
    if not parser.tables:
        raise ValueError(f"No HTML tables were found at {url}")

    frames = []
    for header, rows in parser.tables:
        columns = [normalize_header(col) for col in header]
        normalized_rows = [
            _normalize_row(row, len(columns)) for row in rows if any(cell.strip() for cell in row)
        ]
        frames.append(pd.DataFrame(normalized_rows, columns=columns))
    return frames


def read_html_tables(url: str) -> List[pd.DataFrame]:
    """Mirror pandas.read_html but with a pure-Python fallback."""
    try:
        return pd.read_html(url)
    except (ImportError, ValueError):
        print("pandas.read_html optional dependencies not available; using built-in parser.")
        return _scrape_tables_without_optional_dependencies(url)


def load_html_table(url: str, table_index: int = 0) -> pd.DataFrame:
    tables = read_html_tables(url)
    if table_index >= len(tables):
        raise IndexError(f"Table index {table_index} out of range for {url}")
    return tables[table_index]
