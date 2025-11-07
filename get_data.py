import os
import re
from html.parser import HTMLParser
from urllib.error import URLError
from urllib.request import Request, urlopen
import numpy as np

import pandas as pd


class _SimpleHTMLTableParser(HTMLParser):
    """Very small HTML table parser that does not rely on lxml/bs4."""

    def __init__(self):
        super().__init__()
        self.tables = []
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


def _normalize_header(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value.replace("*", " ")).strip()
    return cleaned


def _normalize_row(row, width):
    padded = row + [""] * max(0, width - len(row))
    return padded[:width]


def _scrape_table_without_optional_dependencies(url: str) -> pd.DataFrame:
    req = Request(url, headers={"User-Agent": "gov-shutdown-parser/1.0"})
    try:
        with urlopen(req) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
    except URLError as exc:
        raise RuntimeError(f"Unable to fetch {url}: {exc}") from exc

    parser = _SimpleHTMLTableParser()
    parser.feed(html)
    if not parser.tables:
        raise ValueError(f"No HTML tables were found at {url}")

    header, rows = parser.tables[0]
    columns = [_normalize_header(col) for col in header]
    normalized_rows = [_normalize_row(row, len(columns)) for row in rows if any(cell.strip() for cell in row)]
    return pd.DataFrame(normalized_rows, columns=columns)


def _load_shutdown_table(url: str) -> pd.DataFrame:
    try:
        return pd.read_html(url)[0]
    except (ImportError, ValueError):
        print("pandas.read_html optional dependencies not available; falling back to built-in parser.")
        return _scrape_table_without_optional_dependencies(url)


def collect_shutdowns(output_path="data/metadata/shutdowns_master.csv"):
    url = "https://history.house.gov/Institution/Shutdown/Government-Shutdowns/"
    print(f"Fetching shutdown table from {url} ...")

    # Step 1: Scrape table (with fallback that does not require lxml/bs4)
    df = _load_shutdown_table(url)

    # Step 2: Clean columns
    df.columns = [_normalize_header(c) for c in df.columns]
    df.rename(
        columns={
            "Fiscal Year": "fiscal_year",
            "Date Funding Ended": "funding_gap_start",
            "Duration of Funding Gap (in Days)": "duration_days",
            "Date Funding Restored": "funding_gap_end",
            "Shutdown Procedures Followed": "shutdown_flag",
            "Legislation Restoring Funding": "restoring_legislation",
        },
        inplace=True,
    )

    # Step 3: Normalize date fields and derived metadata
    df["duration_days"] = pd.to_numeric(df["duration_days"], errors="coerce")
    df["shutdown_flag"] = df["shutdown_flag"].str.contains("Yes", na=np.nan)

    def _build_date_range(row):
        parts = []
        for col in ("funding_gap_start", "funding_gap_end"):
            value = row[col]
            if pd.notna(value):
                text = str(value).strip()
                if text:
                    parts.append(text)
        return " – ".join(parts)

    df["date_range"] = df.apply(_build_date_range, axis=1)
    df["start_date"] = pd.to_datetime(df["funding_gap_start"], errors="coerce")
    df["end_date"] = pd.to_datetime(df["funding_gap_end"], errors="coerce")

    # Step 4: Add placeholder metadata columns for manual enrichment
    df["president"] = None
    df["party_control_house"] = None
    df["party_control_senate"] = None
    df["major_issue"] = None
    df["notes"] = None

    # Step 5: Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"✅ Saved cleaned shutdown dataset to {output_path}")
    return df


if __name__ == "__main__":
    df = collect_shutdowns()
    print(df.head())
