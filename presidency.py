import re

import pandas as pd

from html_table_parser import load_html_table

URL = "https://www.britannica.com/topic/Presidents-of-the-United-States-1846696"


def _normalize_year_fragment(fragment: str, anchor_year: int | None = None) -> int | None:
    """Return a four-digit year from a fragment such as '97 or 1901."""
    if not fragment:
        return None
    digits = re.findall(r"\d+", fragment)
    if not digits:
        return None
    number = digits[-1]
    if len(number) == 4:
        return int(number)
    if len(number) == 2 and anchor_year:
        century = (anchor_year // 100) * 100
        candidate = century + int(number)
        if candidate < anchor_year:
            candidate += 100
        return candidate
    return int(number)


def _split_term_range(term: str) -> tuple[int | None, int | None]:
    if not isinstance(term, str):
        return (None, None)
    cleaned = re.sub(r"[\*\u2020]", "", term)
    cleaned = cleaned.replace("—", "–").replace("-", "–")
    parts = [p.strip() for p in cleaned.split("–") if p.strip()]
    if not parts:
        return (None, None)
    start_year = _normalize_year_fragment(parts[0])
    end_year = (
        _normalize_year_fragment(parts[1], anchor_year=start_year) if len(parts) > 1 else start_year
    )
    return (start_year, end_year)


# Load the Britannica presidents table without relying on bs4/lxml.
df = load_html_table(URL)

# Drop any unnamed/empty columns that show up in the raw HTML table.
df = df.loc[:, df.columns.astype(str).str.strip().astype(bool)]

# Clean and rename columns
df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
df = df.rename(
    columns={
        "no.": "president_number",
        "president": "name",
        "birthplace": "birthplace",
        "political_party": "party",
        "term": "term_of_office",
    }
)

# Remove note rows (e.g., "*Died in office") and unwanted characters/footnotes.
df = df.replace(r"\[.*?\]", "", regex=True)
df = df.replace({"president_number": {r"\D": ""}}, regex=True)
df = df[df["president_number"].str.strip().astype(bool)]
df["president_number"] = pd.to_numeric(df["president_number"], errors="coerce")
df = df.dropna(subset=["president_number"])
df["president_number"] = df["president_number"].astype(int)
df["term_of_office"] = df["term_of_office"].astype(str).str.strip()

# Split the term range into start / end columns with century-aware parsing.
term_bounds = df["term_of_office"].apply(_split_term_range)
df["term_start"] = term_bounds.apply(lambda x: x[0])
df["term_end"] = term_bounds.apply(lambda x: x[1])


# Save to CSV
df.to_csv("data/metadata/us_presidents_britannica.csv", index=False)
print("Saved", len(df), "records to us_presidents_britannica.csv")
print(df.head())
