import os

import pandas as pd

from html_table_parser import load_html_table, normalize_header


def collect_shutdowns(output_path="data/metadata/shutdowns_master.csv"):
    url = "https://history.house.gov/Institution/Shutdown/Government-Shutdowns/"
    print(f"Fetching shutdown table from {url} ...")

    # Step 1: Scrape table (with fallback that does not require lxml/bs4)
    df = load_html_table(url)

    # Step 2: Clean columns
    df.columns = [normalize_header(c) for c in df.columns]
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
    df["shutdown_flag"] = df["shutdown_flag"].str.contains('Yes')

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
    # df["president"] = None
    # df["party_control_house"] = None
    # df["party_control_senate"] = None
    # df["major_issue"] = None
    # df["notes"] = None

    # Step 5: Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"✅ Saved cleaned shutdown dataset to {output_path}")
    return df


if __name__ == "__main__":
    df = collect_shutdowns()
    print(df.head())
