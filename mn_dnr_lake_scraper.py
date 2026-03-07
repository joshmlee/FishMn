"""
MN DNR LakeFinder Survey Scraper
Pulls fish survey data for all lakes in a given county.
"""

import json
import re
import time
import csv
import os
import requests

GAZETTEER_URL = "https://maps.dnr.state.mn.us/cgi-bin/gazetteer/gazetteer2.cgi"
DETAIL_URL = "https://maps.dnr.state.mn.us/cgi-bin/lakefinder/detail.cgi"

COUNTY_ID = "38"
COUNTY_NAME = "Lake"
OUTPUT_DIR = "lake_survey_data"


def fetch_jsonp(url: str, params: dict) -> dict:
    """Fetch a JSONP endpoint and return parsed JSON."""
    params["callback"] = "cb"
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    # Strip JSONP wrapper: cb({...}) or cb([...])
    text = response.text.strip()
    match = re.match(r"^\w+\((.*)\);?$", text, re.DOTALL)
    if match:
        return json.loads(match.group(1))
    return json.loads(text)


def get_lakes_in_county(county_id: str) -> list[dict]:
    """Return list of lakes for the given county ID."""
    data = fetch_jsonp(GAZETTEER_URL, {"type": "lake", "county": county_id})
    if data.get("status") == "ERROR":
        raise RuntimeError(f"Gazetteer error: {data.get('message')}")
    return data.get("results", [])


def get_lake_surveys(dow_number: str) -> dict:
    """Return survey data for a lake by its DOW number."""
    data = fetch_jsonp(DETAIL_URL, {"type": "lake_survey", "id": dow_number})
    if data.get("status") == "ERROR":
        return {}
    return data.get("result", {})


def flatten_catch_summary(dow: str, lake_name: str, survey: dict, catch: dict) -> dict:
    """Flatten one fishCatchSummary row for CSV output."""
    return {
        "dow_number": dow,
        "lake_name": lake_name,
        "survey_id": survey.get("surveyID", ""),
        "survey_date": survey.get("surveyDate", ""),
        "survey_type": survey.get("surveyType", ""),
        "survey_sub_type": survey.get("surveySubType", ""),
        "species": catch.get("species", ""),
        "gear": catch.get("gear", ""),
        "gear_count": catch.get("gearCount", ""),
        "total_catch": catch.get("totalCatch", ""),
        "cpue": catch.get("CPUE", ""),
        "quartile_count_low": catch.get("quartileCount", [None, None])[0] if catch.get("quartileCount") else "",
        "quartile_count_high": catch.get("quartileCount", [None, None])[1] if catch.get("quartileCount") else "",
        "total_weight": catch.get("totalWeight", ""),
        "average_weight": catch.get("averageWeight", ""),
        "quartile_weight_low": catch.get("quartileWeight", [None, None])[0] if catch.get("quartileWeight") else "",
        "quartile_weight_high": catch.get("quartileWeight", [None, None])[1] if catch.get("quartileWeight") else "",
    }


def flatten_length_row(dow: str, lake_name: str, survey: dict, species: str, lengths: dict) -> dict:
    """Flatten one species length-distribution row for CSV output."""
    row = {
        "dow_number": dow,
        "lake_name": lake_name,
        "survey_id": survey.get("surveyID", ""),
        "survey_date": survey.get("surveyDate", ""),
        "species": species,
    }
    row.update(lengths)
    return row


def scrape_county(county_id: str, county_name: str):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    catch_path = os.path.join(OUTPUT_DIR, f"{county_name.lower()}_county_catch_summaries.csv")
    lengths_path = os.path.join(OUTPUT_DIR, f"{county_name.lower()}_county_length_distributions.csv")
    lakes_path = os.path.join(OUTPUT_DIR, f"{county_name.lower()}_county_lakes.csv")

    print(f"Fetching lake list for {county_name} County (ID={county_id})...")
    lakes = get_lakes_in_county(county_id)
    print(f"  Found {len(lakes)} lakes.")

    # Write lake index
    with open(lakes_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "county", "type"])
        writer.writeheader()
        for lake in lakes:
            writer.writerow({k: lake.get(k, "") for k in ["id", "name", "county", "type"]})

    catch_rows = []
    length_rows = []
    length_fieldnames = set()

    for i, lake in enumerate(lakes):
        dow = str(lake["id"])
        name = lake.get("name", "Unknown")
        print(f"  [{i+1}/{len(lakes)}] {name} (DOW {dow})")

        try:
            result = get_lake_surveys(dow)
        except Exception as e:
            print(f"    WARNING: Failed to fetch surveys for {name}: {e}")
            time.sleep(1)
            continue

        surveys = result.get("surveys", [])
        if not surveys:
            time.sleep(0.3)
            continue

        for survey in surveys:
            for catch in survey.get("fishCatchSummaries", []):
                catch_rows.append(flatten_catch_summary(dow, name, survey, catch))

            for species, lengths in survey.get("lengths", {}).items():
                if isinstance(lengths, dict) and lengths:
                    row = flatten_length_row(dow, name, survey, species, lengths)
                    length_rows.append(row)
                    length_fieldnames.update(lengths.keys())

        time.sleep(0.3)  # be polite to the server

    # Write catch summaries
    if catch_rows:
        catch_fields = list(catch_rows[0].keys())
        with open(catch_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=catch_fields)
            writer.writeheader()
            writer.writerows(catch_rows)
        print(f"\nCatch summary rows written: {len(catch_rows)}")
        print(f"  -> {catch_path}")
    else:
        print("\nNo catch summary data found.")

    # Write length distributions
    if length_rows:
        # Build consistent fieldnames: metadata cols first, then sorted size bins
        meta_cols = ["dow_number", "lake_name", "survey_id", "survey_date", "species"]
        size_cols = sorted(length_fieldnames, key=lambda x: int(x.split("-")[0].replace("+", "")) if x.replace("-", "").replace("+", "").isdigit() else 999)
        all_fields = meta_cols + [c for c in size_cols if c not in meta_cols]
        with open(lengths_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=all_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(length_rows)
        print(f"Length distribution rows written: {len(length_rows)}")
        print(f"  -> {lengths_path}")
    else:
        print("No length distribution data found.")

    print(f"\nLake index written: {lakes_path}")
    print("Done.")


if __name__ == "__main__":
    scrape_county(COUNTY_ID, COUNTY_NAME)
