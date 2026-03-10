"""
MN DNR LakeFinder Survey Scraper
Pulls fish survey data for all lakes in all 87 Minnesota counties.
Supports resuming: skips counties whose output files already exist.
"""

import json
import re
import time
import csv
import os
import requests

GAZETTEER_URL = "https://maps.dnr.state.mn.us/cgi-bin/gazetteer/gazetteer2.cgi"
DETAIL_URL = "https://maps.dnr.state.mn.us/cgi-bin/lakefinder/detail.cgi"

OUTPUT_DIR = "lake_survey_data"

# All 87 MN counties, DNR ID = alphabetical position (1-87)
MN_COUNTIES = [
    (1,  "Aitkin"),
    (2,  "Anoka"),
    (3,  "Becker"),
    (4,  "Beltrami"),
    (5,  "Benton"),
    (6,  "Big Stone"),
    (7,  "Blue Earth"),
    (8,  "Brown"),
    (9,  "Carlton"),
    (10, "Carver"),
    (11, "Cass"),
    (12, "Chippewa"),
    (13, "Chisago"),
    (14, "Clay"),
    (15, "Clearwater"),
    (16, "Cook"),
    (17, "Cottonwood"),
    (18, "Crow Wing"),
    (19, "Dakota"),
    (20, "Dodge"),
    (21, "Douglas"),
    (22, "Faribault"),
    (23, "Fillmore"),
    (24, "Freeborn"),
    (25, "Goodhue"),
    (26, "Grant"),
    (27, "Hennepin"),
    (28, "Houston"),
    (29, "Hubbard"),
    (30, "Isanti"),
    (31, "Itasca"),
    (32, "Jackson"),
    (33, "Kanabec"),
    (34, "Kandiyohi"),
    (35, "Kittson"),
    (36, "Koochiching"),
    (37, "Lac qui Parle"),
    (38, "Lake"),
    (39, "Lake of the Woods"),
    (40, "Le Sueur"),
    (41, "Lincoln"),
    (42, "Lyon"),
    (43, "McLeod"),
    (44, "Mahnomen"),
    (45, "Marshall"),
    (46, "Martin"),
    (47, "Meeker"),
    (48, "Mille Lacs"),
    (49, "Morrison"),
    (50, "Mower"),
    (51, "Murray"),
    (52, "Nicollet"),
    (53, "Nobles"),
    (54, "Norman"),
    (55, "Olmsted"),
    (56, "Otter Tail"),
    (57, "Pennington"),
    (58, "Pine"),
    (59, "Pipestone"),
    (60, "Polk"),
    (61, "Pope"),
    (62, "Ramsey"),
    (63, "Red Lake"),
    (64, "Redwood"),
    (65, "Renville"),
    (66, "Rice"),
    (67, "Rock"),
    (68, "Roseau"),
    (69, "St. Louis"),
    (70, "Scott"),
    (71, "Sherburne"),
    (72, "Sibley"),
    (73, "Stearns"),
    (74, "Steele"),
    (75, "Stevens"),
    (76, "Swift"),
    (77, "Todd"),
    (78, "Traverse"),
    (79, "Wabasha"),
    (80, "Wadena"),
    (81, "Waseca"),
    (82, "Washington"),
    (83, "Watonwan"),
    (84, "Wilkin"),
    (85, "Winona"),
    (86, "Wright"),
    (87, "Yellow Medicine"),
]


def fetch_jsonp(url: str, params: dict) -> dict:
    """Fetch a JSONP endpoint and return parsed JSON."""
    params["callback"] = "cb"
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    text = response.text.strip()
    match = re.match(r"^\w+\((.*)\);?$", text, re.DOTALL)
    if match:
        return json.loads(match.group(1))
    return json.loads(text)


def get_lakes_in_county(county_id: str) -> list[dict]:
    data = fetch_jsonp(GAZETTEER_URL, {"type": "lake", "county": county_id})
    if data.get("status") == "ERROR":
        raise RuntimeError(f"Gazetteer error: {data.get('message')}")
    return data.get("results", [])


def get_lake_surveys(dow_number: str) -> dict:
    data = fetch_jsonp(DETAIL_URL, {"type": "lake_survey", "id": dow_number})
    if data.get("status") == "ERROR":
        return {}
    return data.get("result", {})


def flatten_catch_summary(dow: str, lake_name: str, county_name: str, survey: dict, catch: dict) -> dict:
    return {
        "dow_number": dow,
        "lake_name": lake_name,
        "county": county_name,
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


def flatten_length_row(dow: str, lake_name: str, county_name: str, survey: dict, species: str, lengths: dict) -> dict:
    row = {
        "dow_number": dow,
        "lake_name": lake_name,
        "county": county_name,
        "survey_id": survey.get("surveyID", ""),
        "survey_date": survey.get("surveyDate", ""),
        "species": species,
        "minimum_length": lengths.get("minimumLength", ""),
        "fishCount": json.dumps(lengths.get("fishCount", [])),
        "maximum_length": lengths.get("maximumLength", ""),
    }
    return row


def scrape_county(county_id: int, county_name: str, catch_writer, lengths_writer, lakes_writer):
    safe_name = county_name.lower().replace(" ", "_").replace(".", "")
    done_marker = os.path.join(OUTPUT_DIR, f".done_{safe_name}")

    if os.path.exists(done_marker):
        print(f"Skipping {county_name} County (already done).")
        return

    print(f"\n[{county_id}/87] Fetching {county_name} County...")
    try:
        lakes = get_lakes_in_county(str(county_id))
    except Exception as e:
        print(f"  ERROR fetching lake list: {e}")
        return

    print(f"  Found {len(lakes)} lakes.")

    for lake in lakes:
        lakes_writer.writerow({
            "id": lake.get("id", ""),
            "name": lake.get("name", ""),
            "county": county_name,
            "type": lake.get("type", ""),
        })

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
                catch_writer.writerow(flatten_catch_summary(dow, name, county_name, survey, catch))

            for species, lengths in survey.get("lengths", {}).items():
                if isinstance(lengths, dict) and lengths:
                    lengths_writer.writerow(flatten_length_row(dow, name, county_name, survey, species, lengths))

        time.sleep(0.3)

    # Mark county as done so we can resume if interrupted
    open(done_marker, "w").close()
    print(f"  Done with {county_name} County.")


if __name__ == "__main__":
    import sys
    # Optional args: start_id end_id (inclusive, 1-87)
    # e.g. python mn_dnr_lake_scraper.py 11 15
    start_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    end_id   = int(sys.argv[2]) if len(sys.argv) > 2 else 87

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    catch_path = os.path.join(OUTPUT_DIR, "all_counties_catch_summaries.csv")
    lengths_path = os.path.join(OUTPUT_DIR, "all_counties_length_distributions.csv")
    lakes_path = os.path.join(OUTPUT_DIR, "all_counties_lakes.csv")

    catch_fields = [
        "dow_number", "lake_name", "county", "survey_id", "survey_date",
        "survey_type", "survey_sub_type", "species", "gear", "gear_count",
        "total_catch", "cpue", "quartile_count_low", "quartile_count_high",
        "total_weight", "average_weight", "quartile_weight_low", "quartile_weight_high",
    ]
    length_fields = ["dow_number", "lake_name", "county", "survey_id", "survey_date", "species", "minimum_length", "fishCount", "maximum_length"]
    lake_fields = ["id", "name", "county", "type"]

    # Append mode so re-runs don't wipe existing data
    catch_exists = os.path.exists(catch_path)
    lengths_exists = os.path.exists(lengths_path)
    lakes_exists = os.path.exists(lakes_path)

    with (
        open(catch_path, "a", newline="") as catch_f,
        open(lengths_path, "a", newline="") as lengths_f,
        open(lakes_path, "a", newline="") as lakes_f,
    ):
        catch_writer = csv.DictWriter(catch_f, fieldnames=catch_fields)
        lengths_writer = csv.DictWriter(lengths_f, fieldnames=length_fields)
        lakes_writer = csv.DictWriter(lakes_f, fieldnames=lake_fields)

        if not catch_exists:
            catch_writer.writeheader()
        if not lengths_exists:
            lengths_writer.writeheader()
        if not lakes_exists:
            lakes_writer.writeheader()

        for county_id, county_name in MN_COUNTIES:
            if not (start_id <= county_id <= end_id):
                continue
            scrape_county(county_id, county_name, catch_writer, lengths_writer, lakes_writer)

    print("\nAll counties complete.")
    print(f"  {catch_path}")
    print(f"  {lengths_path}")
    print(f"  {lakes_path}")
