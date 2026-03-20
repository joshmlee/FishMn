"""
Converts raw CSVs into clean JSON files for the lake survey web app.

Outputs to web/data/:
  - lakes.json        — list of all lakes with id, name, county
  - counties.json     — sorted list of county names
  - surveys.json      — all catch summary rows, keyed by dow_number
  - species_names.json — mapping of species code -> common name
"""

import csv
import json
import os
from collections import defaultdict

INPUT_DIR = "lake_survey_data"
OUTPUT_DIR = "web/data"

# Common name lookup for species codes used in MN DNR surveys
SPECIES_NAMES = {
    "WAE": "Walleye",
    "NOP": "Northern Pike",
    "LMB": "Largemouth Bass",
    "SMB": "Smallmouth Bass",
    "MUE": "Muskellunge",
    "TLC": "Tullibee (Cisco)",
    "CIS": "Cisco (Tullibee)",
    "BLC": "Black Crappie",
    "WTS": "White Sucker",
    "YEP": "Yellow Perch",
    "BLG": "Bluegill",
    "PKL": "Pumpkinseed",
    "RKB": "Rock Bass",
    "GSF": "Green Sunfish",
    "CCF": "Channel Catfish",
    "BUB": "Bullhead (Brown)",
    "YEB": "Yellow Bullhead",
    "BLB": "Black Bullhead",
    "BRH": "Brown Bullhead",
    "CAP": "Common Carp",
    "CRP": "Common Carp",
    "GLD": "Goldfish",
    "BKT": "Brook Trout",
    "BKS": "Brook Stickleback",
    "RBT": "Rainbow Trout",
    "BNT": "Brown Trout",
    "LKT": "Lake Trout",
    "SPT": "Splake",
    "TGM": "Tiger Muskellunge",
    "TGT": "Tiger Trout",
    "WTE": "White Bass",
    "WEA": "White Bass",
    "FHM": "Fathead Minnow",
    "BHM": "Bluntnose Minnow",
    "EMF": "Emerald Shiner",
    "SPM": "Spotail Shiner",
    "HHM": "Hornyhead Chub",
    "BRB": "Burbot",
    "SHR": "Shorthead Redhorse",
    "GRM": "Greater Redhorse",
    "SLR": "Silver Redhorse",
    "RDH": "Redhorse (unspecified)",
    "QUS": "Quillback",
    "HHC": "Highfin Carpsucker",
    "RCS": "River Carpsucker",
    "CSH": "Common Shiner",
    "BMS": "Bigmouth Shiner",
    "SPS": "Sand Shiner",
    "PPK": "Pumpkinseed",
    "BST": "Bowfin (Dogfish)",
    "GAR": "Longnose Gar",
    "AME": "American Eel",
    "MWF": "Mooneye",
    "MOO": "Mooneye",
    "GZR": "Gizzard Shad",
    "ATS": "Alewife",
    "RBS": "Rainbow Smelt",
    "SMS": "Rainbow Smelt",
    "LAK": "Lake Whitefish",
    "WTF": "Lake Whitefish",
    "LWF": "Lake Whitefish",
    "RFF": "Round Whitefish",
    "PGH": "Pygmy Whitefish",
    "BND": "Banded Killifish",
    "STK": "Stickleback (unspecified)",
    "NIS": "Nine-spine Stickleback",
    "BWS": "Brook Silverside",
    "DWS": "Brook Silverside",
    "TRP": "Trout-perch",
    "TPC": "Trout-perch",
    "SCU": "Sculpin (unspecified)",
    "MSC": "Mottled Sculpin",
    "STR": "Striped Bass",
    "HBD": "Hybrid Sunfish",
    "WLP": "Warmouth",
    "ORL": "Orange-spotted Sunfish",
    "LGS": "Longear Sunfish",
    "DRS": "Dollar Sunfish",
    "BDS": "Banded Sunfish",
    "BDD": "Bigmouth Buffalo",
    "SBF": "Smallmouth Buffalo",
    "BOF": "Black Buffalo",
    "FCF": "Flathead Catfish",
    "STB": "Stonecat",
    "MDM": "Madtom (unspecified)",
    "NMD": "Northern Madtom",
    "FRD": "Freshwater Drum",
    "DRM": "Freshwater Drum",
    "PKS": "Pickerel (unspecified)",
    "GRP": "Grass Pickerel",
    "CHN": "Chain Pickerel",
    "PKD": "Pickerel",
    "SPK": "Spotted Bass",
    "WPR": "White Perch",
    "CHL": "Chinook Salmon",
    "COS": "Coho Salmon",
    "ACS": "Atlantic Salmon",
    "PKT": "Pink Salmon",
    "PNS": "Pink Salmon",
    "BCS": "Black Crappie x White Crappie hybrid",
    "WRC": "White Crappie",
    "CRC": "White Crappie",
    "EMS": "Emerald Shiner",
    "CNM": "Central Mudminnow",
    "MUD": "Mudminnow",
    "ELM": "Eastern Mudminnow",
    "IDS": "Iowa Darter",
    "JHD": "Johnny Darter",
    "LGD": "Logperch",
    "YLD": "Yellow Darter",
    "SLD": "Slenderhead Darter",
    "GLD2": "Gilt Darter",
    "BSD": "Banded Darter",
    "RND": "River Darter",
    "BIB": "Bigmouth Buffalo",
    "CPS": "Carpsucker (unspecified)",
    "CSR": "Cisco x Lake Whitefish",
    "DAR": "Darter (unspecified)",
    "ELT": "Eel (unspecified)",
    "GBF": "Goldfish x Common Carp",
    "HHT": "Hornyhead x Creek Chub",
    "LGP": "Logperch",
    "NIS2": "Nine-spine Stickleback",
    "NMO": "Northern Madtom",
    "PMP": "Pumpkinseed",
    "RMD": "River Darter",
    "SDR": "Sand Darter",
    "SHD": "Shad (unspecified)",
    "SHK": "Shark (unspecified)",
    "SPF": "Spottail Shiner",
    "STO": "Stonecat",
    "STS": "Striped Shiner",
    "SWF": "Suckermouth Minnow",
    "TGR": "Tiger Muskie",
    "WHS": "White Sucker",
    "YBS": "Yellow Bass",
    "BNM": "Bluntnose Minnow",
    "BNS": "Bluntnose Minnow",
    "CMS": "Creek Chub",
}

def parse_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- Lakes ---
    print("Processing lakes...")
    lakes = []
    counties = set()
    with open(f"{INPUT_DIR}/all_counties_lakes.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            lakes.append({
                "id": row["id"],
                "name": row["name"],
                "county": row["county"],
            })
            counties.add(row["county"])

    with open(f"{OUTPUT_DIR}/lakes.json", "w") as f:
        json.dump(lakes, f, separators=(",", ":"))
    print(f"  {len(lakes)} lakes written")

    # --- Counties ---
    sorted_counties = sorted(counties)
    with open(f"{OUTPUT_DIR}/counties.json", "w") as f:
        json.dump(sorted_counties, f, separators=(",", ":"))
    print(f"  {len(sorted_counties)} counties written")

    # --- Catch Summaries (keyed by dow_number) ---
    print("Processing catch summaries...")
    surveys_by_lake = defaultdict(list)
    with open(f"{INPUT_DIR}/all_counties_catch_summaries.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            surveys_by_lake[row["dow_number"]].append({
                "survey_id": row["survey_id"],
                "date": row["survey_date"],
                "type": row["survey_type"],
                "species": row["species"],
                "gear": row["gear"],
                "gear_count": int(row["gear_count"]) if row["gear_count"].isdigit() else None,
                "total_catch": int(row["total_catch"]) if row["total_catch"].isdigit() else None,
                "cpue": parse_float(row["cpue"]),
                "avg_weight": parse_float(row["average_weight"]),
                "total_weight": parse_float(row["total_weight"]),
            })

    # Write one surveys_<county>.json per county (avoids loading 54MB at once)
    county_surveys = defaultdict(dict)
    # Build a dow->county map from lakes
    dow_to_county = {}
    with open(f"{INPUT_DIR}/all_counties_lakes.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            dow_to_county[row["id"]] = row["county"]

    for dow, rows in surveys_by_lake.items():
        county = dow_to_county.get(dow, "Unknown")
        county_surveys[county][dow] = rows

    surveys_dir = f"{OUTPUT_DIR}/surveys"
    os.makedirs(surveys_dir, exist_ok=True)
    for county, data in county_surveys.items():
        safe_name = county.replace(" ", "_").replace("/", "_")
        with open(f"{surveys_dir}/{safe_name}.json", "w") as f:
            json.dump(data, f, separators=(",", ":"))
    print(f"  {len(surveys_by_lake)} lakes with survey data written across {len(county_surveys)} county files")

    # --- Species names ---
    # Also collect any codes from the data not already in our lookup
    all_codes = set()
    for rows in surveys_by_lake.values():
        for r in rows:
            all_codes.add(r["species"])
    for code in sorted(all_codes):
        if code not in SPECIES_NAMES:
            SPECIES_NAMES[code] = code  # fallback: show the code itself

    with open(f"{OUTPUT_DIR}/species_names.json", "w") as f:
        json.dump(SPECIES_NAMES, f, indent=2, sort_keys=True)
    print(f"  {len(SPECIES_NAMES)} species name mappings written")

    print(f"\nDone. Files written to {OUTPUT_DIR}/")

if __name__ == "__main__":
    main()
