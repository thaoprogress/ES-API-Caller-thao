# --------- IMPORTS ---------
import os
import csv
import shutil
import FlowAPI
from pathlib import Path
from dotenv import load_dotenv


# --------- INIT ---------
env_path = Path(__file__).parent / "cred.env"
load_dotenv(env_path)

metadata_api = FlowAPI.Metadata.create_gateway_instance(
    os.environ["FLOW_USER"], os.environ["FLOW_PASSWORD"], os.environ["FLOW_HOST"]
)


# --------- CONFIG ---------
proxy_server_base = Path(r"\\10.0.77.14\RAIDS\RAID_1\flow\files")
clip_list_path = Path(__file__).parent / "AniVision_Filmliste.CSV"
csv_path = Path(__file__).parent / "Results" / "result_all_metadata_all_clips.csv"
download_path = Path(r"\\10.0.77.11\Ablage KI Proxy_1\wip_Lieferung AniVision 1.5Mbit")


# --------- MAIN ---------
TITLE_COLUMNS = [
    "asset_custom_named.04 Title",
    "asset_custom_named.014 Title Original",
    "asset_custom_named.015 Title German",
]

with open(clip_list_path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    clip_titles = {row["Titel"].strip() for row in reader if row["Titel"]}

with open(csv_path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader, start=2):
        matched_title = None
        for col in TITLE_COLUMNS:
            if row.get(col, "").strip() in clip_titles:
                matched_title = row[col].strip()
                break

        if not matched_title:
            continue

        proxy_path = row["proxy_path"]
        if not proxy_path:
            print(f"⚠️  Kein Proxy für: {matched_title} (Zeile {i})")
            continue

        source = proxy_server_base / proxy_path
        dest_name = matched_title + source.suffix
        destination = download_path / dest_name

        #shutil.copy2(source, destination)
        print(f"✅ Kopiert: {matched_title} → {dest_name}")
