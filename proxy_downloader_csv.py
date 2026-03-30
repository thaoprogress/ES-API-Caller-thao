# --------- IMPORTS ---------
import os
import csv
import shutil
import FlowAPI
import html
import paramiko
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

matched_titles = []
matched_sources = []
matched_uuids = []
matched_search_titles = []
no_proxy_titles = []

with open(clip_list_path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter=";")
    clip_titles = {html.unescape(row["Title"].strip()) for row in reader if row["Title"]}

with open(csv_path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader, start=2):
        matched_title = None
        proxy_path = row["proxy_path"]

        for col in TITLE_COLUMNS:
            if html.unescape(row.get(col, "").strip()) in clip_titles:
                matched_title = html.unescape(row[col].strip())
                break

        if not matched_title:
            continue

        if not proxy_path:
            no_proxy_titles.append(matched_title)
            continue

        source = proxy_server_base / proxy_path
        dest_name = matched_title + source.suffix
        destination = download_path / dest_name

        matched_titles.append(matched_title)
        matched_sources.append(str(source))
        matched_uuids.append(row["asset.uuid"])
        matched_search_titles.append(html.unescape(row.get(col, "").strip()))



        if source.exists():
            #shutil.copy2(source, destination)
            print(f"✅ Kopiert: {matched_title} → {str(source)}")


# --------- CHECK ---------
matches_path = Path(__file__).parent / "matches.csv"
with open(matches_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Title"])
    writer.writerows([[t] for t in matched_titles])

overview_path = Path(__file__).parent / "overview.csv"
with open(overview_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["search_title", "found_title", "proxy_path", "asset.uuid"])
    for i in range(len(matched_titles)):
        writer.writerow([matched_search_titles[i], matched_titles[i], matched_sources[i], matched_uuids[i]])

print(f"\n📊 Ergebnis:")
print(f"   Gesuchte Titel:           {len(clip_titles)}")
print(f"   Gefundene Titel:          {len(matched_titles)}")
print(f"   Kein Proxy:               {len(no_proxy_titles)}")
print(f"   Gleiche Quellpfade:       {len(matched_sources) - len(set(matched_sources))}")

if no_proxy_titles:
    print(f"\n   ⚠️  Kein Proxy vorhanden ({len(no_proxy_titles)}):")
    for title in sorted(no_proxy_titles):
        print(f"      - {title}")

duplicate_sources = [s for s in matched_sources if matched_sources.count(s) > 1]

if duplicate_sources:
    duplicates_csv_path = Path(__file__).parent / "duplicates.csv"
    with open(overview_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        overview_rows = [row for row in reader if row["proxy_path"] in set(duplicate_sources)]

    with open(duplicates_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["search_title", "found_title", "proxy_path", "asset.uuid"])
        writer.writeheader()
        writer.writerows(sorted(overview_rows, key=lambda r: r["proxy_path"]))

    print(f"\n   ⚠️  Duplikate als CSV gespeichert: {duplicates_csv_path}")
