# --------- IMPORTS ---------
import os
import csv
import html
import paramiko
from pathlib import Path, PurePosixPath
from dotenv import load_dotenv
import subprocess
import re


# --------- INIT ---------
env_path = Path(__file__).parent / "cred.env"
load_dotenv(env_path)

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(
    os.environ["SSH_HOST"],
    username=os.environ["SSH_USER"],
    password=os.environ["SSH_PASSWORD"]
)

# --------- CONFIG ---------
clip_list_path = Path(__file__).parent / "AniVision_Filmliste.csv"
csv_path = Path(__file__).parent / "Results" / "result_all_metadata_all_clips.csv"
download_path = Path(r"\\10.0.77.11\Ablage KI Proxy_1\wip_Lieferung AniVision 1.5Mbit")

proxy_server_base = PurePosixPath("/RAIDS/RAID_1/flow/files")
PLACEHOLDER_UUID = "5a2122e2-17b0-459c-9253-2ee3c60e105e"
RESULT_CSV_FIELDS = ["title_original", "title_german", "asset.uuid", "126_uuid"]


# --------- FUNC ---------
def make_result_row(row):
    return {
        "title_original": row.get("asset_custom_named.014 Title Original", ""),
        "title_german":   row.get("asset_custom_named.015 Title German", ""),
        "asset.uuid":     row.get("asset.uuid", ""),
        "126_uuid":       row.get("asset_custom_named.126 UUID", ""),
    }

def sanitize_filename(name):
    return re.sub(r'[\\/:*?"<>|]', "_", name)


# --------- NETWORK MOUNT ---------
net_use = subprocess.run([
    "net", "use",
    str(download_path.drive),
    f"/user:{os.environ['FLOW_USER']}",
    os.environ["FLOW_PASSWORD"]
], capture_output=True, encoding="cp1252", errors="replace")

if net_use.returncode != 0:
    print(f"❌ Netzlaufwerk konnte nicht verbunden werden:")
    print(f"   {net_use.stderr}")
    exit(1)
else:
    print(f"✅ Netzlaufwerk verbunden: {download_path.drive}")


# --------- TEST ---------
test_file = download_path / "_connection_test.tmp"
try:
    download_path.mkdir(parents=True, exist_ok=True)
    test_file.write_text("test")
    test_file.unlink()
    print(f"✅ Zielpfad erreichbar: {download_path}")
except Exception as e:
    print(f"❌ Zielpfad nicht erreichbar: {download_path}")
    print(f"   Fehler: {e}")
    exit(1)


# --------- MAIN ---------
TITLE_COLUMNS = [
    "asset_custom_named.04 Title",
    "asset_custom_named.014 Title Original",
    "asset_custom_named.015 Title German",
]

matched_titles = set()
no_proxy_titles = []
no_proxy_rows = []
placeholder_rows = []

sftp = ssh.open_sftp()

with open(clip_list_path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter=";")
    clip_titles = {html.unescape(row["Title"].strip()) for row in reader if row["Title"]}

with open(csv_path, newline="", encoding="utf-8") as f_in:
    reader = csv.DictReader(f_in)

    for i, row in enumerate(reader, start=2):
        matched_title = None
        proxy_path = row["proxy_path"]

        for col in TITLE_COLUMNS:
            if html.unescape(row.get(col, "").strip()) in clip_titles:
                matched_title = html.unescape(row[col].strip())
                break

        if not matched_title:
            continue

        matched_titles.add(matched_title)

        if not proxy_path:
            no_proxy_titles.append(matched_title)
            no_proxy_rows.append(make_result_row(row))
            continue

        if PLACEHOLDER_UUID in proxy_path:
            placeholder_rows.append(make_result_row(row))
            continue

        source = proxy_server_base / proxy_path
        dest_name = sanitize_filename(matched_title) + source.suffix
        destination = download_path / dest_name

        try:
            sftp.get(source.as_posix(), str(destination))
            print(f"✅ Kopiert: {dest_name}")
        except Exception as e:
            print(f"❌ Fehler: {type(e).__name__}: {e}")

sftp.close()
ssh.close()


# --------- NETWORK DEMOUNT ---------
subprocess.run(
    ["net", "use", str(download_path.drive), "/delete"],
    capture_output=True, encoding="cp1252"
)
print(f"✅ Netzlaufwerk getrennt: {download_path.drive}")


# --------- CHECK ---------
never_found = clip_titles - matched_titles

if never_found:
    with open(download_path / "not_found.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["searched_title"])
        writer.writerows([[t] for t in sorted(never_found)])

if no_proxy_rows:
    with open(download_path / "no_proxy.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=RESULT_CSV_FIELDS)
        writer.writeheader()
        writer.writerows(no_proxy_rows)

if placeholder_rows:
    with open(download_path / "placeholder.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=RESULT_CSV_FIELDS)
        writer.writeheader()
        writer.writerows(placeholder_rows)

print(f"\n📊 Ergebnis:")
print(f"  Gesuchte Titel:    {len(clip_titles)}")
print(f"  Gefundene Titel:   {len(matched_titles)}")
print(f"  Nie gefunden:      {len(never_found)}")
print(f"  Kein Proxy:        {len(no_proxy_titles)}")
print(f"  Platzhalter:       {len(placeholder_rows)}")