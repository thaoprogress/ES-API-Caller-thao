# --------- IMPORTS ---------
import os
import csv
from pathlib import Path
import FlowAPI
from dotenv import load_dotenv
import re

# --------- INIT ---------
env_path = Path(__file__).parent / "cred.env"
load_dotenv(env_path)

metadata_api = FlowAPI.Metadata.create_gateway_instance(
    os.environ["FLOW_USER"], os.environ["FLOW_PASSWORD"], os.environ["FLOW_HOST"]
)

# --------- CONFIG ---------
csv_path = Path(__file__).parent / "result_all_metadata_all_clipso.csv"
#limit = metadata_api.numClips()
limit = 500
offset = 0

# --------- FUNC ---------
def duration_tc_ms(start_tc: str, end_tc: str) -> str:
    if start_tc is not None and end_tc is not None:
        start_hmsm, _ = start_tc.rsplit(":", 1)
        end_hmsm, _   = end_tc.rsplit(":", 1)

        sh, sm, ss, sms = start_hmsm.split(":")
        eh, em, es, ems = end_hmsm.split(":")

        start_ms = ((int(sh) * 3600 + int(sm) * 60 + int(ss)) * 1000) + int(sms)
        end_ms   = ((int(eh) * 3600 + int(em) * 60 + int(es)) * 1000) + int(ems)

        diff = end_ms - start_ms

        seconds = diff / 1000.0
        hours = seconds / 3600.0
        
        return f"{hours:.4f}"
    else:
        return None


def get_medaspace_name(clip_metadata: dict) -> str:
    media_space_name = None
    media_type = "video" if clip_metadata.get("has_video") else "audio"
    media_space_name = clip_metadata.get(media_type, [{}])[0].get("file", {}).get("archive_locations", [{}])[0].get("media_space_name")
    return media_space_name


def get_fps(clip_metadata: str) -> str:
    raw_fps = clip_metadata.get("video", [{}])[0].get("frame_rate", None)
    fps = raw_fps.split("/")[0] if raw_fps else None
    return str(fps)


def remove_newline(row: dict) -> dict:
    cleaned = {}
    for k, v in row.items():
        if isinstance(v, str):
            cleaned[k] = v.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
        else:
            cleaned[k] = v
    return cleaned


def flatten(d, parent_key="", sep="."):
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k

        if isinstance(v, dict):
            items.update(flatten(v, new_key, sep=sep))

        elif isinstance(v, list):
            if v and isinstance(v[0], dict):
                # Nur das erste Element nehmen
                items.update(flatten(v[0], new_key, sep=sep))
            else:
                items[new_key] = ",".join(map(str, v))

        else:
            items[new_key] = v
    return items
    

def pretty_header_name(col: str) -> str:
    prefix = "asset.custom."
    if col.startswith(prefix):
        db_key = col[len(prefix):]  # z.B. "field_101"
        display = dbkey_to_name.get(db_key)
        if display:
            return f"asset_custom_named.{display}"
    return col


def natural_sort_key(s):
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', s)]


# --------- MAIN ---------
all_clips = metadata_api.clips(offset=offset, limit=limit)
total_clips = len(all_clips)

# Custom-Feld-Definitionen holen und Mapping db_key -> Name bauen
custom_field_definitions = metadata_api.getCustomMetadataFields()
dbkey_to_name = {f["db_key"]: f["name"] for f in custom_field_definitions}


# --------- PASS 1: alle Keys sammeln ---------
all_keys = set()

for clip_counter, clip in enumerate(all_clips, start=1):
    clip_metadata = metadata_api.get_clip(clip)
    clip_metadata.get("asset", {}).pop("customtypes", None)

    asset_custom = clip_metadata.get("asset", {}).get("custom", {})
    # optional: zusätzlich lesbare Custom-Namen in die Metadaten hängen
    custom_values_named = {
        dbkey_to_name.get(db_key, db_key): value
        for db_key, value in asset_custom.items()
    }
    clip_metadata["asset_custom_named"] = custom_values_named

    tc_start = clip_metadata.get("metadata", {}).get("timecode_start")
    tc_end = clip_metadata.get("metadata", {}).get("timecode_end")
    clip_metadata["duration_hours"] = duration_tc_ms(tc_start, tc_end)

    flat = flatten(clip_metadata)
    all_keys.update(flat.keys())

    print(f"Pass 1: Clip {clip_counter} von {total_clips} verarbeitet")


# --------- PASS 2: CSV schreiben, pro Clip anhängen ---------
file_exists = csv_path.exists()
mode = "a" if file_exists else "w"

fieldnames_raw = sorted(all_keys, key=natural_sort_key)
fieldnames = [pretty_header_name(c) for c in fieldnames_raw]

with open(csv_path, mode, newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    if not file_exists:
        writer.writeheader()

    for clip_counter, clip in enumerate(all_clips, start=1):
        clip_metadata = metadata_api.get_clip(clip)
        clip_metadata.get("asset", {}).pop("customtypes", None)

        asset_custom = clip_metadata.get("asset", {}).get("custom", {})
        custom_values_named = {
            dbkey_to_name.get(db_key, db_key): value
            for db_key, value in asset_custom.items()
        }
        clip_metadata["asset_custom_named"] = custom_values_named

        tc_start = clip_metadata.get("metadata", {}).get("timecode_start")
        tc_end = clip_metadata.get("metadata", {}).get("timecode_end")
        clip_metadata["duration_hours"] = duration_tc_ms(tc_start, tc_end)

        flat = flatten(clip_metadata)

        # Keys umbenennen, genau wie die fieldnames
        flat_renamed = {pretty_header_name(k): v for k, v in flat.items()}

        writer.writerow(remove_newline(flat_renamed))

        f.flush()
        os.fsync(f.fileno())
        print(f"Pass 2: Clip {clip_counter} von {total_clips} in CSV geschrieben")