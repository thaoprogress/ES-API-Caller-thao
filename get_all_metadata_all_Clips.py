# --------- IMPORTS ---------
import os
import csv
import re
from pathlib import Path
import FlowAPI
from dotenv import load_dotenv

# --------- INIT ---------
env_path = Path(__file__).parent / "cred.env"
load_dotenv(env_path)

metadata_api = FlowAPI.Metadata.create_gateway_instance(
    os.environ["FLOW_USER"], os.environ["FLOW_PASSWORD"], os.environ["FLOW_HOST"]
)


# --------- CONFIG ---------
tmp_path   = Path(__file__).parent / "test.csv"
final_path = Path(__file__).parent / "result_test.csv"
#limit  = metadata_api.numClips()
limit = 100
offset = 0


# --------- FUNC ---------
def duration_to_h(start_tc: str, end_tc: str) -> str:
    if start_tc is not None and end_tc is not None:
        start_hmsm, _ = start_tc.rsplit(":", 1)
        end_hmsm, _   = end_tc.rsplit(":", 1)

        sh, sm, ss, sms = start_hmsm.split(":")
        eh, em, es, ems = end_hmsm.split(":")

        start_ms = ((int(sh) * 3600 + int(sm) * 60 + int(ss)) * 1000) + int(sms)
        end_ms   = ((int(eh) * 3600 + int(em) * 60 + int(es)) * 1000) + int(ems)

        diff    = end_ms - start_ms
        seconds = diff / 1000.0
        hours   = seconds / 3600.0
        return f"{hours:.4f}"
    else:
        return None


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
                items.update(flatten(v[0], new_key, sep=sep))
            else:
                items[new_key] = ",".join(map(str, v))
        else:
            items[new_key] = v
    return items


def pretty_header_name(col: str) -> str:
    prefix = "asset.custom."
    if col.startswith(prefix):
        db_key  = col[len(prefix):]
        display = dbkey_to_name.get(db_key)
        if display:
            return f"asset_custom_named.{display}"
    return col


def natural_sort_key(s):
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', s)]


# --------- MAIN ---------
all_clips   = metadata_api.clips(offset=offset, limit=limit)
total_clips = len(all_clips)

custom_field_definitions = metadata_api.getCustomMetadataFields()
dbkey_to_name = {f["db_key"]: f["name"] for f in custom_field_definitions}
fieldnames = [f"asset_custom_named.{name}" for name in dbkey_to_name.values()]

with open(tmp_path, "w", newline="", encoding="utf-8") as f:
    writer = None

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
        tc_end   = clip_metadata.get("metadata", {}).get("timecode_end")
        clip_metadata["duration_hours"] = duration_to_h(tc_start, tc_end)

        flat = flatten(clip_metadata)
        flat_renamed = {}
        for k, v in flat.items():
            new_key = pretty_header_name(k)
            if new_key not in flat_renamed:
                flat_renamed[new_key] = v

        new_keys = [k for k in flat_renamed if k not in fieldnames]
        fieldnames.extend(new_keys)

        if new_keys:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            f.seek(0, 2)

        writer.writerow(remove_newline(flat_renamed))

        f.flush()
        os.fsync(f.fileno())
        print(f"Clip {clip_counter} von {total_clips} geschrieben ({len(new_keys)} neue Felder)")


# --------- Finale CSV: Header sortieren, fehlende Values auffüllen ---------
print("Erzeuge finale CSV...")

with open(tmp_path, "r", newline="", encoding="utf-8") as f_in:
    reader = csv.DictReader(f_in, fieldnames=fieldnames)
    rows   = list(reader)

sorted_fieldnames = sorted(fieldnames, key=natural_sort_key)

with open(final_path, "w", newline="", encoding="utf-8") as f_out:
    writer = csv.DictWriter(f_out, fieldnames=sorted_fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({k: row.get(k, "") for k in sorted_fieldnames})

print(f"✓ Fertig: {final_path}")

tmp_path.unlink()
print(f"✓ Temporäre Datei gelöscht: {tmp_path}")