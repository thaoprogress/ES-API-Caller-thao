import csv
from pathlib import Path

RESULTS_DIR = Path("Results")
INPUT       = RESULTS_DIR / "result_all_metadata_all_clips.csv"
BASENAME    = RESULTS_DIR / "result_all_metadata_all_clips_part_"


ROWS_PER_FILE = 800_000

def split_csv(input_path, rows_per_file):
    input_path = Path(input_path)
    with input_path.open("r", newline="", encoding="utf-8") as f_in:
        reader = csv.reader(f_in)
        header = next(reader)  # Header einmal lesen

        file_index = 1
        row_count_in_current = 0
        out_file = None
        writer = None

        for row in reader:
            # Neues Output-File öffnen, wenn noch keines offen ist
            if out_file is None:
                out_name = RESULTS_DIR / f"{INPUT.stem}_teil_{file_index}.csv"
                out_file = open(out_name, "w", newline="", encoding="utf-8")
                writer = csv.writer(out_file)
                writer.writerow(header)  # Header in jede Teil-CSV schreiben
                row_count_in_current = 0

            writer.writerow(row)
            row_count_in_current += 1

            # Wenn 800k Zeilen erreicht, Datei schließen und zur nächsten wechseln
            if row_count_in_current >= rows_per_file:
                out_file.close()
                out_file = None
                writer = None
                file_index += 1

        # Letzte Datei schließen, falls noch offen
        if out_file is not None:
            out_file.close()

if __name__ == "__main__":
    split_csv(INPUT, ROWS_PER_FILE)
