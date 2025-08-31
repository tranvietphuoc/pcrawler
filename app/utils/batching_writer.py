import os, csv
from typing import List, Dict, Any


def safe_append_rows_csv(path: str, rows: List[Dict[str, Any]], fieldnames: List[str]):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    file_exists = os.path.exists(path)
    tmp_path = path + ".tmp"

    with open(tmp_path, "w", newline="", encoding="utf-8-sig") as ftmp:
        writer = csv.DictWriter(ftmp, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        for r in rows:
            writer.writerow(
                {k: ("" if r.get(k) is None else r.get(k)) for k in fieldnames}
            )
        ftmp.flush()
        os.fsync(ftmp.fileno())

    mode = "a" if file_exists else "w"

    with (
        open(path, mode, newline="", encoding="utf-8-sig") as fout,
        open(tmp_path, "r", encoding="utf-8-sig") as fin,
    ):
        if file_exists:
            fin.readline()
        for line in fin:
            fout.write(line)
        fout.flush()
        os.fsync(fout.fileno())
    try:
        os.remove(tmp_path)
    except Exception:
        pass
