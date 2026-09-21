"""Extract ZIP archives from ``data/raw`` into ``data/extracted``.

Used as a local/manual data-preparation utility before files are uploaded to
the MinIO bronze bucket.
"""

import zipfile
from pathlib import Path

bronze_dir = Path("data/raw")
extract_dir = Path("data/extracted")

for zip_file in bronze_dir.rglob("*.zip"):

    # Find where the zip is relative to data/raw
    relative_path = zip_file.relative_to(bronze_dir)

    # Keep the same parent folder structure
    output_dir = extract_dir / relative_path.parent / zip_file.stem

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Extracting: {zip_file} -> {output_dir}")

    with zipfile.ZipFile(zip_file, "r") as zip_ref:
        zip_ref.extractall(output_dir)
