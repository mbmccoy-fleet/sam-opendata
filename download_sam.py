"""
Download SAM.gov Entity Registration public extracts.

Fetches the file manifest from SAM.gov's internal API, then provides
tools for downloading and verifying completeness.

The SAM.gov download API requires an authenticated session to generate
valid S3 presigned URLs, so automated download won't work without one.
Instead, use --links to get browser-clickable URLs (you must be logged in
to SAM.gov), or place manually-downloaded files in the data directory and
use --list to verify completeness.

Usage:
    uv run python download_sam.py --list            # list files & status
    uv run python download_sam.py --links           # print download URLs for browser
    uv run python download_sam.py --folder "Public V2"  # only one folder
    uv run python download_sam.py --ingest ~/Downloads   # move matching files from a dir
"""

import argparse
import json
import shutil
import sys
from pathlib import Path
from urllib.parse import quote

import httpx

BASE = "https://sam.gov/api/prod/fileextractservices/v1/api"
DATA_DIR = Path(__file__).parent / "data" / "Data Services" / "Entity Registration"

FOLDERS = [
    "Entity Registration/Public - Historical",
    "Entity Registration/Public V2",
]


def list_files(folder: str) -> list[dict]:
    """Fetch the file listing for a folder from the SAM.gov API."""
    url = f"{BASE}/listfiles"
    params = {"domain": folder}
    with httpx.Client(timeout=30) as client:
        r = client.get(url, params=params)
        r.raise_for_status()
    data = r.json()
    return data["_embedded"]["customS3ObjectSummaryList"]


def dest_path(entry: dict) -> Path:
    """Compute the local destination path for a file entry."""
    key = entry["key"]  # e.g. "Entity Registration/Public - Historical/file.zip"
    rel = key.removeprefix("Entity Registration/")
    return DATA_DIR / rel


def download_url(entry: dict) -> str:
    """Build the SAM.gov download URL for a file (works in a logged-in browser)."""
    key = entry["key"]
    return f"https://sam.gov/api/prod/fileextractservices/v1/api/download?fileName={quote(key)}"


def get_manifest(folders: list[str]) -> dict:
    """Build the manifest dict from the API."""
    manifest = {}
    for folder in folders:
        short_name = folder.split("/", 1)[1]
        entries = list_files(folder)
        manifest[short_name] = []
        for entry in entries:
            out = dest_path(entry)
            manifest[short_name].append({
                "name": entry["displayKey"],
                "key": entry["key"],
                "date": entry["dateModified"],
                "local": str(out),
                "downloaded": out.exists(),
                "size": out.stat().st_size if out.exists() else None,
            })
    return manifest


def cmd_list(manifest: dict):
    """Print file listing with download status."""
    for folder_name, files in manifest.items():
        print(f"\n=== {folder_name} ===")
        for f in files:
            exists = f["downloaded"]
            size_str = f"{f['size'] / (1024*1024):.1f} MB" if exists else "missing"
            marker = "OK" if exists else "  "
            print(f"  [{marker}] {f['name']:<55} {f['date']:>12}  {size_str}")

        done = sum(1 for f in files if f["downloaded"])
        print(f"\n  {done}/{len(files)} downloaded")


def cmd_links(manifest: dict):
    """Print download URLs for missing files."""
    any_missing = False
    for folder_name, files in manifest.items():
        missing = [f for f in files if not f["downloaded"]]
        if not missing:
            continue
        any_missing = True
        print(f"\n=== {folder_name} — {len(missing)} missing ===")
        print(f"(Open these in a browser while logged in to SAM.gov)\n")
        for f in missing:
            key = f["key"]
            url = f"https://sam.gov/api/prod/fileextractservices/v1/api/download?fileName={quote(key)}"
            print(url)

    if not any_missing:
        print("\nAll files already downloaded!")


def cmd_ingest(manifest: dict, source_dir: Path):
    """Find and move matching files from source_dir into the data directory."""
    moved = 0
    for folder_name, files in manifest.items():
        for f in files:
            if f["downloaded"]:
                continue
            # Case-insensitive search in source directory
            name = f["name"]
            candidates = list(source_dir.glob(f"*"))
            match = None
            for c in candidates:
                if c.name.lower() == name.lower() and c.is_file():
                    match = c
                    break
            if match:
                dest = Path(f["local"])
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(match), str(dest))
                print(f"  Moved {match.name} -> {dest.relative_to(DATA_DIR)}")
                moved += 1

    print(f"\nMoved {moved} file(s)")


def main():
    parser = argparse.ArgumentParser(description="SAM.gov Entity Registration extract manager")
    parser.add_argument("--list", action="store_true", help="List files and download status")
    parser.add_argument("--links", action="store_true", help="Print download URLs for missing files")
    parser.add_argument("--ingest", metavar="DIR", help="Move matching files from DIR into data/")
    parser.add_argument("--folder", help="Only process this subfolder (e.g. 'Public V2')")
    args = parser.parse_args()

    folders = FOLDERS
    if args.folder:
        folders = [f for f in FOLDERS if args.folder in f]
        if not folders:
            print(f"No matching folder for '{args.folder}'")
            sys.exit(1)

    manifest = get_manifest(folders)

    # Save manifest
    manifest_path = Path(__file__).parent / "file_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    if args.links:
        cmd_links(manifest)
    elif args.ingest:
        cmd_ingest(manifest, Path(args.ingest))
        # Refresh and show status
        manifest = get_manifest(folders)
        manifest_path.write_text(json.dumps(manifest, indent=2))
        cmd_list(manifest)
    else:
        # Default: --list
        cmd_list(manifest)


if __name__ == "__main__":
    main()
