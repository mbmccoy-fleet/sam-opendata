"""Parse saved SAM.gov Data Services HTML pages to extract file lists."""

import json
from pathlib import Path
from bs4 import BeautifulSoup

DOWNLOADS = Path.home() / "Downloads"

PAGES = {
    "Public - Historical": DOWNLOADS / "SAM.gov _ Data Services.html",
    "Public V2": DOWNLOADS / "SAM.gov _ Data Services - Public V2.html",
}


def extract_files(html_path: Path) -> list[str]:
    """Extract file names from a saved SAM.gov data services page."""
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "html.parser")
    files = []
    for link in soup.select("a.data-service-file-link"):
        # Remove sr-only spans before extracting text
        for sr in link.select(".sr-only"):
            sr.decompose()
        text = link.get_text(strip=True)
        if text:
            files.append(text)
    return files


def main():
    manifest = {}
    for folder, html_path in PAGES.items():
        if not html_path.exists():
            print(f"WARNING: {html_path} not found, skipping")
            continue
        files = extract_files(html_path)
        manifest[folder] = files
        print(f"\n=== {folder} ({len(files)} files) ===")
        for f in files:
            print(f"  {f}")

    out = Path(__file__).parent / "file_manifest.json"
    out.write_text(json.dumps(manifest, indent=2))
    print(f"\nManifest written to {out}")


if __name__ == "__main__":
    main()
