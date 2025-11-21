import csv
import sys
import time
import os
from typing import List, Tuple, Optional

import requests

# ========= CONFIGURATION =========
BRAND_ID = 2005  # <-- put your brand ID here
BEARER_TOKEN = "c81cf09ca837e9323260cca8da760572dc9d3fb5f1cf6c4ed46d1dcd5e0c"  # <-- put your token here

# Path to your input CSV (must contain a column named "product_source_id")
INPUT_CSV_PATH = "URBN product source ID.csv"
# Output CSV will be created/updated incrementally
OUTPUT_CSV_PATH = "product_media_report.csv"

REQUEST_DELAY_SECONDS = 0.2

LIBRARY_BASE_URL = "https://app.dashhudson.com"
LIBRARY_BACKEND_BASE_URL = "https://library-backend.dashhudson.com"
AUTH_BASE_URL = "https://auth.dashhudson.com"


def get_brand_name(brand_id: int) -> str:
    """
    Call /api/self and find the brand_name (key in 'brands' dict)
    whose brands[brand_name].id == brand_id.
    Returns the brand_name string (for example 'sunny-today').
    """
    url = f"{AUTH_BASE_URL}/api/self"
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Accept": "application/json",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=30)
    except requests.RequestException as exc:
        print(f"Error calling /api/self: {exc}", file=sys.stderr)
        sys.exit(1)

    if resp.status_code != 200:
        print(
            f"Non-200 from /api/self: {resp.status_code} {resp.text}",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        data = resp.json()
    except ValueError:
        print("Invalid JSON from /api/self", file=sys.stderr)
        sys.exit(1)

    brands = data.get("brands") or {}
    for brand_name, brand_obj in brands.items():
        if brand_obj.get("id") == brand_id:
            return brand_name

    print(f"Brand ID {brand_id} not found in /api/self response", file=sys.stderr)
    sys.exit(1)


def fetch_media_for_product_source_id(product_source_id: str) -> List[dict]:
    """
    Call the products media endpoint for a given product_source_id.
    Returns a list of media objects (JSON array).
    """
    url = f"{LIBRARY_BACKEND_BASE_URL}/public/brands/{BRAND_ID}/products/media"
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Accept": "application/json",
    }
    params = {"product_source_id": product_source_id}

    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
    except Exception as e:
        print(f"Request error for {product_source_id}: {e}", file=sys.stderr)
        return []

    if r.status_code != 200:
        print(
            f"Non-200 ({r.status_code}) for {product_source_id}: {r.text}",
            file=sys.stderr,
        )
        return []

    try:
        data = r.json()
    except Exception:
        print(f"JSON parse error for {product_source_id}", file=sys.stderr)
        return []

    return data if isinstance(data, list) else []


def extract_product_info(
    media_items: List[dict],
    product_source_id: str,
) -> Tuple[Optional[int], Optional[str], int, List[str]]:
    """
    From the list of media items, find:
      - dash_id (product.id)
      - product_url (product.url)
      - media_count (len(media_items))
      - media_image_urls (list of image_sizes.original.url from each media)

    We match the product using:
      - product_tag.source_id == product_source_id
      - OR product.source_id == product_source_id
      - OR any product.product_overrides[*].source_id == product_source_id
    """
    dash_id: Optional[int] = None
    product_url: Optional[str] = None
    media_image_urls: List[str] = []

    for media in media_items:
        # collect image url
        image_sizes = media.get("image_sizes") or {}
        orig = image_sizes.get("original") or {}
        if orig.get("url"):
            media_image_urls.append(orig["url"])

        # once we have product info we can skip searching
        if dash_id is not None and product_url is not None:
            continue

        # search for matching product
        for tag in media.get("products") or []:
            product = tag.get("product") or {}
            overrides = product.get("product_overrides") or []

            override_match = any(
                (ov or {}).get("source_id") == product_source_id for ov in overrides
            )

            if (
                tag.get("source_id") == product_source_id
                or product.get("source_id") == product_source_id
                or override_match
            ):
                if dash_id is None:
                    dash_id = product.get("id") or tag.get("product_id")
                if product_url is None:
                    # use product.url instead of product.original_url
                    product_url = product.get("url")
                break

    media_count = len(media_items)
    return dash_id, product_url, media_count, media_image_urls


def read_product_source_ids(path: str) -> List[str]:
    """
    Read product_source_id values from the input CSV.
    Expects a column named 'product_source_id'.
    """
    values: List[str] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "product_source_id" not in (reader.fieldnames or []):
            raise ValueError(
                f"Input CSV {path} must contain a 'product_source_id' column. "
                f"Found columns: {reader.fieldnames}"
            )
        for row in reader:
            value = (row.get("product_source_id") or "").strip()
            if value:
                values.append(value)
    return values


def load_already_processed_rows(path: str) -> set:
    """
    If the output CSV already exists, read it and return the set of
    product_id values (which correspond to product_source_id input)
    already processed so we can resume.
    """
    if not os.path.exists(path):
        return set()

    processed = set()
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "product_id" not in (reader.fieldnames or []):
            # Different schema from a previous run; ignore resume to avoid confusion.
            return set()

        for row in reader:
            processed.add(row["product_id"])
    return processed


def append_row(path: str, row: dict, write_header_if_needed: bool = True) -> None:
    """
    Append a single row to the output CSV.
    Writes the header if the file did not exist before.
    """
    fieldnames = [
        "product_id",         # this is product_source_id from the feed
        "dash_id",            # internal Dash product.id
        "dash_library_link",
        "product_url",
        "media_count",
        "media_image_urls"
    ]

    file_exists = os.path.exists(path)
    write_header = (not file_exists) and write_header_if_needed

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        if write_header:
            writer.writeheader()

        writer.writerow(row)
        f.flush()  # ensure immediate write to disk


def build_dash_library_link(brand_name: Optional[str], dash_id: Optional[int]) -> str:
    """
    Build the Dash Library link:
      https://app.dashhudson.com/{brand_name}/library/products?d=product%7CproductId%3A{dash_id}
    Returns empty string if brand_name or dash_id is missing.
    """
    if not brand_name or not dash_id:
        return ""
    return (
        f"{LIBRARY_BASE_URL}/{brand_name}/library/products"
        f"?d=product%7CproductId%3A{dash_id}"
    )


def main() -> None:
    if not BEARER_TOKEN or BEARER_TOKEN == "YOUR_BEARER_TOKEN_HERE":
        print("Please set BEARER_TOKEN at the top of the script.", file=sys.stderr)
        sys.exit(1)

    # Fetch brand_name once from /api/self
    print(f"Fetching brand_name for brand_id={BRAND_ID} from /api/self...")
    brand_name = get_brand_name(BRAND_ID)
    print(f"Using brand_name: {brand_name}")

    print(f"Reading product_source_id values from {INPUT_CSV_PATH}...")
    product_source_ids = read_product_source_ids(INPUT_CSV_PATH)
    print(f"Found {len(product_source_ids)} product_source_id values.")

    processed = load_already_processed_rows(OUTPUT_CSV_PATH)
    if processed:
        print(f"Resuming: {len(processed)} product_id values already processed.")

    for idx, product_source_id in enumerate(product_source_ids, start=1):
        if product_source_id in processed:
            print(f"[{idx}] Skipping {product_source_id} (already processed)")
            continue

        print(f"[{idx}] Fetching media for {product_source_id}...")
        media_items = fetch_media_for_product_source_id(product_source_id)

        dash_id, product_url, media_count, media_image_urls = extract_product_info(
            media_items, product_source_id
        )

        dash_library_link = build_dash_library_link(brand_name, dash_id)

        row = {
            "product_id": product_source_id,                 # renamed from product_source_id
            "dash_id": dash_id if dash_id is not None else "",
            "dash_library_link": dash_library_link,
            "product_url": product_url or "",
            "media_count": media_count,                      # 0 if no media
            "media_image_urls": "; ".join(media_image_urls)
        }

        append_row(OUTPUT_CSV_PATH, row)
        time.sleep(REQUEST_DELAY_SECONDS)

    print("Done. Rows were saved incrementally, safe to interrupt and resume.")


if __name__ == "__main__":
    main()
