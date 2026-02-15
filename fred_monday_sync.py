import requests
import json
import os
import datetime

# ==== CONFIG ====
MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")
BOARD_ID = os.getenv("BOARD_ID")
MONDAY_API_URL = "https://api.monday.com/v2"

if not MONDAY_API_KEY:
    raise RuntimeError("Missing MONDAY_API_KEY")
if not FRED_API_KEY:
    raise RuntimeError("Missing FRED_API_KEY")
if not BOARD_ID:
    raise RuntimeError("Missing BOARD_ID")

# ---- Monday column IDs ----
COLUMN_SYMBOL = "text_mkwxpng"
COLUMN_RATE = "numeric_mkwxeqs"
COLUMN_INDEX = "numeric_mkzvts68"
COLUMN_DATE = "date4"
COLUMN_SOURCE = "text_mkwxc0yj"


# ---------------------------------------------------
# Utility: detect whether symbol should go in RATE column
# ---------------------------------------------------
def is_rate_series(symbol: str) -> bool:
    s = symbol.upper()
    rate_keywords = [
        "DGS",
        "SOFR",
        "PRIME",
        "FEDFUNDS",
        "MORTGAGE",
        "UNRATE",
        "DRCL",
        "DRTS",
        "RATE"
    ]
    return any(k in s for k in rate_keywords)


# ---------------------------------------------------
# Monday GraphQL helper
# ---------------------------------------------------
def monday_request(payload: dict) -> dict:
    r = requests.post(
        MONDAY_API_URL,
        headers={
            "Authorization": MONDAY_API_KEY,
            "Content-Type": "application/json"
        },
        json=payload,
        timeout=30
    )

    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"Monday decode error: {r.text}")

    if not r.ok:
        raise RuntimeError(f"Monday HTTP {r.status_code}: {data}")

    if "errors" in data:
        raise RuntimeError(f"Monday GraphQL error: {data['errors']}")

    return data.get("data", {})


# ---------------------------------------------------
# Pull all items from board
# ---------------------------------------------------
def fetch_all_items():
    items = []
    cursor = None

    while True:
        cursor_clause = f', cursor: "{cursor}"' if cursor else ""
        query = f"""
        query {{
          boards(ids: {BOARD_ID}) {{
            items_page(limit: 500{cursor_clause}) {{
              cursor
              items {{
                id
                name
                column_values(ids: ["{COLUMN_SYMBOL}"]) {{
                  text
                }}
              }}
            }}
          }}
        }}
        """

        data = monday_request({"query": query})
        page = data["boards"][0]["items_page"]

        for it in page["items"]:
            symbol_text = ""
            cvs = it.get("column_values", [])
            if cvs:
                symbol_text = (cvs[0].get("text") or "").strip()

            items.append({
                "id": str(it["id"]),
                "name": it.get("name", ""),
                "symbol": symbol_text
            })

        cursor = page.get("cursor")
        if not cursor:
            break

    return items


# ---------------------------------------------------
# Get latest FRED value
# ---------------------------------------------------
def fetch_latest_fred_value(series_id: str) -> float:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 20
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    for obs in data.get("observations", []):
        v = obs.get("value")
        if v not in ("", ".", None):
            return float(v)

    raise Exception(f"No valid observation for {series_id}")


# ---------------------------------------------------
# Update Monday item
# ---------------------------------------------------
def update_item(item_id: str, symbol: str, value: float):
    today = datetime.date.today().isoformat()

    target_column = COLUMN_RATE if is_rate_series(symbol) else COLUMN_INDEX

    vals = {
        target_column: str(value),
        COLUMN_DATE: {"date": today},
        COLUMN_SOURCE: "FRED",
        COLUMN_SYMBOL: symbol
    }

    mutation = """
    mutation ($board: ID!, $item: ID!, $vals: JSON!) {
      change_multiple_column_values(board_id: $board, item_id: $item, column_values: $vals) {
        id
      }
    }
    """

    payload = {
        "query": mutation,
        "variables": {
            "board": str(BOARD_ID),
            "item": str(item_id),
            "vals": json.dumps(vals)
        }
    }

    monday_request(payload)


# ---------------------------------------------------
# Main sync
# ---------------------------------------------------
if __name__ == "__main__":
    items = fetch_all_items()

    updated = 0
    skipped = 0
    failed = 0

    for it in items:
        symbol = (it["symbol"] or "").strip()

        # Skip manual rows (SBA, etc.)
        if not symbol:
            skipped += 1
            continue

        try:
            value = fetch_latest_fred_value(symbol)
            update_item(it["id"], symbol, value)
            updated += 1
            print(f"✅ Updated {it['name']} ({symbol}) -> {value}")
        except Exception as e:
            failed += 1
            print(f"❌ Failed {it['name']} ({symbol}): {e}")

    print("\n--- SUMMARY ---")
    print(f"Updated: {updated}")
    print(f"Skipped (manual/no symbol): {skipped}")
    print(f"Failed: {failed}")
