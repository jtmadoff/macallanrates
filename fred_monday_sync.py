import requests
import json
import os
import datetime

# ==== CONFIG ====
MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")
BOARD_ID = os.getenv("BOARD_ID")  # string ok
MONDAY_API_URL = "https://api.monday.com/v2"

if not MONDAY_API_KEY:
    raise RuntimeError("Missing MONDAY_API_KEY env var")
if not FRED_API_KEY:
    raise RuntimeError("Missing FRED_API_KEY env var")
if not BOARD_ID:
    raise RuntimeError("Missing BOARD_ID env var")

# ---- Monday column IDs (from your board) ----
COLUMN_MAP = {
    "symbol": "text_mkwxpng",
    "rate": "numeric_mkwxeqs",      # Current Rate (%)
    "index": "numeric_mkzvts68",    # Index/levels
    "date": "date4",               # Last Updated
    "source": "text_mkwxc0yj"       # Source
}

# ---- Group routing ----
RATES_GROUP_TITLE = "Rates"
INDEX_GROUP_TITLE = "Index"


def monday_request(payload: dict) -> dict:
    resp = requests.post(
        MONDAY_API_URL,
        headers={"Authorization": MONDAY_API_KEY, "Content-Type": "application/json"},
        json=payload,
        timeout=30
    )
    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"Could not decode Monday response: HTTP {resp.status_code} {resp.text}")

    if not resp.ok:
        raise RuntimeError(f"Monday HTTP {resp.status_code}: {data}")

    if "errors" in data:
        raise RuntimeError(f"Monday GraphQL errors: {data['errors']}")

    return data.get("data", {})


def fetch_latest_fred_value(series_id: str) -> float:
    """
    Returns latest non-missing numeric value for a FRED series.
    """
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 20  # grab a few in case latest is "."
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    obs = data.get("observations", [])
    for o in obs:
        v = o.get("value")
        if v not in ("", ".", None):
            return float(v)

    raise Exception(f"No valid observation for {series_id}")


def fetch_all_board_items() -> list[dict]:
    """
    Pulls every item in BOARD_ID with its group title + symbol text.
    Uses items_page pagination.
    """
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
                group {{ title }}
                column_values(ids: ["{COLUMN_MAP["symbol"]}"]) {{
                  id
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
            if cvs and isinstance(cvs, list):
                symbol_text = (cvs[0].get("text") or "").strip()

            items.append({
                "id": str(it["id"]),
                "name": it.get("name", ""),
                "group": (it.get("group", {}) or {}).get("title", "") or "",
                "symbol": symbol_text
            })

        cursor = page.get("cursor")
        if not cursor:
            break

    return items


def update_monday_item(item_id: str, symbol: str, value: float, group_title: str) -> None:
    """
    Writes value into the correct column based on group:
      - Rates -> Current Rate (%)
      - Index -> Index/levels
    Always sets:
      - Last Updated = today
      - Source = FRED
      - Symbol column = symbol (keeps it normalized)
    """
    today = datetime.date.today().isoformat()

    group_norm = group_title.strip().lower()
    is_index = (group_norm == INDEX_GROUP_TITLE.lower())
    target_col = COLUMN_MAP["index"] if is_index else COLUMN_MAP["rate"]

    vals = {
        target_col: str(value),
        COLUMN_MAP["date"]: {"date": today},
        COLUMN_MAP["source"]: "FRED",
        COLUMN_MAP["symbol"]: symbol
    }

    query = """
    mutation ($board: ID!, $item: ID!, $vals: JSON!) {
      change_multiple_column_values(board_id: $board, item_id: $item, column_values: $vals) {
        id
      }
    }
    """

    payload = {
        "query": query,
        "variables": {
            "board": str(BOARD_ID),
            "item": str(item_id),
            "vals": json.dumps(vals)
        }
    }

    monday_request(payload)


if __name__ == "__main__":
    all_items = fetch_all_board_items()

    updated = 0
    skipped_manual = 0
    failed = 0

    for it in all_items:
        item_id = it["id"]
        name = it["name"]
        group_title = it["group"]
        symbol = (it["symbol"] or "").strip()

        # Skip manual rows (SBA etc.) where symbol is blank
        if not symbol:
            skipped_manual += 1
            continue

        try:
            value = fetch_latest_fred_value(symbol)
            update_monday_item(item_id, symbol, value, group_title)
            updated += 1
            print(f"✅ Updated {name} [{group_title}] ({symbol}) -> {value}")
        except Exception as e:
            failed += 1
            print(f"❌ Error updating {name} [{group_title}] ({symbol}) (item {item_id}): {e}")

    print("\n--- SUMMARY ---")
    print(f"Updated: {updated}")
    print(f"Skipped manual (blank symbol): {skipped_manual}")
    print(f"Failed: {failed}")
