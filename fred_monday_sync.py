import requests
import json
import os
from typing import Dict, Any, List, Optional, Tuple

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
COL_SYMBOL = "text_mkwxpng"
COL_RATE   = "numeric_mkwxeqs"    # Current Rate (%)
COL_INDEX  = "numeric_mkzvts68"   # Index/levels
COL_DATE   = "date4"             # Last Updated
COL_SOURCE = "text_mkwxc0yj"     # Source
COL_DELTA  = "numeric_mm0k5gy4"  # Change (Δ)

# ---------------------------------------------------
# Series routing rule (fast + stable, no metadata)
# ---------------------------------------------------
def is_rate_series(symbol: str) -> bool:
    s = symbol.upper().strip()
    rate_keywords = [
        "DGS", "SOFR", "PRIME", "FEDFUNDS", "MORTGAGE", "UNRATE",
        "DRCL", "DRTS", "RATE", "BSBY", "SWAP"
    ]
    return any(k in s for k in rate_keywords)

def monday_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.post(
        MONDAY_API_URL,
        headers={"Authorization": MONDAY_API_KEY, "Content-Type": "application/json"},
        json=payload,
        timeout=30
    )
    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"Monday decode error (HTTP {r.status_code}): {r.text}")

    if not r.ok:
        raise RuntimeError(f"Monday HTTP {r.status_code}: {data}")

    if "errors" in data:
        raise RuntimeError(f"Monday GraphQL errors: {data['errors']}")

    return data.get("data", {})

# ---------------------------------------------------
# Pull all items with Symbol + both numeric cols (for delta + clearing)
# ---------------------------------------------------
def fetch_all_items() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    cursor = None

    col_ids = [COL_SYMBOL, COL_RATE, COL_INDEX]
    col_ids_str = ",".join([f"\"{c}\"" for c in col_ids])

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
                column_values(ids: [{col_ids_str}]) {{
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
            cv_map = {cv["id"]: (cv.get("text") or "").strip() for cv in (it.get("column_values") or [])}
            items.append({
                "id": str(it["id"]),
                "name": it.get("name", ""),
                "symbol": cv_map.get(COL_SYMBOL, "").strip(),
                "prev_rate": cv_map.get(COL_RATE, ""),
                "prev_index": cv_map.get(COL_INDEX, "")
            })

        cursor = page.get("cursor")
        if not cursor:
            break

    return items

# ---------------------------------------------------
# FRED latest value + observation date (YYYY-MM-DD)
# ---------------------------------------------------
def fetch_latest_fred_value_and_date(series_id: str) -> Tuple[float, str]:
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
        d = obs.get("date")
        if v not in ("", ".", None) and d:
            return float(v), str(d)

    raise Exception(f"No valid observation for {series_id}")

def parse_float_maybe(s: str) -> Optional[float]:
    if not s:
        return None
    try:
        return float(s.replace("%", "").replace(",", "").strip())
    except Exception:
        return None

# ---------------------------------------------------
# Update Monday item (write target, clear other, set meta, delta)
# ---------------------------------------------------
def update_item(item: Dict[str, Any], new_value: float, fred_date: str) -> None:
    item_id = item["id"]
    symbol = item["symbol"]

    target_is_rate = is_rate_series(symbol)
    target_col = COL_RATE if target_is_rate else COL_INDEX
    clear_col  = COL_INDEX if target_is_rate else COL_RATE

    # Rounding rules
    if target_is_rate:
        write_value = round(new_value, 2)
        prev_val = parse_float_maybe(item.get("prev_rate", ""))
        delta_round = 2
    else:
        write_value = new_value  # keep raw for index/levels
        prev_val = parse_float_maybe(item.get("prev_index", ""))
        delta_round = 6

    delta_val = None
    if prev_val is not None:
        delta_val = write_value - prev_val

    vals: Dict[str, Any] = {
        target_col: str(write_value),
        clear_col: "",  # clear stale data in the non-target numeric column
        COL_DATE: {"date": fred_date},  # ✅ use FRED observation date
        COL_SOURCE: "FRED",
        COL_SYMBOL: symbol
    }

    if delta_val is not None:
        vals[COL_DELTA] = str(round(delta_val, delta_round))

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
# Main
# ---------------------------------------------------
if __name__ == "__main__":
    all_items = fetch_all_items()

    updated = 0
    skipped_manual = 0
    failed = 0
    failures: List[str] = []

    for it in all_items:
        symbol = (it.get("symbol") or "").strip()

        # Skip manual items (SBA) where Symbol is blank
        if not symbol:
            skipped_manual += 1
            continue

        try:
            val, fred_date = fetch_latest_fred_value_and_date(symbol)
            update_item(it, val, fred_date)
            updated += 1
            print(f"✅ Updated {it['name']} ({symbol}) -> {val} | as of {fred_date}")
        except Exception as e:
            failed += 1
            msg = f"{it.get('name','')} ({symbol}) item {it.get('id')} : {e}"
            failures.append(msg)
            print(f"❌ {msg}")

    print("\n--- SUMMARY ---")
    print(f"Updated: {updated}")
    print(f"Skipped manual (blank symbol): {skipped_manual}")
    print(f"Failed: {failed}")

    if failures:
        print("\n--- FAILURES ---")
        for f in failures:
            print(f"- {f}")
