import requests
import json
import os
import datetime
from typing import Dict, Any, List, Optional

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

# ---- Monday column IDs (from your board) ----
COL_SYMBOL = "text_mkwxpng"
COL_RATE   = "numeric_mkwxeqs"    # Current Rate (%)
COL_INDEX  = "numeric_mkzvts68"   # Index/levels
COL_DATE   = "date4"             # Last Updated
COL_SOURCE = "text_mkwxc0yj"     # Source

# OPTIONAL: if you add a "Change (Δ)" numbers column in Monday, set its column ID here.
# Example: COL_DELTA = "numeric_abc123"
COL_DELTA: Optional[str] = None

# ---------------------------------------------------
# Series routing rule (fast + stable, no metadata)
# ---------------------------------------------------
def is_rate_series(symbol: str) -> bool:
    s = symbol.upper().strip()
    rate_keywords = [
        "DGS",          # Treasuries
        "SOFR",         # SOFR + SOFR30DAYAVG
        "PRIME",        # MPRIME/DPRIME
        "FEDFUNDS",     # Fed Funds
        "MORTGAGE",     # MORTGAGE30US
        "UNRATE",       # Unemployment rate
        "DRCL",         # Delinquency rate
        "DRTS",         # Lending standards index-ish but you want it in rate col (fine)
        "RATE",         # catch-all
        "BSBY",         # if you add it later
        "SWAP"          # if you ever add swap series ids
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
# Pull all items with Symbol + both numeric columns (for delta + clearing)
# ---------------------------------------------------
def fetch_all_items() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    cursor = None

    # We pull existing numeric values so we can compute delta.
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
            # Map column id -> text
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
# FRED latest value
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

def parse_float_maybe(s: str) -> Optional[float]:
    if not s:
        return None
    try:
        # strip percent sign if Monday shows it
        return float(s.replace("%", "").replace(",", "").strip())
    except Exception:
        return None

# ---------------------------------------------------
# Update Monday item (write target, clear other, set meta, optional delta)
# ---------------------------------------------------
def update_item(item: Dict[str, Any], new_value: float) -> None:
    item_id = item["id"]
    symbol = item["symbol"]

    today = datetime.date.today().isoformat()
    target_is_rate = is_rate_series(symbol)

    target_col = COL_RATE if target_is_rate else COL_INDEX
    clear_col  = COL_INDEX if target_is_rate else COL_RATE

    # Rounding rules
    if target_is_rate:
        write_value = round(new_value, 2)
        prev_val = parse_float_maybe(item.get("prev_rate", ""))
    else:
        # Keep index raw (no rounding) – if you want rounding, change here
        write_value = new_value
        prev_val = parse_float_maybe(item.get("prev_index", ""))

    delta_val = None
    if prev_val is not None:
        delta_val = write_value - prev_val

    vals: Dict[str, Any] = {
        target_col: str(write_value),
        clear_col: "",  # clears other numeric column to prevent stale data
        COL_DATE: {"date": today},
        COL_SOURCE: "FRED",
        COL_SYMBOL: symbol
    }

    # Optional delta column update
    if COL_DELTA and delta_val is not None:
        # round delta lightly; rates delta usually 2dp is fine
        vals[COL_DELTA] = str(round(delta_val, 2) if target_is_rate else delta_val)

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
            val = fetch_latest_fred_value(symbol)
            update_item(it, val)
            updated += 1
            print(f"✅ Updated {it['name']} ({symbol}) -> {val}")
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
