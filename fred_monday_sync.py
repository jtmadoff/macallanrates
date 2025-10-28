import requests
import json
import os

# ==== CONFIG ====
MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")
BOARD_ID = int(os.getenv("BOARD_ID"))

# Map each row’s FRED series ID to its Monday item ID
SERIES_MAP = {
    "SOFR": "item_id_here_1",
    "DGS10": "item_id_here_2",
    "CPIAUCSL": "item_id_here_3"
}

# Replace with your actual Monday column IDs
COLUMN_MAP = {
    "rate": "current_rate",
    "date": "last_updated",
    "source": "source"
}

def get_latest_fred_value(series_id):
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json"
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    obs = data["observations"][-1]
    return float(obs["value"]), obs["date"]

def update_monday_item(item_id, rate, date):
    query = """
    mutation ($board: Int!, $item: Int!, $vals: JSON!) {
      change_multiple_column_values(board_id: $board, item_id: $item, column_values: $vals) {
        id
      }
    }
    """
    vals = {
        COLUMN_MAP["rate"]: str(rate),
        COLUMN_MAP["date"]: {"date": date},
        COLUMN_MAP["source"]: "FRED"
    }
    data = {
        "query": query,
        "variables": {
            "board": BOARD_ID,
            "item": item_id,
            "vals": json.dumps(vals)
        }
    }
    resp = requests.post(
        "https://api.monday.com/v2",
        headers={"Authorization": MONDAY_API_KEY, "Content-Type": "application/json"},
        json=data
    )
    if not resp.ok or "errors" in resp.json():
        print("Monday.com error:", resp.text)
        resp.raise_for_status()

if __name__ == "__main__":
    for symbol, item_id in SERIES_MAP.items():
        try:
            rate, date = get_latest_fred_value(symbol)
            update_monday_item(item_id, rate, date)
            print(f"✅ {symbol} updated to {rate}% ({date})")
        except Exception as e:
            print(f"❌ Error updating {symbol}: {e}")
