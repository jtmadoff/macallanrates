import requests
import json
import os

# ==== CONFIG ====
MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")
BOARD_ID = os.getenv("BOARD_ID")  # Must be a string!

# Map each FRED series ID to its Monday item ID (these are your real item IDs)
SERIES_MAP = {
    "SOFR": "18225199389",
    "DGS10": "18225199408",
    "CPIAUCSL": "18225199433"
    "FEDFUNDS": "18284354330"
    "MPRIME": "18284354350"
    "MORTGAGE30US": "18284354378"
    "COMREPONSAMR": "18284354402"
    "COMLOANS": "18284354421",  # CRE Loans, All Commercial Banks
    "DRCLACBS": "18284354437",  # CRE Loan Delinquency Rate
    "TLNRESCONS": "18284354457",  # Private Nonresidential Construction Spending
    "UNRATE": "18284354474",    # Unemployment Rate
    "GDP": "18284354486",       # GDP
    "PPIFGS": "18284354505",    # PPI: Finished Goods
    "DRTSCLCC": "18284354515",  # CRE Lending Standards (Senior Loan Survey)
}
}

# Your actual Monday column IDs
COLUMN_MAP = {
    "symbol": "text_mkwxpng",
    "rate": "numeric_mkwxeqs",
    "date": "date4",
    "source": "text_mkwxc0yj"
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

def update_monday_item(item_id, symbol, rate, date):
    query = """
    mutation ($board: ID!, $item: ID!, $vals: JSON!) {
      change_multiple_column_values(board_id: $board, item_id: $item, column_values: $vals) {
        id
      }
    }
    """
    vals = {
        COLUMN_MAP["rate"]: str(rate),
        COLUMN_MAP["date"]: {"date": date},
        COLUMN_MAP["source"]: "FRED",
        COLUMN_MAP["symbol"]: symbol
    }
    data = {
        "query": query,
        "variables": {
            "board": str(BOARD_ID),
            "item": str(item_id),
            "vals": json.dumps(vals)
        }
    }
    resp = requests.post(
        "https://api.monday.com/v2",
        headers={"Authorization": MONDAY_API_KEY, "Content-Type": "application/json"},
        json=data
    )
    try:
        resp_json = resp.json()
    except Exception:
        print("❌ Error decoding Monday.com response:", resp.text)
        return False

    if not resp.ok or "errors" in resp_json:
        print("❌ Monday.com error:", resp_json)
        return False
    print(f"✅ Updated item {item_id} ({symbol}) to {rate} on {date}")
    return True

if __name__ == "__main__":
    for symbol, item_id in SERIES_MAP.items():
        try:
            rate, date = get_latest_fred_value(symbol)
            success = update_monday_item(item_id, symbol, rate, date)
            if not success:
                print(f"❌ Failed to update {symbol} ({item_id})")
        except Exception as e:
            print(f"❌ Error updating {symbol}: {e}")
