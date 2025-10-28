import requests
import json
import os

# ==== CONFIG ====
MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")
BOARD_ID = os.getenv("BOARD_ID")  # <-- No int()!

# ... rest unchanged ...

def update_monday_item(item_id, rate, date):
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
        COLUMN_MAP["source"]: "FRED"
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
    if not resp.ok or "errors" in resp.json():
        print("Monday.com error:", resp.text)
        resp.raise_for_status()
