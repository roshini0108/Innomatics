import pandas as pd
import sqlite3
import os
import json

print("\n========== FOOD DELIVERY DATA PIPELINE ==========\n")

# ------------------------------------------------
# STEP 1 — LOAD ORDERS CSV
# ------------------------------------------------
print("Loading orders.csv ...")
orders = pd.read_csv("orders.csv")
print("Orders columns:", list(orders.columns))


# ------------------------------------------------
# STEP 2 — LOAD USERS JSON (robust loader)
# ------------------------------------------------
print("\nLoading users.json ...")

users = None

try:
    # Case 1 → normal JSON list
    users = pd.read_json("users.json")
    if len(users.columns) == 0:
        raise ValueError

except:
    try:
        # Case 2 → line-by-line JSON
        users = pd.read_json("users.json", lines=True)
    except:
        # Case 3 → nested JSON
        with open("users.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        users = pd.json_normalize(data)

print("Users columns:", list(users.columns))


# ------------------------------------------------
# STEP 3 — LOAD SQL → SQLITE
# ------------------------------------------------
print("\nLoading restaurants.sql ...")

# delete old DB so no 'already exists' error
if os.path.exists("restaurants.db"):
    os.remove("restaurants.db")

conn = sqlite3.connect("restaurants.db")

with open("restaurants.sql", "r", encoding="utf-8") as f:
    conn.executescript(f.read())

restaurants = pd.read_sql("SELECT * FROM restaurants", conn)

print("Restaurants columns:", list(restaurants.columns))


# ------------------------------------------------
# STEP 4 — STANDARDIZE COLUMN NAMES
# ------------------------------------------------
print("\nStandardizing column names...")

orders.columns = orders.columns.str.lower()
users.columns = users.columns.str.lower()
restaurants.columns = restaurants.columns.str.lower()

# auto rename common variations
rename_map = {
    "userid": "user_id",
    "user id": "user_id",
    "restaurantid": "restaurant_id",
    "restaurant id": "restaurant_id",
    "id": "user_id"
}

users = users.rename(columns=rename_map)
orders = orders.rename(columns=rename_map)
restaurants = restaurants.rename(columns=rename_map)


# ------------------------------------------------
# STEP 5 — MERGE DATA
# ------------------------------------------------
print("\nMerging datasets...")

if "user_id" not in users.columns:
    raise Exception("❌ users.json must contain user_id column")

if "restaurant_id" not in restaurants.columns:
    raise Exception("❌ restaurants.sql must contain restaurant_id column")

merged = pd.merge(orders, users, on="user_id", how="left")
final_df = pd.merge(merged, restaurants, on="restaurant_id", how="left")


# ------------------------------------------------
# STEP 6 — SAVE FINAL FILE
# ------------------------------------------------
final_df.to_csv("final_food_delivery_dataset.csv", index=False)

print("\n✅ SUCCESS!")
print("Created → final_food_delivery_dataset.csv")
print("Rows:", len(final_df))
print("\n==============================================")