import requests
import pandas as pd
import logging
import re
import json
import pyodbc
from datetime import datetime, timedelta

# ——— STATE ABBREVIATION MAP & CLEANUP ———
us_state_abbrev = {
    'ALABAMA':'AL','ALASKA':'AK','ARIZONA':'AZ','ARKANSAS':'AR','CALIFORNIA':'CA',
    'COLORADO':'CO','CONNECTICUT':'CT','DELAWARE':'DE','FLORIDA':'FL','GEORGIA':'GA',
    'HAWAII':'HI','IDAHO':'ID','ILLINOIS':'IL','INDIANA':'IN','IOWA':'IA',
    'KANSAS':'KS','KENTUCKY':'KY','LOUISIANA':'LA','MAINE':'ME','MARYLAND':'MD',
    'MASSACHUSETTS':'MA','MICHIGAN':'MI','MINNESOTA':'MN','MISSISSIPPI':'MS','MISSOURI':'MO',
    'MONTANA':'MT','NEBRASKA':'NE','NEVADA':'NV','NEW HAMPSHIRE':'NH','NEW JERSEY':'NJ',
    'NEW MEXICO':'NM','NEW YORK':'NY','NORTH CAROLINA':'NC','NORTH DAKOTA':'ND','OHIO':'OH',
    'OKLAHOMA':'OK','OREGON':'OR','PENNSYLVANIA':'PA','RHODE ISLAND':'RI','SOUTH CAROLINA':'SC',
    'SOUTH DAKOTA':'SD','TENNESSEE':'TN','TEXAS':'TX','UTAH':'UT','VERMONT':'VT',
    'VIRGINIA':'VA','WASHINGTON':'WA','WEST VIRGINIA':'WV','WISCONSIN':'WI','WYOMING':'WY',
    'DISTRICT OF COLUMBIA':'DC'
}

def clean_state(state):
    if not state:
        return ''
    s = state.strip().upper()
    if len(s) == 2 and s in us_state_abbrev.values():
        return s
    return us_state_abbrev.get(s, s)

# ——— LOGGING ———
logging.basicConfig(
    filename='veeqo_open_orders.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ——— LOAD CONFIG ———
with open(r'\\nasow\CustomerService\Python\config.json') as f:
    data = json.load(f)
username = data['username']
password = data['password']

# ——— DB CONNECTION ———
try:
    conn_ow = pyodbc.connect(f'DSN=IBS-OW;UID={username};PWD={password}')
    logging.info("Connected to IBS database.")
except Exception as e:
    logging.error(f"Connection to IBS-OW failed: {e}")
    raise SystemExit(e)

# ——— BATCH ORDER CHECK FUNCTION ———
def get_existing_orders(conn_ow, order_numbers):
    if not order_numbers:
        return set()
    existing = set()
    order_numbers = list(order_numbers)
    batch_size = 900  # ODBC param limit is typically 1000, 900 is safe
    cursor = conn_ow.cursor()
    for i in range(0, len(order_numbers), batch_size):
        batch = order_numbers[i:i+batch_size]
        placeholders = ",".join(["?"] * len(batch))
        sql = f"""
            SELECT OHOREF, OHCOPE FROM OW1664AFOW.SRBSOH
            WHERE OHOREF IN ({placeholders}) OR OHCOPE IN ({placeholders})
        """
        params = batch + batch  # both OHOREF and OHCOPE
        cursor.execute(sql, params)
        for row in cursor.fetchall():
            if row[0]: existing.add(str(row[0]).strip())
            if row[1]: existing.add(str(row[1]).strip())
    cursor.close()
    return existing

# ——— VEEQO/OUTPUT SETTINGS ———
api_key = "Vqt/26a63daf5e6fa90936385234108ec6d8"
headers = {'x-api-key': api_key}
store_to_dealer = {
    "Test": "AMAZON",
    "Phone": "AMAZON",
    "Amazon Channel": "AMAZON",
    "Wright Parts": "WMMKT",
    "PRORUN": "11102W",
    "PRORUN Store": "11102A",
    "proruntools": "11102E"
}
columns = [
    "DEALER#", "WAREHOUSE", "PO#", "ORDER DATE", "SHIP BY",
    "CUSTOMER NAME", "CUSTOMER PHONE NUMBER", "SHIP TO ADDRESS 1",
    "SHIP TO ADDRESS 2", "CITY", "STATE", "ZIP", "LINE#", "SKU",
    "QTY", "REQUESTED CARRIER METHOD"
]

def clean_phone_number(phone):
    if not phone:
        return "6369781313"
    phone = re.sub(r"^\+1\s*", "", phone)
    phone = re.sub(r"\s*ext\.\s*\d+$", "", phone)
    phone = re.sub(r"[()\-\s]", "", phone)
    return phone

def get_shipping_method(dealer_number, order):
    is_prime = any("urgent" in tag.get("name", "").lower() for tag in order.get("tags", []))
    dexp = order.get("delivery_expectation", {})
    ship_by, deliver_by = dexp.get("dispatch_by"), dexp.get("deliver_by")
    created = order.get("created_at", "")
    ship_date_str = (ship_by or created or "")[:10]
    delivery_date_str = (deliver_by or order.get("delivery_date") or order.get("due_date") or "")[:10]

    if dealer_number == "11102A":
        if is_prime:
            try:
                sd = datetime.strptime(ship_date_str, "%Y-%m-%d")
                dd = datetime.strptime(delivery_date_str, "%Y-%m-%d")
                if (dd - sd).days <= 2:
                    return "FEDEX HOME DELIVERY"
            except Exception as e:
                logging.warning(f"Could not calc window for {order.get('number')}: {e}")
        return "FEDEX HOME DELIVERY"
    if dealer_number in ("11102W","11102E"):
        return "FEDEX HOME DELIVERY"
    if dealer_number in ("AMAZON","WMMKT"):
        return "UPS SUREPOST"
    return order.get("delivery_method",{}).get("name","STANDARD").upper()

try:
    # Step 1: Fetch ALL orders from Veeqo
    all_orders, page, page_size = [], 1, 100
    while True:
        resp = requests.get(
            'https://api.veeqo.com/orders',
            headers=headers,
            params={'status': 'awaiting_fulfillment', 'page_size': page_size, 'page': page}
        )
        if not resp.ok:
            logging.error(f"Failed to retrieve page {page}: {resp.status_code}")
            break
        batch = resp.json()
        if not batch:
            break
        all_orders.extend(batch)
        if len(batch) < page_size:
            break
        page += 1
    logging.info(f"Total orders retrieved: {len(all_orders)}")

    # Step 2: Gather all order numbers from Veeqo
    veeqo_order_numbers = set()
    for o in all_orders:
        order_number = o.get("number", "UNKNOWN")
        if order_number and order_number != "UNKNOWN":
            veeqo_order_numbers.add(order_number)
    logging.info(f"Unique order numbers from Veeqo: {len(veeqo_order_numbers)}")

    # Step 3: Query existing order numbers from IBM i system in batch
    existing_orders = get_existing_orders(conn_ow, veeqo_order_numbers)
    logging.info(f"Order numbers already in system: {len(existing_orders)}")

    # Step 4: Build the export for NEW orders only
    rows = []
    for o in all_orders:
        order_number = o.get("number", "UNKNOWN")
        if order_number in existing_orders:
            logging.info(f"Order {order_number} found in system; skipping.")
            continue
        try:
            detail = requests.get(f"https://api.veeqo.com/orders/{o['id']}", headers=headers)
            if not detail.ok:
                logging.warning(f"Could not fetch details for {order_number}")
                continue
            order = detail.json()

            # No date logic; include all!
            ship_by = order.get("dispatch_date", "")[:10]

            store_name = order.get("channel", {}).get("name", "UNKNOWN")
            dealer_number = store_to_dealer.get(store_name, "UNKNOWN")

            alloc = order.get("allocations", [{}])[0]
            wname = alloc.get("warehouse", {}).get("name", "UNKNOWN")
            wh = re.search(r"\d+", wname)
            warehouse = wh.group(0) if wh else wname

            d = order.get("deliver_to", {})
            customer = f"{d.get('first_name','')} {d.get('last_name','')}".strip().upper()
            phone = clean_phone_number(d.get("phone", ""))
            state_code = clean_state(d.get("state", ""))
            ship_method = get_shipping_method(dealer_number, order)

            for li in order.get("line_items", []):
                sku = (li.get("sellable", {}).get("sku_code") or 
                       li.get("sellable", {}).get("product", {}).get("sku_code", "UNKNOWN")).upper()
                qty = li.get("quantity", 0)

                if "contents" in li.get("sellable", {}):
                    for comp in li["sellable"]["contents"]:
                        comp_sku = (comp.get("sku_code") or
                                   comp.get("product", {}).get("sku_code", "UNKNOWN")).upper()
                        per_bundle = comp.get("quantity", 0)
                        total = qty * per_bundle
                        logging.debug(f"{order_number} line {li.get('line_number')}: {qty}×{per_bundle}→{total}")
                        rows.append({
                            "DEALER#": dealer_number,
                            "WAREHOUSE": warehouse,
                            "PO#": order_number,
                            "ORDER DATE": order.get("created_at", "")[:10],
                            "SHIP BY": ship_by,
                            "CUSTOMER NAME": customer,
                            "CUSTOMER PHONE NUMBER": phone,
                            "SHIP TO ADDRESS 1": d.get("address1", "").upper(),
                            "SHIP TO ADDRESS 2": (d.get("address2") or "").upper(),
                            "CITY": d.get("city", "").upper(),
                            "STATE": state_code,
                            "ZIP": d.get("zip", ""),
                            "LINE#": li.get("line_number", 1),
                            "SKU": comp_sku,
                            "QTY": total,
                            "REQUESTED CARRIER METHOD": ship_method
                        })
                else:
                    rows.append({
                        "DEALER#": dealer_number,
                        "WAREHOUSE": warehouse,
                        "PO#": order_number,
                        "ORDER DATE": order.get("created_at", "")[:10],
                        "SHIP BY": ship_by,
                        "CUSTOMER NAME": customer,
                        "CUSTOMER PHONE NUMBER": phone,
                        "SHIP TO ADDRESS 1": d.get("address1", "").upper(),
                        "SHIP TO ADDRESS 2": (d.get("address2") or "").upper(),
                        "CITY": d.get("city", "").upper(),
                        "STATE": state_code,
                        "ZIP": d.get("zip", ""),
                        "LINE#": li.get("line_number", 1),
                        "SKU": sku,
                        "QTY": qty,
                        "REQUESTED CARRIER METHOD": ship_method
                    })
            logging.info(f"Successfully processed order {order_number}")
        except Exception as e:
            logging.error(f"Error processing {order_number}: {e}")

    # Step 5: Write to Excel if there are rows
    if rows:
        df = pd.DataFrame(rows, columns=columns)
        out = r"\\nasow\CustomerService\Python\SalesOrderImport\open_orders_with_warehouse3.0.xlsx"
        df.to_excel(out, index=False)
        print(f"Excel file '{out}' created successfully.")
        logging.info(f"Excel file '{out}' created successfully.")
    else:
        print("No data to export.")
        logging.warning("No data to export.")

except Exception as e:
    logging.error(f"Fatal script error: {e}")
    print(f"Fatal script error: {e}")

finally:
    try:
        conn_ow.close()
    except Exception:
        pass
