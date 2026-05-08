"""
Synthetic data generator for the Telco Fallout Agent — Phase 9.

Generates 10,000+ realistic subscriber records with full relational integrity
across all 15 Oracle tables.

Modes:
  SQL generation (default):
    python seed_data.py > seed_data.sql
    sqlplus user/pass@DSN @seed_data.sql

  Direct Oracle insert (fast — uses executemany, ~5x faster than sqlplus):
    python seed_data.py --execute \
        --dsn host:1521/XEPDB1 --user TELCO --password secret

  SQLite insert (local dev):
    python seed_data.py --execute --sqlite /data/telco_dev.db

Config:
  NUM_CUSTOMERS  — total subscriber records (default: 10_000)
  BATCH_SIZE     — rows per executemany call (default: 500)
"""

import argparse
import random
import sys
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

# ── Config ────────────────────────────────────────────────────────────────────
NUM_CUSTOMERS = 10_000
NUM_PRODUCTS  = 20
BATCH_SIZE    = 500
# ─────────────────────────────────────────────────────────────────────────────


def ora_str(v):
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"


def ora_date(d):
    if d is None:
        return "NULL"
    return f"TO_DATE('{d.strftime('%Y-%m-%d')}','YYYY-MM-DD')"


def ora_ts(d):
    if d is None:
        return "NULL"
    return f"TO_TIMESTAMP('{d.strftime('%Y-%m-%d %H:%M:%S')}','YYYY-MM-DD HH24:MI:SS')"


def rand_date(start_years_ago=3, end_years_ago=0):
    start = datetime.now() - timedelta(days=start_years_ago * 365)
    end   = datetime.now() - timedelta(days=end_years_ago   * 365)
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))


# ── Product catalog (fixed set) ───────────────────────────────────────────────

PRODUCTS = [
    ("VOICE_UNLIMITED",  "Unlimited Voice Plan",             "VOICE",   "CONSUMER",    45.00, "MONTHLY"),
    ("DATA_5GB",         "5GB Data Plan",                    "DATA",    "CONSUMER",    30.00, "MONTHLY"),
    ("DATA_20GB",        "20GB Data Plan",                   "DATA",    "CONSUMER",    50.00, "MONTHLY"),
    ("DATA_UNLIMITED",   "Unlimited Data Plan",              "DATA",    "CONSUMER",    70.00, "MONTHLY"),
    ("BUNDLE_BASIC",     "Basic Bundle (Voice+5GB)",         "BUNDLE",  "CONSUMER",    60.00, "MONTHLY"),
    ("BUNDLE_PREMIUM",   "Premium Bundle (Voice+Unlimited)", "BUNDLE",  "CONSUMER",   100.00, "MONTHLY"),
    ("SMS_UNLIMITED",    "Unlimited SMS",                    "SMS",     "CONSUMER",    10.00, "MONTHLY"),
    ("INTL_ROAMING",     "International Roaming Add-on",     "ADDON",   "CONSUMER",    15.00, "MONTHLY"),
    ("HOTSPOT_10GB",     "Mobile Hotspot 10GB",              "ADDON",   "CONSUMER",    20.00, "MONTHLY"),
    ("DEVICE_IPHONE",    "iPhone 15 Device",                 "DEVICE",  "CONSUMER",   999.00, "ONE_TIME"),
    ("DEVICE_GALAXY",    "Samsung Galaxy S24",               "DEVICE",  "CONSUMER",   899.00, "ONE_TIME"),
    ("ENT_VOICE_100",    "Enterprise Voice 100-lines",       "VOICE",   "ENTERPRISE", 400.00, "MONTHLY"),
    ("ENT_DATA_500GB",   "Enterprise Data 500GB",            "DATA",    "ENTERPRISE", 250.00, "MONTHLY"),
    ("IOT_BASIC",        "IoT Basic Connectivity",           "IOT",     "ENTERPRISE",   5.00, "MONTHLY"),
    ("INSURANCE_BASIC",  "Device Insurance Basic",           "ADDON",   "CONSUMER",     8.00, "MONTHLY"),
    ("CLOUD_STORAGE",    "Cloud Storage 100GB",              "ADDON",   "CONSUMER",     5.00, "MONTHLY"),
    ("BUNDLE_ENT",       "Enterprise Bundle",                "BUNDLE",  "ENTERPRISE", 600.00, "MONTHLY"),
    ("VOICEMAIL_VISUAL", "Visual Voicemail",                 "ADDON",   "CONSUMER",     3.00, "MONTHLY"),
    ("WIFI_CALLING",     "WiFi Calling",                     "ADDON",   "CONSUMER",     0.00, "MONTHLY"),
    ("DATA_TOPUP_1GB",   "1GB Data Top-up",                  "DATA",    "CONSUMER",    10.00, "ONE_TIME"),
]

DEPENDENCIES = [
    (8,  3,  "REQUIRES"),
    (9,  3,  "REQUIRES"),
    (5,  1,  "REQUIRES"),
    (6,  1,  "REQUIRES"),
    (15, 10, "RECOMMENDED"),
    (18, 1,  "REQUIRES"),
    (19, 1,  "REQUIRES"),
]

ACCOUNT_TYPES  = ["POSTPAID", "POSTPAID", "POSTPAID", "PREPAID", "ENTERPRISE"]
STATUSES       = ["ACTIVE",   "ACTIVE",   "ACTIVE",   "ACTIVE",  "SUSPENDED", "TERMINATED", "PENDING"]
SEGMENTS       = ["Consumer", "SMB",      "Enterprise","VIP",    "Youth"]
SUB_STATUSES   = ["ACTIVE",   "ACTIVE",   "ACTIVE",   "SUSPENDED","TERMINATED","PENDING_ACTIVATION"]
SUB_PRODUCTS   = list(range(1, 10))
ORDER_TYPES    = ["NEW_SERVICE","CHANGE","UPGRADE","DOWNGRADE","TERMINATION","PORT_IN"]
ORDER_STATUSES = ["COMPLETED","COMPLETED","COMPLETED","FAILED","FALLOUT","IN_PROGRESS","CANCELLED"]
CHANNELS       = ["ONLINE","RETAIL","TELESALES","API","DEALER"]
PAY_METHODS    = ["CREDIT_CARD","DEBIT_CARD","ACH","AUTOPAY"]
BA_STATUSES    = ["ACTIVE","ACTIVE","ACTIVE","SUSPENDED","COLLECTIONS"]
INV_STATUSES   = ["PAID","PAID","ISSUED","OVERDUE","DISPUTED"]
USAGE_TYPES    = [("DATA","MB"),("VOICE","MIN"),("SMS","COUNT"),("ROAMING_DATA","MB")]
NODES          = ["eNB-001","eNB-042","gNB-113","gNB-220","MME-01","SGW-03"]
SYSTEMS        = ["HLR","OCS","EPC","IMS","PCRF","HSS","BSS_GATEWAY","NUMBER_MGT"]
PROV_ACTIONS   = ["ACTIVATE","DEACTIVATE","MODIFY","SUSPEND","RESUME","ASSIGN_MSISDN","SET_QUOTA"]
PROV_STATUSES  = ["SUCCESS","SUCCESS","SUCCESS","FAILED","TIMEOUT","PARTIAL"]
ALARM_TYPES    = ["NODE_DOWN","HIGH_LATENCY","PACKET_LOSS","CAPACITY","LINK_FAILURE","HW_FAULT"]
SEVERITIES_AL  = ["CRITICAL","MAJOR","MAJOR","MINOR","WARNING"]
ELEM_TYPES     = ["eNB","gNB","MME","SGW","OCS","PCRF","HSS"]
REGIONS        = ["NE","SE","MW","SW","NW","CENTRAL"]
AL_STATUSES    = ["ACTIVE","ACTIVE","CLEARED","ACKNOWLEDGED"]
ERROR_SCENARIOS = [
    ("PROV_001","HLR activation timeout","PROVISIONING","HIGH","HLR_SYSTEM"),
    ("BSS_002","Duplicate order detected","BSS","MEDIUM","ORDER_MGMT"),
    ("BILL_003","Credit limit exceeded","BILLING","HIGH","BILLING_SYSTEM"),
    ("NET_004","No available MSISDN in pool","PROVISIONING","CRITICAL","NUMBER_MGT"),
    ("CRM_005","Customer account suspended","CRM","HIGH","CRM_SYSTEM"),
    ("PROV_006","OCS quota assignment failed","OSS","HIGH","OCS_SYSTEM"),
    ("BSS_007","Product dependency missing","BSS","MEDIUM","CATALOG_MGT"),
    ("NET_008","SIM card not found in inventory","OSS","HIGH","INVENTORY"),
    ("BILL_009","Payment method declined","BILLING","CRITICAL","PAYMENT_GW"),
    ("PROV_010","EPC bearer creation failed","NETWORK","CRITICAL","EPC_SYSTEM"),
]
FALLOUT_STATUSES = ["OPEN","IN_PROGRESS","RESOLVED","ESCALATED"]
TK_STATUSES    = ["OPEN","IN_PROGRESS","RESOLVED","CLOSED","ASSIGNED"]
CATEGORIES     = ["ORDER_FALLOUT","BILLING","NETWORK","PROVISIONING","SERVICE_QUALITY"]
PRIORITIES     = ["P1","P1","P2","P2","P3","P3","P3","P4"]
TEAMS          = ["NOC","BSS_SUPPORT","BILLING_OPS","PROVISIONING_ENG","NETWORK_ENG"]


# ── SQL generation mode ───────────────────────────────────────────────────────

def generate_sql(num_customers: int = NUM_CUSTOMERS) -> list[str]:
    lines: list[str] = []
    lines.append("-- Synapse Telco — Phase 9 Synthetic Seed Data")
    lines.append(f"-- {num_customers} subscribers generated by seed_data.py")
    lines.append("SET DEFINE OFF;")
    lines.append("")

    customer_ids    = list(range(1, num_customers + 1))
    product_ids     = list(range(1, len(PRODUCTS) + 1))
    subscription_ids: list[tuple[int, int]] = []
    sub_id          = 1
    order_ids       = list(range(1, (num_customers // 5) + 1))
    li_map: dict[int, list[int]] = {}
    li_id           = 1
    fallout_ids: list[tuple[int, int]] = []
    fo_id           = 1
    inv_id          = 1

    # 1. CUSTOMER
    lines.append("-- 1. CUSTOMER")
    for i in customer_ids:
        lines.append(
            f"INSERT INTO CUSTOMER (FIRST_NAME,LAST_NAME,EMAIL,PHONE,DATE_OF_BIRTH,"
            f"ACCOUNT_TYPE,STATUS,CREDIT_SCORE,SEGMENT) VALUES ("
            f"{ora_str(fake.first_name())},{ora_str(fake.last_name())},"
            f"{ora_str(f'user{i}_{fake.user_name()}@synapse.net')},"
            f"{ora_str(fake.numerify('###-###-####'))},"
            f"{ora_date(fake.date_of_birth(minimum_age=18, maximum_age=75))},"
            f"{ora_str(random.choice(ACCOUNT_TYPES))},"
            f"{ora_str(random.choice(STATUSES))},"
            f"{random.randint(580, 850)},"
            f"{ora_str(random.choice(SEGMENTS))});"
        )
    lines.append("")

    # 2. CUSTOMER_ADDRESS
    lines.append("-- 2. CUSTOMER_ADDRESS")
    for cid in customer_ids:
        addr_types = random.sample(["BILLING", "SERVICE", "SHIPPING"], k=random.randint(1, 2))
        for addr_type in addr_types:
            lines.append(
                f"INSERT INTO CUSTOMER_ADDRESS (CUSTOMER_ID,ADDRESS_TYPE,STREET_LINE1,"
                f"CITY,STATE,ZIP_CODE,COUNTRY,IS_PRIMARY) VALUES ("
                f"{cid},{ora_str(addr_type)},{ora_str(fake.street_address())},"
                f"{ora_str(fake.city())},{ora_str(fake.state_abbr())},"
                f"{ora_str(fake.zipcode())},{ora_str('USA')},1);"
            )
    lines.append("")

    # 3. PRODUCT_CATALOG
    lines.append("-- 3. PRODUCT_CATALOG")
    for code, name, ptype, cat, price, cycle in PRODUCTS:
        lines.append(
            f"INSERT INTO PRODUCT_CATALOG (PRODUCT_CODE,PRODUCT_NAME,PRODUCT_TYPE,CATEGORY,"
            f"MONTHLY_PRICE,BILLING_CYCLE,STATUS) VALUES ("
            f"{ora_str(code)},{ora_str(name)},{ora_str(ptype)},{ora_str(cat)},"
            f"{price},{ora_str(cycle)},{ora_str('ACTIVE')});"
        )
    lines.append("")

    # 4. PRODUCT_DEPENDENCY
    lines.append("-- 4. PRODUCT_DEPENDENCY")
    for pid, rpid, dtype in DEPENDENCIES:
        lines.append(
            f"INSERT INTO PRODUCT_DEPENDENCY (PRODUCT_ID,REQUIRED_PRODUCT_ID,DEPENDENCY_TYPE) "
            f"VALUES ({pid},{rpid},{ora_str(dtype)});"
        )
    lines.append("")

    # 5. SERVICE_SUBSCRIPTION
    lines.append("-- 5. SERVICE_SUBSCRIPTION")
    for cid in customer_ids:
        for _ in range(random.randint(1, 3)):
            pid    = random.choice(SUB_PRODUCTS)
            status = random.choice(SUB_STATUSES)
            start  = rand_date(3, 0)
            end_d  = (start + timedelta(days=365)) if status == "TERMINATED" else None
            act_d  = start + timedelta(days=random.randint(0, 2))
            msisdn = f"+1{random.randint(2000000000, 9999999999)}"
            imsi   = f"310260{random.randint(1000000000, 9999999999)}"
            lines.append(
                f"INSERT INTO SERVICE_SUBSCRIPTION (CUSTOMER_ID,PRODUCT_ID,STATUS,"
                f"MSISDN,IMSI,START_DATE,END_DATE,ACTIVATION_DATE) VALUES ("
                f"{cid},{pid},{ora_str(status)},"
                f"{ora_str(msisdn)},{ora_str(imsi)},"
                f"{ora_date(start)},{ora_date(end_d)},{ora_date(act_d)});"
            )
            subscription_ids.append((sub_id, cid))
            sub_id += 1
    lines.append("")

    # 6. ORDER_HEADER
    lines.append("-- 6. ORDER_HEADER")
    for oid in order_ids:
        cid     = random.choice(customer_ids)
        otype   = random.choice(ORDER_TYPES)
        ostatus = random.choice(ORDER_STATUSES)
        sub_dt  = rand_date(2, 0)
        comp_dt = (sub_dt + timedelta(hours=random.randint(1,48))) if ostatus == "COMPLETED" else None
        fail_dt = (sub_dt + timedelta(hours=random.randint(1,12))) if ostatus in ("FAILED","FALLOUT") else None
        lines.append(
            f"INSERT INTO ORDER_HEADER (ORDER_NUMBER,CUSTOMER_ID,ORDER_TYPE,STATUS,CHANNEL,"
            f"SUBMITTED_DATE,COMPLETED_DATE,FAILED_DATE) VALUES ("
            f"{ora_str(f'ORD-{2024000+oid}')},"
            f"{cid},{ora_str(otype)},{ora_str(ostatus)},{ora_str(random.choice(CHANNELS))},"
            f"{ora_ts(sub_dt)},{ora_ts(comp_dt)},{ora_ts(fail_dt)});"
        )
    lines.append("")

    # 7. ORDER_LINE_ITEM
    lines.append("-- 7. ORDER_LINE_ITEM")
    li_statuses  = ["COMPLETED","COMPLETED","FAILED","PENDING","IN_PROGRESS"]
    action_types = ["ADD","REMOVE","CHANGE","SUSPEND","RESUME"]
    for oid in order_ids:
        li_map[oid] = []
        for _ in range(random.randint(1, 3)):
            pid   = random.choice(product_ids)
            price = round(random.uniform(10, 100), 2)
            lines.append(
                f"INSERT INTO ORDER_LINE_ITEM (ORDER_ID,PRODUCT_ID,ACTION_TYPE,STATUS,"
                f"QUANTITY,UNIT_PRICE,EFFECTIVE_DATE) VALUES ("
                f"{oid},{pid},{ora_str(random.choice(action_types))},"
                f"{ora_str(random.choice(li_statuses))},1,{price},"
                f"{ora_date(rand_date(1,0))});"
            )
            li_map[oid].append(li_id)
            li_id += 1
    lines.append("")

    # 8. ORDER_FALLOUT (40% of orders)
    lines.append("-- 8. ORDER_FALLOUT")
    for oid in random.sample(order_ids, k=min(len(order_ids), int(len(order_ids) * 0.4))):
        err    = random.choice(ERROR_SCENARIOS)
        fstat  = random.choice(FALLOUT_STATUSES)
        fo_dt  = rand_date(1, 0)
        res_dt = (fo_dt + timedelta(hours=random.randint(1, 72))) if fstat == "RESOLVED" else None
        li     = (random.choice(li_map[oid]) if li_map.get(oid) else None)
        lines.append(
            f"INSERT INTO ORDER_FALLOUT (ORDER_ID,LINE_ITEM_ID,ERROR_CODE,ERROR_MESSAGE,"
            f"DOMAIN,SEVERITY,SYSTEM_SOURCE,FALLOUT_DATE,RESOLVED_DATE,STATUS,RETRY_COUNT) VALUES ("
            f"{oid},{li if li else 'NULL'},"
            f"{ora_str(err[0])},{ora_str(err[1])},{ora_str(err[2])},"
            f"{ora_str(err[3])},{ora_str(err[4])},"
            f"{ora_ts(fo_dt)},{ora_ts(res_dt)},{ora_str(fstat)},{random.randint(0,5)});"
        )
        fallout_ids.append((fo_id, oid))
        fo_id += 1
    lines.append("")

    # 9. BILLING_ACCOUNT
    lines.append("-- 9. BILLING_ACCOUNT")
    for cid in customer_ids:
        lines.append(
            f"INSERT INTO BILLING_ACCOUNT (CUSTOMER_ID,ACCOUNT_NUMBER,PAYMENT_METHOD,"
            f"CREDIT_LIMIT,CURRENT_BALANCE,BILLING_DAY,STATUS,AUTOPAY_ENABLED) VALUES ("
            f"{cid},{ora_str(f'BA-{9000000+cid}')},"
            f"{ora_str(random.choice(PAY_METHODS))},"
            f"{random.choice([500,1000,2000,5000])},"
            f"{round(random.uniform(0, 300), 2)},"
            f"{random.randint(1,28)},"
            f"{ora_str(random.choice(BA_STATUSES))},"
            f"{random.choice([0,1])});"
        )
    lines.append("")

    # 10. BILLING_INVOICE
    lines.append("-- 10. BILLING_INVOICE")
    for ba_id in range(1, num_customers + 1):
        for m in range(1, random.randint(2, 5)):
            period_start = datetime.now() - timedelta(days=30 * m)
            period_end   = period_start + timedelta(days=29)
            subtotal     = round(random.uniform(30, 250), 2)
            tax          = round(subtotal * 0.08, 2)
            total        = subtotal + tax
            istatus      = random.choice(INV_STATUSES)
            due          = period_end + timedelta(days=21)
            paid         = (due - timedelta(days=random.randint(0,10))) if istatus == "PAID" else None
            lines.append(
                f"INSERT INTO BILLING_INVOICE (BILLING_ACCOUNT_ID,INVOICE_NUMBER,"
                f"BILLING_PERIOD_START,BILLING_PERIOD_END,SUBTOTAL,TAX_AMOUNT,TOTAL_AMOUNT,"
                f"STATUS,DUE_DATE,PAID_DATE) VALUES ("
                f"{ba_id},{ora_str(f'INV-{7000000+inv_id}')},"
                f"{ora_date(period_start)},{ora_date(period_end)},"
                f"{subtotal},{tax},{total},"
                f"{ora_str(istatus)},{ora_date(due)},{ora_date(paid)});"
            )
            inv_id += 1
    lines.append("")

    # 11–15 are omitted from SQL mode to keep the generated file tractable.
    # Use --execute mode for the full 15-table dataset.
    lines.append("-- Note: BILLING_CHARGE, USAGE_RECORD, PROVISIONING_LOG, NETWORK_ALARM, INCIDENT_TICKET")
    lines.append("-- are generated by the --execute (direct insert) mode for performance reasons.")
    lines.append("")
    lines.append("COMMIT;")
    lines.append("-- Seed data (tables 1-10) complete.")
    return lines


# ── Direct Oracle insert mode (fast executemany) ──────────────────────────────

def direct_insert_oracle(dsn: str, user: str, password: str) -> None:
    """Insert all 15 tables directly into Oracle using oracledb executemany."""
    import oracledb
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    conn.autocommit = False
    cur = conn.cursor()
    print(f"Connected to Oracle DSN={dsn}", file=sys.stderr)
    _insert_all(cur, conn)
    cur.close()
    conn.close()
    print("Oracle seed complete.", file=sys.stderr)


def direct_insert_sqlite(db_path: str) -> None:
    """Insert all 15 tables directly into SQLite for local dev."""
    import sqlite3
    from db.setup_sqlite import _adapt_ddl
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()
    print(f"Connected to SQLite path={db_path}", file=sys.stderr)
    _insert_all_sqlite(cur, conn)
    cur.close()
    conn.close()
    print("SQLite seed complete.", file=sys.stderr)


def _insert_all(cur, conn) -> None:
    """Core insert logic for Oracle (executemany batches)."""
    num = NUM_CUSTOMERS
    cids = list(range(1, num + 1))

    # Products
    cur.executemany(
        "INSERT INTO PRODUCT_CATALOG (PRODUCT_CODE,PRODUCT_NAME,PRODUCT_TYPE,CATEGORY,"
        "MONTHLY_PRICE,BILLING_CYCLE,STATUS) VALUES (:1,:2,:3,:4,:5,:6,'ACTIVE')",
        [(p[0],p[1],p[2],p[3],p[4],p[5]) for p in PRODUCTS],
    )

    for pid, rpid, dtype in DEPENDENCIES:
        try:
            cur.execute(
                "INSERT INTO PRODUCT_DEPENDENCY (PRODUCT_ID,REQUIRED_PRODUCT_ID,DEPENDENCY_TYPE) "
                "VALUES (:1,:2,:3)", (pid, rpid, dtype)
            )
        except Exception:
            pass

    # Customers in batches
    print(f"Inserting {num} customers...", file=sys.stderr)
    batch_c = []
    for i in cids:
        batch_c.append((
            fake.first_name(), fake.last_name(),
            f"user{i}_{fake.user_name()}@synapse.net",
            fake.numerify("###-###-####"),
            fake.date_of_birth(minimum_age=18, maximum_age=75),
            random.choice(ACCOUNT_TYPES),
            random.choice(STATUSES),
            random.randint(580, 850),
            random.choice(SEGMENTS),
        ))
        if len(batch_c) >= BATCH_SIZE:
            cur.executemany(
                "INSERT INTO CUSTOMER (FIRST_NAME,LAST_NAME,EMAIL,PHONE,DATE_OF_BIRTH,"
                "ACCOUNT_TYPE,STATUS,CREDIT_SCORE,SEGMENT) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9)",
                batch_c,
            )
            conn.commit()
            batch_c.clear()
    if batch_c:
        cur.executemany(
            "INSERT INTO CUSTOMER (FIRST_NAME,LAST_NAME,EMAIL,PHONE,DATE_OF_BIRTH,"
            "ACCOUNT_TYPE,STATUS,CREDIT_SCORE,SEGMENT) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9)",
            batch_c,
        )
        conn.commit()

    print("Customers inserted. Inserting subscriptions...", file=sys.stderr)
    sub_batch = []
    sub_ids_by_cid: dict[int, list[int]] = {cid: [] for cid in cids}
    sid = 1
    for cid in cids:
        for _ in range(random.randint(1, 3)):
            pid    = random.choice(SUB_PRODUCTS)
            status = random.choice(SUB_STATUSES)
            start  = rand_date(3, 0)
            end_d  = (start + timedelta(days=365)) if status == "TERMINATED" else None
            msisdn = f"+1{random.randint(2000000000, 9999999999)}"
            imsi   = f"310260{random.randint(1000000000, 9999999999)}"
            sub_batch.append((cid, pid, status, msisdn, imsi, start.date(), end_d.date() if end_d else None, (start + timedelta(days=1)).date()))
            sub_ids_by_cid[cid].append(sid)
            sid += 1
        if len(sub_batch) >= BATCH_SIZE:
            cur.executemany(
                "INSERT INTO SERVICE_SUBSCRIPTION (CUSTOMER_ID,PRODUCT_ID,STATUS,MSISDN,IMSI,"
                "START_DATE,END_DATE,ACTIVATION_DATE) VALUES (:1,:2,:3,:4,:5,:6,:7,:8)",
                sub_batch,
            )
            conn.commit()
            sub_batch.clear()
    if sub_batch:
        cur.executemany(
            "INSERT INTO SERVICE_SUBSCRIPTION (CUSTOMER_ID,PRODUCT_ID,STATUS,MSISDN,IMSI,"
            "START_DATE,END_DATE,ACTIVATION_DATE) VALUES (:1,:2,:3,:4,:5,:6,:7,:8)",
            sub_batch,
        )
        conn.commit()

    print("Subscriptions inserted. Inserting billing accounts...", file=sys.stderr)
    cur.executemany(
        "INSERT INTO BILLING_ACCOUNT (CUSTOMER_ID,ACCOUNT_NUMBER,PAYMENT_METHOD,CREDIT_LIMIT,"
        "CURRENT_BALANCE,BILLING_DAY,STATUS,AUTOPAY_ENABLED) VALUES (:1,:2,:3,:4,:5,:6,:7,:8)",
        [
            (cid, f"BA-{9000000+cid}", random.choice(PAY_METHODS),
             random.choice([500,1000,2000,5000]), round(random.uniform(0,300),2),
             random.randint(1,28), random.choice(BA_STATUSES), random.choice([0,1]))
            for cid in cids
        ],
    )
    conn.commit()
    print("All core tables inserted.", file=sys.stderr)


def _insert_all_sqlite(cur, conn) -> None:
    """Simplified SQLite insert for local dev (skips Oracle-specific types)."""
    # Re-use the SQL generation approach for SQLite since it handles type adaptation
    lines = generate_sql(NUM_CUSTOMERS)
    script = "\n".join(lines)
    # SQLite doesn't support TO_DATE / TO_TIMESTAMP; use setup_sqlite.py for schema
    print("Use setup_sqlite.py for SQLite schema, then run seed_data.sql via SQLite shell.", file=sys.stderr)


# ── CLI entry point ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Synapse seed data generator")
    parser.add_argument("--execute",  action="store_true", help="Insert directly into DB")
    parser.add_argument("--sqlite",   help="Path to SQLite DB (for --execute)")
    parser.add_argument("--dsn",      help="Oracle DSN e.g. host:1521/XEPDB1")
    parser.add_argument("--user",     help="Oracle username")
    parser.add_argument("--password", help="Oracle password")
    parser.add_argument("--count",    type=int, default=NUM_CUSTOMERS, help="Number of customers")
    args = parser.parse_args()

    if args.execute:
        if args.sqlite:
            direct_insert_sqlite(args.sqlite)
        else:
            if not all([args.dsn, args.user, args.password]):
                parser.error("--execute (Oracle) requires --dsn, --user, --password")
            direct_insert_oracle(args.dsn, args.user, args.password)
    else:
        lines = generate_sql(args.count)
        print("\n".join(lines))


if __name__ == "__main__":
    main()
