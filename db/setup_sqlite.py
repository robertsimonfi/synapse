"""
SQLite setup script for local development.
Creates all 15 tables and seeds them with realistic Telco data.

Usage:
    python db/setup_sqlite.py
"""

import sqlite3
import random
import os
from datetime import datetime, timedelta

try:
    from faker import Faker
except ImportError:
    print("Run: pip install faker")
    raise

from dotenv import load_dotenv
load_dotenv()

DB_PATH = os.getenv("SQLITE_PATH", "telco_dev.db")
fake = Faker()
random.seed(42)
Faker.seed(42)

# ── DDL ───────────────────────────────────────────────────────────────────────

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS CUSTOMER (
  CUSTOMER_ID   INTEGER PRIMARY KEY AUTOINCREMENT,
  FIRST_NAME    TEXT NOT NULL,
  LAST_NAME     TEXT NOT NULL,
  EMAIL         TEXT NOT NULL UNIQUE,
  PHONE         TEXT,
  DATE_OF_BIRTH TEXT,
  ACCOUNT_TYPE  TEXT DEFAULT 'POSTPAID' CHECK(ACCOUNT_TYPE IN ('POSTPAID','PREPAID','ENTERPRISE')),
  STATUS        TEXT DEFAULT 'ACTIVE' CHECK(STATUS IN ('ACTIVE','SUSPENDED','TERMINATED','PENDING')),
  CREDIT_SCORE  INTEGER,
  SEGMENT       TEXT,
  CREATED_DATE  TEXT DEFAULT (datetime('now')),
  UPDATED_DATE  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS CUSTOMER_ADDRESS (
  ADDRESS_ID    INTEGER PRIMARY KEY AUTOINCREMENT,
  CUSTOMER_ID   INTEGER NOT NULL REFERENCES CUSTOMER(CUSTOMER_ID) ON DELETE CASCADE,
  ADDRESS_TYPE  TEXT DEFAULT 'BILLING',
  STREET_LINE1  TEXT NOT NULL,
  STREET_LINE2  TEXT,
  CITY          TEXT NOT NULL,
  STATE         TEXT NOT NULL,
  ZIP_CODE      TEXT NOT NULL,
  COUNTRY       TEXT DEFAULT 'USA',
  IS_PRIMARY    INTEGER DEFAULT 1,
  CREATED_DATE  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS PRODUCT_CATALOG (
  PRODUCT_ID    INTEGER PRIMARY KEY AUTOINCREMENT,
  PRODUCT_CODE  TEXT NOT NULL UNIQUE,
  PRODUCT_NAME  TEXT NOT NULL,
  PRODUCT_TYPE  TEXT,
  CATEGORY      TEXT,
  MONTHLY_PRICE REAL DEFAULT 0,
  SETUP_FEE     REAL DEFAULT 0,
  BILLING_CYCLE TEXT DEFAULT 'MONTHLY',
  STATUS        TEXT DEFAULT 'ACTIVE',
  DESCRIPTION   TEXT,
  CREATED_DATE  TEXT DEFAULT (datetime('now')),
  UPDATED_DATE  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS PRODUCT_DEPENDENCY (
  DEPENDENCY_ID       INTEGER PRIMARY KEY AUTOINCREMENT,
  PRODUCT_ID          INTEGER NOT NULL REFERENCES PRODUCT_CATALOG(PRODUCT_ID),
  REQUIRED_PRODUCT_ID INTEGER NOT NULL REFERENCES PRODUCT_CATALOG(PRODUCT_ID),
  DEPENDENCY_TYPE     TEXT,
  CREATED_DATE        TEXT DEFAULT (datetime('now')),
  UNIQUE(PRODUCT_ID, REQUIRED_PRODUCT_ID)
);

CREATE TABLE IF NOT EXISTS SERVICE_SUBSCRIPTION (
  SUBSCRIPTION_ID INTEGER PRIMARY KEY AUTOINCREMENT,
  CUSTOMER_ID     INTEGER NOT NULL REFERENCES CUSTOMER(CUSTOMER_ID),
  PRODUCT_ID      INTEGER NOT NULL REFERENCES PRODUCT_CATALOG(PRODUCT_ID),
  STATUS          TEXT DEFAULT 'ACTIVE',
  MSISDN          TEXT UNIQUE,
  IMSI            TEXT UNIQUE,
  ICC_ID          TEXT,
  START_DATE      TEXT NOT NULL,
  END_DATE        TEXT,
  ACTIVATION_DATE TEXT,
  SUSPEND_REASON  TEXT,
  CREATED_DATE    TEXT DEFAULT (datetime('now')),
  UPDATED_DATE    TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ORDER_HEADER (
  ORDER_ID        INTEGER PRIMARY KEY AUTOINCREMENT,
  ORDER_NUMBER    TEXT NOT NULL UNIQUE,
  CUSTOMER_ID     INTEGER NOT NULL REFERENCES CUSTOMER(CUSTOMER_ID),
  ORDER_TYPE      TEXT,
  STATUS          TEXT DEFAULT 'PENDING',
  CHANNEL         TEXT,
  PRIORITY        TEXT DEFAULT 'NORMAL',
  SUBMITTED_DATE  TEXT DEFAULT (datetime('now')),
  COMPLETED_DATE  TEXT,
  FAILED_DATE     TEXT,
  NOTES           TEXT,
  CREATED_DATE    TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ORDER_LINE_ITEM (
  LINE_ITEM_ID    INTEGER PRIMARY KEY AUTOINCREMENT,
  ORDER_ID        INTEGER NOT NULL REFERENCES ORDER_HEADER(ORDER_ID) ON DELETE CASCADE,
  PRODUCT_ID      INTEGER NOT NULL REFERENCES PRODUCT_CATALOG(PRODUCT_ID),
  SUBSCRIPTION_ID INTEGER REFERENCES SERVICE_SUBSCRIPTION(SUBSCRIPTION_ID),
  ACTION_TYPE     TEXT,
  STATUS          TEXT DEFAULT 'PENDING',
  QUANTITY        INTEGER DEFAULT 1,
  UNIT_PRICE      REAL,
  DISCOUNT_PCT    REAL DEFAULT 0,
  EFFECTIVE_DATE  TEXT,
  CREATED_DATE    TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ORDER_FALLOUT (
  FALLOUT_ID      INTEGER PRIMARY KEY AUTOINCREMENT,
  ORDER_ID        INTEGER NOT NULL REFERENCES ORDER_HEADER(ORDER_ID),
  LINE_ITEM_ID    INTEGER REFERENCES ORDER_LINE_ITEM(LINE_ITEM_ID),
  ERROR_CODE      TEXT NOT NULL,
  ERROR_MESSAGE   TEXT NOT NULL,
  DOMAIN          TEXT,
  SEVERITY        TEXT DEFAULT 'MEDIUM',
  SYSTEM_SOURCE   TEXT,
  STACK_TRACE     TEXT,
  FALLOUT_DATE    TEXT DEFAULT (datetime('now')),
  RESOLVED_DATE   TEXT,
  STATUS          TEXT DEFAULT 'OPEN',
  RETRY_COUNT     INTEGER DEFAULT 0,
  CREATED_DATE    TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS BILLING_ACCOUNT (
  BILLING_ACCOUNT_ID INTEGER PRIMARY KEY AUTOINCREMENT,
  CUSTOMER_ID        INTEGER NOT NULL UNIQUE REFERENCES CUSTOMER(CUSTOMER_ID),
  ACCOUNT_NUMBER     TEXT NOT NULL UNIQUE,
  PAYMENT_METHOD     TEXT,
  CREDIT_LIMIT       REAL DEFAULT 500,
  CURRENT_BALANCE    REAL DEFAULT 0,
  BILLING_DAY        INTEGER DEFAULT 1,
  STATUS             TEXT DEFAULT 'ACTIVE',
  AUTOPAY_ENABLED    INTEGER DEFAULT 0,
  CREATED_DATE       TEXT DEFAULT (datetime('now')),
  UPDATED_DATE       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS BILLING_INVOICE (
  INVOICE_ID           INTEGER PRIMARY KEY AUTOINCREMENT,
  BILLING_ACCOUNT_ID   INTEGER NOT NULL REFERENCES BILLING_ACCOUNT(BILLING_ACCOUNT_ID),
  INVOICE_NUMBER       TEXT NOT NULL UNIQUE,
  BILLING_PERIOD_START TEXT NOT NULL,
  BILLING_PERIOD_END   TEXT NOT NULL,
  SUBTOTAL             REAL DEFAULT 0,
  TAX_AMOUNT           REAL DEFAULT 0,
  TOTAL_AMOUNT         REAL DEFAULT 0,
  STATUS               TEXT DEFAULT 'DRAFT',
  DUE_DATE             TEXT,
  PAID_DATE            TEXT,
  PAYMENT_REFERENCE    TEXT,
  CREATED_DATE         TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS BILLING_CHARGE (
  CHARGE_ID       INTEGER PRIMARY KEY AUTOINCREMENT,
  INVOICE_ID      INTEGER NOT NULL REFERENCES BILLING_INVOICE(INVOICE_ID) ON DELETE CASCADE,
  SUBSCRIPTION_ID INTEGER REFERENCES SERVICE_SUBSCRIPTION(SUBSCRIPTION_ID),
  CHARGE_TYPE     TEXT,
  DESCRIPTION     TEXT,
  AMOUNT          REAL NOT NULL,
  TAX_AMOUNT      REAL DEFAULT 0,
  CHARGE_DATE     TEXT NOT NULL,
  PERIOD_START    TEXT,
  PERIOD_END      TEXT,
  CREATED_DATE    TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS USAGE_RECORD (
  USAGE_ID        INTEGER PRIMARY KEY AUTOINCREMENT,
  SUBSCRIPTION_ID INTEGER NOT NULL REFERENCES SERVICE_SUBSCRIPTION(SUBSCRIPTION_ID),
  USAGE_TYPE      TEXT,
  QUANTITY        REAL NOT NULL,
  UNIT            TEXT,
  START_TIME      TEXT NOT NULL,
  END_TIME        TEXT,
  RATED_AMOUNT    REAL DEFAULT 0,
  IS_RATED        INTEGER DEFAULT 0,
  NETWORK_NODE    TEXT,
  CELL_ID         TEXT,
  ROAMING_COUNTRY TEXT,
  CREATED_DATE    TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS PROVISIONING_LOG (
  LOG_ID            INTEGER PRIMARY KEY AUTOINCREMENT,
  ORDER_ID          INTEGER REFERENCES ORDER_HEADER(ORDER_ID),
  SUBSCRIPTION_ID   INTEGER REFERENCES SERVICE_SUBSCRIPTION(SUBSCRIPTION_ID),
  SYSTEM_NAME       TEXT NOT NULL,
  ACTION            TEXT NOT NULL,
  REQUEST_PAYLOAD   TEXT,
  RESPONSE_PAYLOAD  TEXT,
  STATUS            TEXT DEFAULT 'PENDING',
  RESPONSE_CODE     TEXT,
  ERROR_DETAIL      TEXT,
  ATTEMPT_NUMBER    INTEGER DEFAULT 1,
  DURATION_MS       INTEGER,
  CREATED_DATE      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS NETWORK_ALARM (
  ALARM_ID        INTEGER PRIMARY KEY AUTOINCREMENT,
  ALARM_CODE      TEXT NOT NULL,
  ALARM_TYPE      TEXT,
  SEVERITY        TEXT DEFAULT 'MAJOR',
  NETWORK_ELEMENT TEXT NOT NULL,
  ELEMENT_TYPE    TEXT,
  REGION          TEXT,
  LATITUDE        REAL,
  LONGITUDE       REAL,
  DESCRIPTION     TEXT,
  RAISED_TIME     TEXT DEFAULT (datetime('now')),
  CLEARED_TIME    TEXT,
  STATUS          TEXT DEFAULT 'ACTIVE',
  AFFECTED_SUBS   INTEGER DEFAULT 0,
  CREATED_DATE    TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS INCIDENT_TICKET (
  TICKET_ID        INTEGER PRIMARY KEY AUTOINCREMENT,
  TICKET_NUMBER    TEXT NOT NULL UNIQUE,
  CUSTOMER_ID      INTEGER REFERENCES CUSTOMER(CUSTOMER_ID),
  ORDER_ID         INTEGER REFERENCES ORDER_HEADER(ORDER_ID),
  FALLOUT_ID       INTEGER REFERENCES ORDER_FALLOUT(FALLOUT_ID),
  ALARM_ID         INTEGER REFERENCES NETWORK_ALARM(ALARM_ID),
  TITLE            TEXT NOT NULL,
  DESCRIPTION      TEXT,
  CATEGORY         TEXT,
  PRIORITY         TEXT DEFAULT 'P3',
  STATUS           TEXT DEFAULT 'OPEN',
  ASSIGNED_TEAM    TEXT,
  ASSIGNED_TO      TEXT,
  ROOT_CAUSE       TEXT,
  RESOLUTION_NOTES TEXT,
  SLA_BREACH       INTEGER DEFAULT 0,
  CREATED_DATE     TEXT DEFAULT (datetime('now')),
  UPDATED_DATE     TEXT DEFAULT (datetime('now')),
  RESOLVED_DATE    TEXT
);
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def rand_dt(days_ago_max=365, days_ago_min=0):
    delta = random.randint(days_ago_min * 86400, days_ago_max * 86400)
    return (datetime.now() - timedelta(seconds=delta)).strftime("%Y-%m-%d %H:%M:%S")

def rand_date(days_ago_max=365, days_ago_min=0):
    delta = random.randint(days_ago_min * 86400, days_ago_max * 86400)
    return (datetime.now() - timedelta(seconds=delta)).strftime("%Y-%m-%d")

# ── Seed ──────────────────────────────────────────────────────────────────────

def seed(conn):
    cur = conn.cursor()

    print("  Seeding CUSTOMER...")
    account_types = ["POSTPAID","POSTPAID","POSTPAID","PREPAID","ENTERPRISE"]
    statuses      = ["ACTIVE","ACTIVE","ACTIVE","ACTIVE","SUSPENDED","TERMINATED","PENDING"]
    segments      = ["Consumer","SMB","Enterprise","VIP","Youth"]
    NUM_CUSTOMERS = 50
    for i in range(1, NUM_CUSTOMERS + 1):
        cur.execute("""INSERT INTO CUSTOMER
            (FIRST_NAME,LAST_NAME,EMAIL,PHONE,DATE_OF_BIRTH,ACCOUNT_TYPE,STATUS,CREDIT_SCORE,SEGMENT)
            VALUES (?,?,?,?,?,?,?,?,?)""", (
            fake.first_name(), fake.last_name(),
            f"user{i}_{fake.user_name()}@example.com",
            fake.numerify("###-###-####"),
            str(fake.date_of_birth(minimum_age=18, maximum_age=75)),
            random.choice(account_types),
            random.choice(statuses),
            random.randint(580, 850),
            random.choice(segments),
        ))

    print("  Seeding CUSTOMER_ADDRESS...")
    for cid in range(1, NUM_CUSTOMERS + 1):
        for atype in random.sample(["BILLING","SERVICE","SHIPPING"], k=random.randint(1,2)):
            cur.execute("""INSERT INTO CUSTOMER_ADDRESS
                (CUSTOMER_ID,ADDRESS_TYPE,STREET_LINE1,CITY,STATE,ZIP_CODE,COUNTRY,IS_PRIMARY)
                VALUES (?,?,?,?,?,?,?,?)""", (
                cid, atype, fake.street_address(),
                fake.city(), fake.state_abbr(), fake.zipcode(), "USA", 1,
            ))

    print("  Seeding PRODUCT_CATALOG...")
    products = [
        ("VOICE_UNLIMITED","Unlimited Voice Plan","VOICE","CONSUMER",45.00,"MONTHLY"),
        ("DATA_5GB","5GB Data Plan","DATA","CONSUMER",30.00,"MONTHLY"),
        ("DATA_20GB","20GB Data Plan","DATA","CONSUMER",50.00,"MONTHLY"),
        ("DATA_UNLIMITED","Unlimited Data Plan","DATA","CONSUMER",70.00,"MONTHLY"),
        ("BUNDLE_BASIC","Basic Bundle (Voice+5GB)","BUNDLE","CONSUMER",60.00,"MONTHLY"),
        ("BUNDLE_PREMIUM","Premium Bundle (Voice+Unlimited)","BUNDLE","CONSUMER",100.00,"MONTHLY"),
        ("SMS_UNLIMITED","Unlimited SMS","SMS","CONSUMER",10.00,"MONTHLY"),
        ("INTL_ROAMING","International Roaming Add-on","ADDON","CONSUMER",15.00,"MONTHLY"),
        ("HOTSPOT_10GB","Mobile Hotspot 10GB","ADDON","CONSUMER",20.00,"MONTHLY"),
        ("DEVICE_IPHONE","iPhone 15 Device","DEVICE","CONSUMER",999.00,"ONE_TIME"),
        ("DEVICE_GALAXY","Samsung Galaxy S24","DEVICE","CONSUMER",899.00,"ONE_TIME"),
        ("ENT_VOICE_100","Enterprise Voice 100-lines","VOICE","ENTERPRISE",400.00,"MONTHLY"),
        ("ENT_DATA_500GB","Enterprise Data 500GB","DATA","ENTERPRISE",250.00,"MONTHLY"),
        ("IOT_BASIC","IoT Basic Connectivity","IOT","ENTERPRISE",5.00,"MONTHLY"),
        ("INSURANCE_BASIC","Device Insurance Basic","ADDON","CONSUMER",8.00,"MONTHLY"),
        ("CLOUD_STORAGE","Cloud Storage 100GB","ADDON","CONSUMER",5.00,"MONTHLY"),
        ("BUNDLE_ENT","Enterprise Bundle","BUNDLE","ENTERPRISE",600.00,"MONTHLY"),
        ("VOICEMAIL_VISUAL","Visual Voicemail","ADDON","CONSUMER",3.00,"MONTHLY"),
        ("WIFI_CALLING","WiFi Calling","ADDON","CONSUMER",0.00,"MONTHLY"),
        ("DATA_TOPUP_1GB","1GB Data Top-up","DATA","CONSUMER",10.00,"ONE_TIME"),
    ]
    for code, name, ptype, cat, price, cycle in products:
        cur.execute("""INSERT INTO PRODUCT_CATALOG
            (PRODUCT_CODE,PRODUCT_NAME,PRODUCT_TYPE,CATEGORY,MONTHLY_PRICE,BILLING_CYCLE,STATUS)
            VALUES (?,?,?,?,?,?,?)""", (code, name, ptype, cat, price, cycle, "ACTIVE"))

    print("  Seeding PRODUCT_DEPENDENCY...")
    deps = [(8,3,"REQUIRES"),(9,3,"REQUIRES"),(5,1,"REQUIRES"),(6,1,"REQUIRES"),
            (15,10,"RECOMMENDED"),(18,1,"REQUIRES"),(19,1,"REQUIRES")]
    for pid, rpid, dtype in deps:
        cur.execute("INSERT INTO PRODUCT_DEPENDENCY (PRODUCT_ID,REQUIRED_PRODUCT_ID,DEPENDENCY_TYPE) VALUES (?,?,?)",
                    (pid, rpid, dtype))

    print("  Seeding SERVICE_SUBSCRIPTION...")
    sub_statuses  = ["ACTIVE","ACTIVE","ACTIVE","SUSPENDED","TERMINATED","PENDING_ACTIVATION"]
    sub_products  = [1,2,3,4,5,6,7,8,9]
    subscription_ids = []
    for cid in range(1, NUM_CUSTOMERS + 1):
        for _ in range(random.randint(1, 3)):
            pid     = random.choice(sub_products)
            status  = random.choice(sub_statuses)
            start   = rand_date(1095, 0)
            end_d   = None if status != "TERMINATED" else rand_date(30, 0)
            act_d   = start
            msisdn  = f"+1{random.randint(2000000000,9999999999)}"
            imsi    = f"310260{random.randint(1000000000,9999999999)}"
            cur.execute("""INSERT INTO SERVICE_SUBSCRIPTION
                (CUSTOMER_ID,PRODUCT_ID,STATUS,MSISDN,IMSI,START_DATE,END_DATE,ACTIVATION_DATE)
                VALUES (?,?,?,?,?,?,?,?)""", (cid, pid, status, msisdn, imsi, start, end_d, act_d))
            subscription_ids.append(cur.lastrowid)

    print("  Seeding ORDER_HEADER...")
    order_types   = ["NEW_SERVICE","CHANGE","UPGRADE","DOWNGRADE","TERMINATION","PORT_IN"]
    order_statuses= ["COMPLETED","COMPLETED","COMPLETED","FAILED","FALLOUT","IN_PROGRESS","CANCELLED"]
    channels      = ["ONLINE","RETAIL","TELESALES","API","DEALER"]
    order_ids     = []
    for i in range(1, 81):
        cid     = random.randint(1, NUM_CUSTOMERS)
        otype   = random.choice(order_types)
        ostatus = random.choice(order_statuses)
        channel = random.choice(channels)
        sub_dt  = rand_dt(730, 0)
        comp_dt = None if ostatus != "COMPLETED" else rand_dt(30, 0)
        fail_dt = None if ostatus not in ("FAILED","FALLOUT") else rand_dt(30, 0)
        cur.execute("""INSERT INTO ORDER_HEADER
            (ORDER_NUMBER,CUSTOMER_ID,ORDER_TYPE,STATUS,CHANNEL,SUBMITTED_DATE,COMPLETED_DATE,FAILED_DATE)
            VALUES (?,?,?,?,?,?,?,?)""",
            (f"ORD-{2024000+i}", cid, otype, ostatus, channel, sub_dt, comp_dt, fail_dt))
        order_ids.append(cur.lastrowid)

    print("  Seeding ORDER_LINE_ITEM...")
    li_statuses  = ["COMPLETED","COMPLETED","FAILED","PENDING","IN_PROGRESS"]
    action_types = ["ADD","REMOVE","CHANGE","SUSPEND","RESUME"]
    line_item_ids = {oid: [] for oid in order_ids}
    for oid in order_ids:
        for _ in range(random.randint(1, 3)):
            pid   = random.randint(1, len(products))
            price = round(random.uniform(10, 100), 2)
            cur.execute("""INSERT INTO ORDER_LINE_ITEM
                (ORDER_ID,PRODUCT_ID,ACTION_TYPE,STATUS,QUANTITY,UNIT_PRICE,EFFECTIVE_DATE)
                VALUES (?,?,?,?,?,?,?)""",
                (oid, pid, random.choice(action_types),
                 random.choice(li_statuses), 1, price, rand_date(365, 0)))
            line_item_ids[oid].append(cur.lastrowid)

    print("  Seeding ORDER_FALLOUT...")
    error_scenarios = [
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
    fo_statuses = ["OPEN","IN_PROGRESS","RESOLVED","ESCALATED"]
    fallout_ids = []
    for oid in random.sample(order_ids, min(20, len(order_ids))):
        err    = random.choice(error_scenarios)
        fstat  = random.choice(fo_statuses)
        fo_dt  = rand_dt(365, 0)
        res_dt = rand_dt(30, 0) if fstat == "RESOLVED" else None
        li_lst = line_item_ids.get(oid, [])
        li     = random.choice(li_lst) if li_lst else None
        cur.execute("""INSERT INTO ORDER_FALLOUT
            (ORDER_ID,LINE_ITEM_ID,ERROR_CODE,ERROR_MESSAGE,DOMAIN,SEVERITY,
             SYSTEM_SOURCE,FALLOUT_DATE,RESOLVED_DATE,STATUS,RETRY_COUNT)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (oid, li, err[0], err[1], err[2], err[3], err[4],
             fo_dt, res_dt, fstat, random.randint(0,5)))
        fallout_ids.append((cur.lastrowid, oid))

    print("  Seeding BILLING_ACCOUNT...")
    pay_methods = ["CREDIT_CARD","DEBIT_CARD","ACH","AUTOPAY"]
    ba_statuses = ["ACTIVE","ACTIVE","ACTIVE","SUSPENDED","COLLECTIONS"]
    for cid in range(1, NUM_CUSTOMERS + 1):
        cur.execute("""INSERT INTO BILLING_ACCOUNT
            (CUSTOMER_ID,ACCOUNT_NUMBER,PAYMENT_METHOD,CREDIT_LIMIT,CURRENT_BALANCE,
             BILLING_DAY,STATUS,AUTOPAY_ENABLED)
            VALUES (?,?,?,?,?,?,?,?)""",
            (cid, f"BA-{9000000+cid}", random.choice(pay_methods),
             random.choice([500,1000,2000,5000]),
             round(random.uniform(0, 300), 2),
             random.randint(1, 28), random.choice(ba_statuses), random.choice([0,1])))

    print("  Seeding BILLING_INVOICE...")
    inv_statuses = ["PAID","PAID","ISSUED","OVERDUE","DISPUTED"]
    invoice_ids  = []
    for ba_id in range(1, NUM_CUSTOMERS + 1):
        for m in range(1, random.randint(2, 5)):
            from datetime import date
            period_start = (datetime.now() - timedelta(days=30*m)).strftime("%Y-%m-%d")
            period_end   = (datetime.now() - timedelta(days=30*m-29)).strftime("%Y-%m-%d")
            subtotal     = round(random.uniform(30, 250), 2)
            tax          = round(subtotal * 0.08, 2)
            total        = subtotal + tax
            istatus      = random.choice(inv_statuses)
            due          = (datetime.now() - timedelta(days=30*m-21)).strftime("%Y-%m-%d")
            paid         = rand_date(30, 0) if istatus == "PAID" else None
            inv_num      = f"INV-{7000000 + len(invoice_ids) + 1}"
            cur.execute("""INSERT INTO BILLING_INVOICE
                (BILLING_ACCOUNT_ID,INVOICE_NUMBER,BILLING_PERIOD_START,BILLING_PERIOD_END,
                 SUBTOTAL,TAX_AMOUNT,TOTAL_AMOUNT,STATUS,DUE_DATE,PAID_DATE)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (ba_id, inv_num, period_start, period_end,
                 subtotal, tax, total, istatus, due, paid))
            invoice_ids.append(cur.lastrowid)

    print("  Seeding BILLING_CHARGE...")
    charge_types = ["RECURRING","USAGE","ONE_TIME","ADJUSTMENT","TAX","FEE"]
    for inv_id in invoice_ids:
        for _ in range(random.randint(1, 4)):
            sub_id = random.choice(subscription_ids) if subscription_ids else None
            amount = round(random.uniform(5, 100), 2)
            tax_a  = round(amount * 0.08, 2)
            cur.execute("""INSERT INTO BILLING_CHARGE
                (INVOICE_ID,SUBSCRIPTION_ID,CHARGE_TYPE,DESCRIPTION,AMOUNT,TAX_AMOUNT,CHARGE_DATE)
                VALUES (?,?,?,?,?,?,?)""",
                (inv_id, sub_id, random.choice(charge_types),
                 fake.bs(), amount, tax_a, rand_date(60, 0)))

    print("  Seeding USAGE_RECORD...")
    usage_types = [("DATA","MB"),("VOICE","MIN"),("SMS","COUNT"),("ROAMING_DATA","MB")]
    nodes       = ["eNB-001","eNB-042","gNB-113","gNB-220","MME-01","SGW-03"]
    for sid in random.sample(subscription_ids, min(80, len(subscription_ids))):
        for _ in range(random.randint(3, 8)):
            utype, unit = random.choice(usage_types)
            qty   = round(random.uniform(1, 5000), 2)
            st    = rand_dt(365, 0)
            et    = (datetime.strptime(st, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=random.randint(1,60))).strftime("%Y-%m-%d %H:%M:%S")
            cur.execute("""INSERT INTO USAGE_RECORD
                (SUBSCRIPTION_ID,USAGE_TYPE,QUANTITY,UNIT,START_TIME,END_TIME,RATED_AMOUNT,IS_RATED,NETWORK_NODE)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (sid, utype, qty, unit, st, et, round(qty*0.001,4), 1, random.choice(nodes)))

    print("  Seeding PROVISIONING_LOG...")
    systems  = ["HLR","OCS","EPC","IMS","PCRF","HSS","BSS_GATEWAY","NUMBER_MGT"]
    actions  = ["ACTIVATE","DEACTIVATE","MODIFY","SUSPEND","RESUME","ASSIGN_MSISDN","SET_QUOTA"]
    p_status = ["SUCCESS","SUCCESS","SUCCESS","FAILED","TIMEOUT","PARTIAL"]
    for oid in random.sample(order_ids, min(40, len(order_ids))):
        sid = random.choice(subscription_ids) if subscription_ids else None
        for _ in range(random.randint(1, 4)):
            sys_n  = random.choice(systems)
            act    = random.choice(actions)
            pstat  = random.choice(p_status)
            rcode  = "200" if pstat == "SUCCESS" else random.choice(["500","503","408","409"])
            err    = None if pstat == "SUCCESS" else fake.sentence()
            cur.execute("""INSERT INTO PROVISIONING_LOG
                (ORDER_ID,SUBSCRIPTION_ID,SYSTEM_NAME,ACTION,STATUS,RESPONSE_CODE,
                 ERROR_DETAIL,ATTEMPT_NUMBER,DURATION_MS)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (oid, sid, sys_n, act, pstat, rcode, err,
                 random.randint(1,3), random.randint(50,5000)))

    print("  Seeding NETWORK_ALARM...")
    alarm_types  = ["NODE_DOWN","HIGH_LATENCY","PACKET_LOSS","CAPACITY","LINK_FAILURE","HW_FAULT"]
    severities   = ["CRITICAL","MAJOR","MAJOR","MINOR","WARNING"]
    elem_types   = ["eNB","gNB","MME","SGW","OCS","PCRF","HSS"]
    regions      = ["NE","SE","MW","SW","NW","CENTRAL"]
    al_statuses  = ["ACTIVE","ACTIVE","CLEARED","ACKNOWLEDGED"]
    alarm_ids    = []
    for i in range(1, 16):
        atype  = random.choice(alarm_types)
        sev    = random.choice(severities)
        et     = random.choice(elem_types)
        region = random.choice(regions)
        elem   = f"{et}-{fake.numerify('###')}-{region}"
        raised = rand_dt(180, 0)
        astat  = random.choice(al_statuses)
        cleared= rand_dt(30, 0) if astat == "CLEARED" else None
        cur.execute("""INSERT INTO NETWORK_ALARM
            (ALARM_CODE,ALARM_TYPE,SEVERITY,NETWORK_ELEMENT,ELEMENT_TYPE,REGION,
             DESCRIPTION,RAISED_TIME,CLEARED_TIME,STATUS,AFFECTED_SUBS)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (f"ALM-{8000+i}", atype, sev, elem, et, region,
             f"{atype} detected on {elem}", raised, cleared, astat,
             random.randint(0, 5000)))
        alarm_ids.append(cur.lastrowid)

    print("  Seeding INCIDENT_TICKET...")
    categories  = ["ORDER_FALLOUT","BILLING","NETWORK","PROVISIONING","SERVICE_QUALITY"]
    priorities  = ["P1","P1","P2","P2","P3","P3","P3","P4"]
    tk_statuses = ["OPEN","IN_PROGRESS","RESOLVED","CLOSED","ASSIGNED"]
    teams       = ["NOC","BSS_SUPPORT","BILLING_OPS","PROVISIONING_ENG","NETWORK_ENG"]
    for i, (fo_id, oid) in enumerate(fallout_ids[:15], 1):
        cid    = random.randint(1, NUM_CUSTOMERS)
        al_id  = random.choice(alarm_ids) if random.random() < 0.4 and alarm_ids else None
        tstat  = random.choice(tk_statuses)
        cr_dt  = rand_dt(180, 0)
        res_dt = rand_dt(30, 0) if tstat in ("RESOLVED","CLOSED") else None
        cur.execute("""INSERT INTO INCIDENT_TICKET
            (TICKET_NUMBER,CUSTOMER_ID,ORDER_ID,FALLOUT_ID,ALARM_ID,TITLE,
             CATEGORY,PRIORITY,STATUS,ASSIGNED_TEAM,ASSIGNED_TO,CREATED_DATE,RESOLVED_DATE)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (f"TKT-{5000000+i}", cid, oid, fo_id, al_id,
             f"Fallout on order ORD-{2024000+i}: auto-detected issue",
             random.choice(categories), random.choice(priorities),
             tstat, random.choice(teams), fake.name(), cr_dt, res_dt))

    conn.commit()


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"Setting up SQLite database at: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    print("Schema created.")
    seed(conn)
    conn.close()

    # Quick verification
    conn = sqlite3.connect(DB_PATH)
    tables = ["CUSTOMER","ORDER_HEADER","ORDER_FALLOUT","SERVICE_SUBSCRIPTION",
              "BILLING_ACCOUNT","NETWORK_ALARM","INCIDENT_TICKET"]
    print("\nRow counts:")
    for t in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t:<30} {count:>5} rows")
    conn.close()
    print("\nDone! Database ready.")
