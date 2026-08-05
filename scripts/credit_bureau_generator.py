#!/usr/bin/env python3
"""
Continuous generator for the CDC demo pipeline (EPAS -> Debezium -> Kafka -> FlowServer -> WarehousePG).

Unlike order-generator.go / iot-generator.go (which produce directly to Kafka),
this writes ordinary INSERT/UPDATE statements straight into EPAS's oltp schema —
Debezium's logical replication is what turns those row changes into CDC events,
exactly as it would for a real operational system. This script never touches
Kafka directly.

Runs three independent activities on every tick, each gated by its own
probability (converted from a per-minute rate so behavior doesn't depend on
--interval):
  - new account origination      -> INSERT into oltp.credit_accounts
  - existing account update       -> UPDATE oltp.credit_accounts (merge/upsert path downstream)
  - new bureau score event        -> INSERT into oltp.bureau_score_events

Usage:
  python3 credit_bureau_generator.py \
      --new-accounts-rate 2 --update-rate 3 --score-rate 2 --interval 5

  (rates are "per minute"; --interval is seconds between ticks)

Env vars (same convention as the dashboard API):
  EPAS_HOST EPAS_PORT EPAS_USER EPAS_PASSWORD EPAS_DB
"""
import argparse
import os
import random
import signal
import sys
import time

import psycopg2

EPAS_HOST     = os.environ.get("EPAS_HOST", "localhost")
EPAS_PORT     = int(os.environ.get("EPAS_PORT", 5444))
EPAS_USER     = os.environ.get("EPAS_USER", "enterprisedb")
EPAS_PASSWORD = os.environ.get("EPAS_PASSWORD", "admin")
EPAS_DB       = os.environ.get("EPAS_DB", "tu")

PRODUCTS       = ["credit_card", "personal_loan", "auto_loan", "mortgage", "line_of_credit"]
SCORE_TRIGGERS = ["new_inquiry", "account_update", "delinquency", "payoff", "periodic_refresh"]

_stop_requested = False


def _handle_signal(signum, frame):
    global _stop_requested
    _stop_requested = True


def get_conn():
    return psycopg2.connect(
        host=EPAS_HOST, port=EPAS_PORT, user=EPAS_USER,
        password=EPAS_PASSWORD, dbname=EPAS_DB, connect_timeout=5,
    )


def pick_dpd_and_status(outstanding_amount, sanctioned_amount, product):
    """
    Choose dpd_bucket/status so delinquency actually correlates with the
    features the MADlib model uses (utilization ratio, product) — instead of
    independent random draws, which produce a model that just learns the
    overall base rate and can't separate accounts into meaningful risk tiers.
    Higher utilization (and revolving products like credit_card/personal_loan)
    push probability of delinquency up.
    """
    utilization = min(float(outstanding_amount) / max(float(sanctioned_amount), 1.0), 1.5)
    p_delinquent = 0.05 + 0.55 * min(utilization, 1.0)
    if product in ("credit_card", "personal_loan"):
        p_delinquent += 0.08
    p_delinquent = min(max(p_delinquent, 0.02), 0.95)

    if random.random() < p_delinquent:
        dpd_bucket = random.choices(["60", "90+"], weights=[0.55, 0.45])[0]
        status = random.choices(["delinquent", "active"], weights=[0.75, 0.25])[0]
    else:
        dpd_bucket = random.choices(["current", "30"], weights=[0.8, 0.2])[0]
        status = random.choices(["active", "closed"], weights=[0.85, 0.15])[0]
    return dpd_bucket, status


def insert_new_account(cur):
    cur.execute("SELECT COALESCE(MAX(account_id), 100000) FROM oltp.credit_accounts")
    next_id = cur.fetchone()[0] + 1
    consumer_id = random.randint(1000, 9999)
    lender_id = random.randint(1, 20)
    product = random.choice(PRODUCTS)
    sanctioned = round(random.uniform(1000, 50000), 2)
    outstanding = round(sanctioned * random.uniform(0.1, 0.95), 2)
    dpd_bucket, status = pick_dpd_and_status(outstanding, sanctioned, product)
    cur.execute("""
        INSERT INTO oltp.credit_accounts
            (account_id, consumer_id, lender_id, product, sanctioned_amount,
             outstanding_amount, dpd_bucket, status, opened_ts, event_ts)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now(), now())
    """, (next_id, consumer_id, lender_id, product, sanctioned, outstanding, dpd_bucket, status))
    return next_id


def update_existing_account(cur):
    cur.execute("""
        SELECT account_id, outstanding_amount, sanctioned_amount, product
        FROM oltp.credit_accounts ORDER BY random() LIMIT 1""")
    row = cur.fetchone()
    if not row:
        return None
    account_id, outstanding, sanctioned, product = row
    new_outstanding = round(float(outstanding) * random.uniform(0.70, 0.98), 2)
    new_dpd, new_status = pick_dpd_and_status(new_outstanding, sanctioned, product)
    cur.execute("""
        UPDATE oltp.credit_accounts
        SET outstanding_amount = %s, dpd_bucket = %s, status = %s, event_ts = now()
        WHERE account_id = %s
    """, (new_outstanding, new_dpd, new_status, account_id))
    return account_id


def insert_score_event(cur):
    cur.execute("SELECT COALESCE(MAX(score_event_id), 100000) FROM oltp.bureau_score_events")
    next_id = cur.fetchone()[0] + 1

    # Link to a real existing account's consumer_id most of the time — otherwise
    # score events land on consumer_ids that never match any credit_accounts row,
    # and the "latest bureau score" feature in the MADlib pipeline just falls
    # back to its neutral default for almost every account. A small fraction of
    # unlinked consumer_ids is left in on purpose (bureau-only consumers with no
    # account at this lender yet — realistic).
    consumer_id = None
    cur.execute("SELECT consumer_id FROM oltp.credit_accounts ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if row and random.random() < 0.85:
        consumer_id = row[0]
    if consumer_id is None:
        consumer_id = random.randint(1000, 9999)

    score_value = random.randint(300, 850)
    score_version = random.choice(["v3.0", "v4.0"])
    score_trigger = random.choice(SCORE_TRIGGERS)
    cur.execute("""
        INSERT INTO oltp.bureau_score_events
            (score_event_id, consumer_id, score_value, score_version, score_trigger, score_ts)
        VALUES (%s, %s, %s, %s, %s, now())
    """, (next_id, consumer_id, score_value, score_version, score_trigger))
    return next_id


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--new-accounts-rate", type=float, default=2.0, help="new accounts per minute")
    ap.add_argument("--update-rate",       type=float, default=3.0, help="existing-account updates per minute")
    ap.add_argument("--score-rate",        type=float, default=2.0, help="new bureau score events per minute")
    ap.add_argument("--interval",          type=float, default=5.0, help="seconds between generator ticks")
    ap.add_argument("--max-runtime",       type=int,   default=0,   help="stop after N seconds (0 = unlimited)")
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    ticks_per_min = 60.0 / args.interval
    p_new    = args.new_accounts_rate / ticks_per_min
    p_update = args.update_rate / ticks_per_min
    p_score  = args.score_rate / ticks_per_min

    print(f"Starting credit-bureau-generator: new_accounts={args.new_accounts_rate}/min "
          f"updates={args.update_rate}/min scores={args.score_rate}/min "
          f"interval={args.interval}s  target={EPAS_USER}@{EPAS_HOST}:{EPAS_PORT}/{EPAS_DB}",
          flush=True)

    total_new = total_upd = total_score = total_err = 0
    start = time.time()
    last_report = start

    while not _stop_requested:
        if args.max_runtime and (time.time() - start) >= args.max_runtime:
            break

        conn = None
        try:
            conn = get_conn()
            conn.autocommit = False
            cur = conn.cursor()

            if random.random() < p_new:
                insert_new_account(cur)
                total_new += 1
            if random.random() < p_update:
                if update_existing_account(cur) is not None:
                    total_upd += 1
            if random.random() < p_score:
                insert_score_event(cur)
                total_score += 1

            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            total_err += 1
            print(f"ERROR: {e}", flush=True)
            if conn:
                try:
                    conn.rollback()
                    conn.close()
                except Exception:
                    pass

        if time.time() - last_report >= 5:
            print(f"new_accounts={total_new}  updates={total_upd}  score_events={total_score}  "
                  f"errors={total_err}  elapsed={int(time.time()-start)}s", flush=True)
            last_report = time.time()

        time.sleep(args.interval)

    print(f"Stopped. Total: new_accounts={total_new}  updates={total_upd}  "
          f"score_events={total_score}  errors={total_err}", flush=True)


if __name__ == "__main__":
    main()

