#!/usr/bin/env python3
"""
Remittance Platform Comparison — Local Web Service
================================================

Implements all query logic locally, issuing concurrent requests to
mid-market rate sources and remittance platforms to avoid cumulative delays.

Usage:
  .venv/bin/python3 webapp.py                 # Default 127.0.0.1:5000
  .venv/bin/python3 webapp.py --port 8000     # Specify port

Open http://127.0.0.1:5000/ the your browser.
"""

import argparse
import json
import re
# from curl_cffi import requests as cffi_requests
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from curl_cffi import requests as cffi_requests
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, render_template, request, send_from_directory

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__)

# ============================================================
# General Configuration & Currency Metadata
# ============================================================

SOURCE_META = {
    "GBP": {"remitly": "GBR", "panda": "GBR",
            "lemfi_country": "United Kingdom", "worldremit_country": "GB",
            "taptap_country": "GB"},
    "AUD": {"remitly": "AUS", "panda": "AUS", "lemfi_country": "Australia",
            "worldremit_country": "AU", "taptap_country": "AU"},
    "EUR": {"remitly": "DEU", "panda": "EU", "lemfi_country": "Germany",
            "worldremit_country": "DE", "taptap_country": "DE"},
    "USD": {"remitly": "USA", "panda": "USA",
            "lemfi_country": "United States", "worldremit_country": "US",
            "taptap_country": "US"},
    "CAD": {"remitly": "CAN", "panda": "CAN", "lemfi_country": "Canada",
            "worldremit_country": "CA", "taptap_country": "CA"}
}

TARGET_META = {
    "CNY": {"remitly": "CHN", "panda": "CHN", "lemfi_to": "CNY",
            "worldremit_country": "CN", "worldremit_payout": "WLT"},
    "INR": {"remitly": "IND", "panda": "IND", "lemfi_to": "INR",
            "worldremit_country": "IN", "worldremit_payout": "BNK"},
    "NGN": {"remitly": "NGA", "panda": "NGA", "lemfi_to": "NGN",
            "worldremit_country": "NG", "worldremit_payout": "BNK"},
    "GHS": {"remitly": "GHA", "panda": "GHA", "lemfi_to": "GHS",
            "worldremit_country": "GH", "worldremit_payout": "MOB"},
    "KES": {"remitly": "KEN", "panda": "KEN", "lemfi_to": "KES",
            "worldremit_country": "KE", "worldremit_payout": "MOB"},
    "PHP": {"remitly": "PHL", "panda": "PHL", "lemfi_to": "PHP",
            "worldremit_country": "PH", "worldremit_payout": "BNK"},
    "PKR": {"remitly": "PAK", "panda": "PAK", "lemfi_to": "PKR",
            "worldremit_country": "PK", "worldremit_payout": "BNK"},
    "BDT": {"remitly": "BGD", "panda": "BGD", "lemfi_to": "BDT",
            "worldremit_country": "BD", "worldremit_payout": "BNK"},
    "BRL": {"remitly": "BRA", "panda": "BRA", "lemfi_to": "BRL",
            "worldremit_country": "BR", "worldremit_payout": "BNK"},
    "VND": {"remitly": "VNM", "panda": "VNM", "lemfi_to": "VND",
            "worldremit_country": "VN", "worldremit_payout": "BNK"},
    "EGP": {"remitly": "EGY", "panda": "EGY", "lemfi_to": "EGP",
            "worldremit_country": "EG", "worldremit_payout": "BNK"},
    "NPR": {"remitly": "NPL", "panda": "NPL", "lemfi_to": "NPR",
            "worldremit_country": "NP", "worldremit_payout": "BNK"},
    "XOF": {"remitly": "BEN", "panda": "BEN", "lemfi_to": "XOF",
            "worldremit_country": "BJ", "worldremit_payout": "MOB"},
    "RWF": {"remitly": "RWA", "panda": "RWA", "lemfi_to": "RWF",
            "worldremit_country": "RW", "worldremit_payout": "MOB"},
    "TZS": {"remitly": "TZA", "panda": "TZA", "lemfi_to": "TZS",
            "worldremit_country": "TZ", "worldremit_payout": "MOB"},
    "XAF": {"remitly": "CMR", "panda": "CMR", "lemfi_to": "XAF",
            "worldremit_country": "CM", "worldremit_payout": "MOB"},
    "UGX": {"remitly": "UGA", "panda": "UGA", "lemfi_to": "UGX",
            "worldremit_country": "UG", "worldremit_payout": "MOB"}
}

MAX_RETRIES = 3
RETRY_BASE_DELAY = 3  # seconds
MAX_WORKERS = 8

BROWSER_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


# ============================================================
# Utility Functions
# ============================================================

def parse_http_date(date_str):
    """Parse HTTP date response header (GMT) to local readable format"""
    if not date_str:
        return "N/A"
    try:
        dt = parsedate_to_datetime(date_str)
        dt_local = dt.astimezone(timezone(timedelta(hours=8)))
        return dt_local.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return date_str

def parse_iso_time(iso_str):
    """Parse ISO 8601 UTC time to local readable format"""
    if not iso_str:
        return "N/A"
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        dt_local = dt.astimezone(timezone(timedelta(hours=8)))
        return dt_local.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return iso_str

def http_get_with_retry(url, params=None, headers=None, timeout=15):
    """GET request with automatic 429 retries"""
    last_exc = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code == 429 and attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            last_exc = e
            break
        except requests.exceptions.RequestException as e:
            last_exc = e
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                time.sleep(delay)
                continue
            raise
    if last_exc:
        raise last_exc


def http_post_with_retry(url, payload=None, headers=None, timeout=15):
    """POST request with automatic 429 retries"""
    last_exc = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
            if resp.status_code == 429 and attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            last_exc = e
            break
        except requests.exceptions.RequestException as e:
            last_exc = e
            if attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                time.sleep(delay)
                continue
            raise
    if last_exc:
        raise last_exc


# ============================================================
# Bank of China Mid-Market Rate API
# ============================================================

CHINAMONEY_API_URL = "https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/fx/ccpr.json"


def query_chinamoney_mid_rate(source, target):
    """Queries Bank of China mid-market rate (only supports CNY targets)."""
    if target != "CNY":
        return {"name": "Bank of China", "error": "Only supports CNY targets"}
   
    headers = {
        "User-Agent": BROWSER_UA,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": "https://www.chinamoney.com.cn/chinese/bkccpr/",
    }

    try:
        resp = http_get_with_retry(CHINAMONEY_API_URL, headers=headers, timeout=15)
        data = resp.json()
    except Exception:
        return {"name": "Bank of China", "error": "Fetch failed"}

    last_date = data.get("data", {}).get("lastDate", "") or data.get("lastDate", "")
    records = data.get("data", {}).get("records", []) or data.get("records", [])
    
    pair = f"{source}/CNY"
    gbp_record = next((r for r in records if r.get("vrtEName", "") == pair), None)

    if not gbp_record:
        return {"name": "Bank of China", "error": f"No data found for {pair}"}

    try:
        rate = float(gbp_record.get("price", "0"))
    except (ValueError, TypeError):
        return {"name": "Bank of China", "error": "Invalid rate format"}

    return {
        "name": "Bank of China",
        "rate": rate,
        "bp": gbp_record.get("bpDouble", 0),
        "quote_time": last_date if last_date else "N/A",
        "source": "China Foreign Exchange Trade System",
    }


# ============================================================
# AllRatesToday Mid-Market Rate API
# ============================================================

ALLRATESTODAY_API_URL = "https://allratestoday.com/api/rate"
ALLRATESTODAY_API_KEY = "art_live_c1UbQxoIOavP2lVwaW1KZ2rPh5FyZH5b"

def query_allratestoday_mid_rate(source, target):
    """Queries AllRatesToday interbank mid-market rate."""
    headers = {"User-Agent": BROWSER_UA, "Accept": "application/json"}
    params = {"source": source, "target": target, "key": ALLRATESTODAY_API_KEY}

    try:
        resp = http_get_with_retry(ALLRATESTODAY_API_URL, params=params, headers=headers, timeout=15)
        data = resp.json()
    except Exception:
        return {"name": "AllRatesToday", "error": "Fetch failed"}

    rate = data.get("rate")
    if rate is None:
        return {"name": "AllRatesToday", "error": "No rate field in response"}

    try:
        rate = float(rate)
    except (ValueError, TypeError):
        return {"name": "AllRatesToday", "error": f"Invalid rate format: {rate}"}

    return {
        "name": "AllRatesToday",
        "rate": rate,
        "quote_time": parse_http_date(resp.headers.get("date", "")),
        "source": f"AllRatesToday ({data.get('source', 'interbank')})",
    }


# ============================================================
# Wise API
# ============================================================

WISE_API_URL = "https://api.wise.com/v3/quotes"

WISE_PAYIN_LABELS = {
    "PISP": "Bank Transfer",
    "BANK_TRANSFER": "Bank Transfer",
    "BALANCE": "Wise Balance",
    "DEBIT": "Debit Card",
    "CREDIT": "Credit Card",
    "SWIFT": "SWIFT Transfer",
}

WISE_PAYOUT_LABELS = {
    "ALIPAY": "Alipay",
    "WECHATPAY": "WeChat",
    "CARD": "Bank Card",
    "ACCOUNT": "Bank Account"
}

def query_wise(total_payment, source, target):
    """Queries Wise Quotes API dynamically with fallback payout handling."""
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    
    payload = {
        "sourceCurrency": source,
        "targetCurrency": target,
        "sourceAmount": total_payment,
        "preferredPayIn": "BANK_TRANSFER",
    }
    
    if target == "CNY":
        payload["payOut"] = "ALIPAY"

    try:
        resp = http_post_with_retry(WISE_API_URL, payload=payload, headers=headers)
        data = resp.json()
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else "Unknown"
        return {"name": "Wise", "error": f"HTTP {status_code}"}
    except Exception:
        return {"name": "Wise", "error": "Connection Failed"}

    rate = data.get("rate", 0)
    options = data.get("paymentOptions", [])
    
    if not options:
        return {"name": "Wise", "error": f"No options for {source}->{target}"}

    opt = next((o for o in options if o.get("payIn") in ["PISP", "BANK_TRANSFER"] and not o.get("disabled")), None) or options[0]

    fee_total = opt.get("fee", {}).get("total", 0)
    receive = opt.get("targetAmount", 0)
    actual_pay_in = opt.get("payIn", "Bank Transfer")
    actual_payout = opt.get("payOut", "Bank Account")

    return {
        "name": "Wise",
        "method": f"{WISE_PAYIN_LABELS.get(actual_pay_in, actual_pay_in)} · {WISE_PAYOUT_LABELS.get(actual_payout, actual_payout)}",
        "rate": rate,
        "fee": fee_total,
        "conversion": opt.get("sourceAmount") or total_payment,
        "receive": receive,
        "total_payment": total_payment,
        "effective_rate": receive / total_payment if total_payment > 0 else 0,
        "rate_type": "Mid-market rate",
        "quote_time": parse_iso_time(data.get("createdTime", "")),
    }


# ============================================================
# Remitly API
# ============================================================

REMITLY_API_URL = "https://api.remitly.io/v3/calculator/estimate"
REMITLY_HEADERS = {
    "Accept": "application/json",
    "User-Agent": BROWSER_UA,
    "Origin": "https://www.remitly.com",
    "Referer": "https://www.remitly.com/",
}

def query_remitly(total_payment, source, target):
    """Queries Remitly estimate using dynamic conduit."""
    source_meta = SOURCE_META.get(source)
    target_meta = TARGET_META.get(target)
    
    if not source_meta or not target_meta:
        return {"name": "Remitly", "error": f"Corridor {source}->{target} unsupported"}
        
    params = {
        "conduit": f"{source_meta['remitly']}:{source}-{target_meta['remitly']}:{target}",
        "anchor": "SEND",
        "amount": total_payment,
        "purpose": "OTHER",
        "customer_segment": "STANDARD",
        "customer_recognition": "UNRECOGNIZED",
    }
    
    try:
        resp = http_get_with_retry(REMITLY_API_URL, params=params, headers=REMITLY_HEADERS)
        data = resp.json()
    except Exception:
        return {"name": "Remitly", "error": "API Error"}

    estimate = data.get("estimate", {})
    rate = float(estimate.get("exchange_rate", {}).get("base_rate", "0"))
    fee = float(estimate.get("fee", {}).get("total_fee_amount", "0"))
    conversion = total_payment - fee
    receive = conversion * rate

    return {
        "name": "Remitly",
        "method": "Standard",
        "rate": rate,
        "fee": fee,
        "conversion": conversion,
        "receive": receive,
        "total_payment": total_payment,
        "effective_rate": receive / total_payment if total_payment > 0 else 0,
        "rate_type": "Includes rate markup",
        "quote_time": parse_http_date(resp.headers.get("date", "")),
    }


# ============================================================
# Panda Remit API
# ============================================================

PANDA_FEE_API = "https://prod.pandaremit.com/web/ratefee/fee"
PANDA_RATE_API = "https://prod.pandaremit.com/pricing/rate/query/diamond"
PANDA_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": BROWSER_UA,
    "Origin": "https://www.pandaremit.com",
}

def query_panda(total_payment, source, target):
    """Queries Panda Remit for standard non-promotional transfer."""
    source_meta = SOURCE_META.get(source)
    target_meta = TARGET_META.get(target)
    
    if not source_meta or not target_meta:
        return {"name": "Panda Remit", "error": "Corridor unsupported"}

    fee_payload = {
        "sourceAmount": total_payment,
        "sourceCountry": source_meta["panda"],
        "sourceCurrency": source,
        "targetCountry": target_meta["panda"],
        "targetCurrency": target,
        "payerId": "",
    }
    
    try:
        resp_fee = http_post_with_retry(PANDA_FEE_API, payload=fee_payload, headers=PANDA_HEADERS)
        default_fee = float(resp_fee.json().get("model", {}).get("defaultFee", "0"))
        
        time.sleep(0.3)
        rate_payload = {"sourceCurrency": source, "targetCurrency": target, "requestSource": "1"}
        resp_rate = http_post_with_retry(PANDA_RATE_API, payload=rate_payload, headers=PANDA_HEADERS)
        rate_model = resp_rate.json().get("model", {})
    except Exception:
        return {"name": "Panda Remit", "error": "API Error"}

    panda_rate = float(rate_model.get("pandaRate", "0"))
    conversion = total_payment - default_fee
    receive = conversion * panda_rate

    return {
        "name": "Panda Remit",
        "method": "Standard Transfer",
        "rate": panda_rate,
        "fee": default_fee,
        "conversion": conversion,
        "receive": receive,
        "total_payment": total_payment,
        "effective_rate": receive / total_payment if total_payment > 0 else 0,
        "rate_type": "Includes rate markup",
        "quote_time": parse_http_date(resp_rate.headers.get("date", "")),
    }


# ============================================================
# LemFi API
# ============================================================

LEMFY_API_URL = "https://www.lemfi.com/api/lemonade/v2/exchange"
LEMFY_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": BROWSER_UA,
    "Origin": "https://www.lemfi.com",
}

def query_lemfi(total_payment, source, target):
    """Queries LemFi API and gracefully handles unsupported corridors."""
    meta = SOURCE_META.get(source)
    if not meta:
        return {"name": "LemFi", "error": f"Unsupported source: {source}"}
        
    payload = {"from": source, "to": target, "sender_country": meta["lemfi_country"]}
    
    try:
        resp = http_post_with_retry(LEMFY_API_URL, payload=payload, headers=LEMFY_HEADERS)
        res_json = resp.json()
        
        if not res_json.get("success", True) or "data" not in res_json:
            return {"name": "LemFi", "error": f"Corridor {source}->{target} unsupported"}
            
        data = res_json.get("data", {})
    except Exception:
        return {"name": "LemFi", "error": f"Corridor {source}->{target} unsupported"}

    transaction_fee = float(data.get("transaction_fee", 0))
    min_txn = float(data.get("min_transaction_amount", 0))
    min_exchange = float(data.get("min_transaction_exchange_amount", 0))
    rate_raw_float = float(data.get("rate", "0"))

    rate = None
    if 0 < rate_raw_float < 50000:
        rate = rate_raw_float
    elif min_txn != transaction_fee and min_exchange != 0:
        candidate = min_exchange / (min_txn - transaction_fee)
        if 0 < candidate < 50000:
            rate = candidate

    if rate is None or rate == 0:
        return {"name": "LemFi", "error": f"No rate for {source}->{target}"}

    rate = round(rate, 3)
    fee = transaction_fee
    conversion = total_payment - fee
    receive = int((conversion * rate) * 100) / 100

    return {
        "name": "LemFi",
        "method": "Standard",
        "rate": rate,
        "fee": fee,
        "conversion": conversion,
        "receive": receive,
        "total_payment": total_payment,
        "effective_rate": receive / total_payment if total_payment > 0 else 0,
        "rate_type": "Includes markup",
        "quote_time": parse_http_date(resp.headers.get("date", "")),
    }


# ============================================================
# OrbitRemit API
# ============================================================

ORBIT_RATE_API = "https://www.orbitremit.com/api/rates"
ORBIT_FEE_API = "https://www.orbitremit.com/api/fees"
ORBIT_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": BROWSER_UA,
    "Origin": "https://www.orbitremit.com",
}

def query_orbit(total_payment, source, target):
    """Queries OrbitRemit (strictly limited to AUD source)."""
    if source != "AUD":
        return {"name": "Orbit Remit", "error": "Only supports AUD"}
        
    payout_type = "alipay_wallet" if target == "CNY" else "bank_account"
        
    try:
        fee_resp = http_get_with_retry(
            ORBIT_FEE_API,
            params={"send": source, "payout": target, "amount": f"{total_payment:.2f}", "type": payout_type},
            headers=ORBIT_HEADERS,
        )
        fee = float(fee_resp.json()["data"]["fee"])
        send_amount = total_payment - fee

        rate_payload = {
            "sendCurrency": source, "payoutCurrency": target, 
            "amount": f"{send_amount:.2f}", "recipientType": payout_type, "focus": "send"
        }
        resp = http_post_with_retry(ORBIT_RATE_API, payload=rate_payload, headers=ORBIT_HEADERS)
        attrs = resp.json()["data"]["data"]["attributes"]
    except Exception:
        return {"name": "Orbit Remit", "error": "API Error"}

    rate = float(attrs["standard_rate"])
    receive = send_amount * rate

    return {
        "name": "Orbit Remit",
        "method": "Standard",
        "rate": rate,
        "fee": fee,
        "conversion": send_amount,
        "receive": receive,
        "total_payment": total_payment,
        "effective_rate": receive / total_payment if total_payment > 0 else 0,
        "rate_type": "Includes rate markup",
        "quote_time": parse_http_date(fee_resp.headers.get("date", "")),
    }


# ============================================================
# WorldRemit GraphQL API
# ============================================================

WORLDREMIT_API_URL = "https://api.worldremit.com/graphql"
WORLDREMIT_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": BROWSER_UA,
    "Origin": "https://www.worldremit.com",
    "Referer": "https://www.worldremit.com/",
    "x-wr-platform": "Web",
}

WORLDREMIT_CALCULATION_QUERY = """
mutation createCalculation(
  $amount: BigDecimal!,
  $type: CalculationType!,
  $sendCountryCode: CountryCode!,
  $sendCurrencyCode: CurrencyCode!,
  $receiveCountryCode: CountryCode!,
  $receiveCurrencyCode: CurrencyCode!,
  $payOutMethodCode: String,
  $correspondentId: String
) {
  createCalculation(
    calculationInput: {
      amount: $amount,
      send: {country: $sendCountryCode, currency: $sendCurrencyCode},
      type: $type,
      receive: {country: $receiveCountryCode, currency: $receiveCurrencyCode},
      payOutMethodCode: $payOutMethodCode,
      correspondentId: $correspondentId
    }
  ) {
    calculation {
      id
      isFree
      informativeSummary {
        fee {
          value {
            amount
            currency
          }
          type
        }
        totalToPay {
          amount
        }
      }
      send {
        currency
        amount
      }
      receive {
        amount
        currency
      }
      exchangeRate {
        value
        crossedOutValue
      }
    }
    errors {
      ...GenericCalculationError
      ...ValidationCalculationError
    }
  }
}
fragment GenericCalculationError on GenericCalculationError {
  message
  genericType: type
}
fragment ValidationCalculationError on ValidationCalculationError {
  message
  type
  code
  description
}
"""


def query_worldremit(total_payment, source, target):
    """Queries WorldRemit via GraphQL API, trying multiple payout methods dynamically."""
    source_meta = SOURCE_META.get(source)
    target_meta = TARGET_META.get(target)

    if not source_meta or not target_meta:
        return {"name": "WorldRemit", "error": "Corridor unsupported"}

    # Prioritize the known default, then fall back to the others
    primary_method = target_meta.get("worldremit_payout", "BNK")

    # Add any new 3-letter payout codes to this master list
    all_methods = ["BNK", "MOB", "WLT", "CSH", "DEP", "DDP", "CRD"]

    methods_to_try = [primary_method] + [m for m in all_methods if m != primary_method]

    calc = None
    errors = []

    for method in methods_to_try:
        payload = {
            "operationName": "createCalculation",
            "variables": {
                "amount": total_payment,
                "type": "SEND",
                "sendCountryCode": source_meta.get("worldremit_country", "GB"),
                "sendCurrencyCode": source,
                "receiveCountryCode": target_meta.get("worldremit_country", "NG"),
                "receiveCurrencyCode": target,
                "payOutMethodCode": method,
                "correspondentId": ""
            },
            "query": WORLDREMIT_CALCULATION_QUERY
        }

        try:
            resp = cffi_requests.post(
                WORLDREMIT_API_URL, 
                json=payload, 
                headers=WORLDREMIT_HEADERS, 
                impersonate="chrome110",
                timeout=15
            )
            res_json = resp.json()
            calc = res_json.get("data", {}).get("createCalculation", {}).get("calculation")
            errors = res_json.get("data", {}).get("createCalculation", {}).get("errors", [])

            # If no GraphQL errors and calculation object exists, the method worked
            if calc and not errors:
                break
                
        except Exception:
            continue # If connection drops, move to the next method

    if not calc or errors:
        err_msg = errors[0].get("message") if errors else "No valid payout method found"
        return {"name": "WorldRemit", "error": f"WorldRemit Error: {err_msg}"}

    fee_val = calc.get("informativeSummary", {}).get("fee", {}).get("value", {}).get("amount", 0.0)
    fee = float(fee_val) if fee_val is not None else 0.0

    send_amount = float(calc.get("send", {}).get("amount", total_payment))
    receive_amount = float(calc.get("receive", {}).get("amount", 0.0))

    exchange_rate_data = calc.get("exchangeRate", {})
    rate_val = float(exchange_rate_data.get("value", 0.0)) if exchange_rate_data.get("value") is not None else 0.0
    crossed_out_val = exchange_rate_data.get("crossedOutValue")

    total_cost = send_amount + fee

    if crossed_out_val is not None and crossed_out_val != "":
        nominal_rate = float(crossed_out_val)
        calc_receive = send_amount * nominal_rate
        effective_rate = calc_receive / total_cost if total_cost > 0 else 0.0
        final_receive = calc_receive
    else:
        nominal_rate = rate_val
        final_receive = receive_amount
        effective_rate = receive_amount / total_cost if total_cost > 0 else 0.0

    return {
        "name": "WorldRemit",
        "method": "Standard",
        "rate": nominal_rate,
        "fee": fee,
        "conversion": send_amount,
        "receive": final_receive,
        "total_payment": total_payment,
        "effective_rate": effective_rate,
        "rate_type": "Includes rate markup",
        "quote_time": parse_http_date(resp.headers.get("date", "")),
    }


# ============================================================
# TapTap Send API
# ============================================================

TAPTAPSEND_API_URL = "https://api.taptapsend.com/api/fxRates"
TAPTAPSEND_HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "appian-version": "web/2022-05-03.0",
    "origin": "https://www.taptapsend.com",
    "referer": "https://www.taptapsend.com/",
    "user-agent": BROWSER_UA,
    "x-device-id": "web",
    "x-device-model": "web"
}

def query_taptapsend(total_payment, source, target):
    """Queries TapTap Send and dynamically processes flat, percentage, or tiered fees."""
    source_meta = SOURCE_META.get(source)
    if not source_meta:
        return {"name": "TapTap Send", "error": "Unsupported source"}
        
    taptap_country = source_meta.get("taptap_country")
    
    try:
        resp = http_get_with_retry(TAPTAPSEND_API_URL, headers=TAPTAPSEND_HEADERS)
        data = resp.json()
    except Exception as e:
        return {"name": "TapTap Send", "error": f"API Error: {str(e)}"}
        
    countries = data.get("availableCountries", [])
    
    # Locate the sender profile based on the source country code
    sender_profile = next((c for c in countries if c.get("isoCountryCode") == taptap_country), None)
    if not sender_profile:
        return {"name": "TapTap Send", "error": f"Sender country unsupported"}
        
    # Locate the target corridor
    corridors = sender_profile.get("corridors", [])
    corridor = next((c for c in corridors if c.get("currency") == target), None)
    
    if not corridor:
        return {"name": "TapTap Send", "error": f"Corridor {source}->{target} unsupported"}
        
    rate = float(corridor.get("fxRate", 0.0))
    fee = 0.0
    fee_tier_name = "Standard"
    
    # Process dynamic fee schedule
    fee_schedule = corridor.get("feeSchedule")
    if fee_schedule:
        fs_type = fee_schedule.get("type")
        if fs_type == "standard":
            flat = float(fee_schedule.get("flatFee", 0))
            pct = float(fee_schedule.get("feePercent", 0)) / 100.0
            fee = flat + (total_payment * pct)
            if "maxFee" in fee_schedule:
                fee = min(fee, float(fee_schedule["maxFee"]))
        elif fs_type == "tiered":
            fee_tier_name = "Tiered"
            # Sort tiers by minimum value ascending to safely override as total_payment increases
            tiers = sorted(fee_schedule.get("tiers", []), key=lambda x: float(x.get("minValue", 0)))
            for t in tiers:
                if total_payment >= float(t.get("minValue", 0)):
                    fee = float(t.get("fee", 0))
    elif "senderCurrencyFlatFee" in corridor:
        fee = float(corridor["senderCurrencyFlatFee"])
 
    conversion = total_payment - fee
    receive = conversion * rate
    effective_rate = receive / total_payment if total_payment > 0 else 0.0
    
    return {
        "name": "TapTap Send",
        "method": fee_tier_name,
        "rate": rate,
        "fee": fee,
        "conversion": conversion,
        "receive": receive,
        "total_payment": total_payment,
        "effective_rate": effective_rate,
        "rate_type": "Includes rate markup",
        "quote_time": parse_http_date(resp.headers.get("date", "")),
    }


# ============================================================
# MasterRemit API
# ============================================================

MASTERREMIT_API_URL = "https://www.masterremit.com/api/converter/public"
MASTERREMIT_HEADERS = {
    "accept": "application/json",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "dnt": "1",
    "pragma": "no-cache",
    "referer": "https://www.masterremit.com/",
    "user-agent": BROWSER_UA,
}


def query_masterremit(total_payment, source, target):
    """Queries MasterRemit public converter API, using regular sell exchange rates."""
    source_meta = SOURCE_META.get(source)
    target_meta = TARGET_META.get(target)

    if not source_meta or not target_meta:
        return {"name": "MasterRemit", "error": "Corridor unsupported"}

    send_country = source_meta.get("worldremit_country")
    recv_country = target_meta.get("worldremit_country")

    params = {
        "sending_country_code": send_country,
        "receiving_country_code": recv_country,
        "receiving_currency_code": target,
        "sending_amount": total_payment,
    }

    try:
        resp = http_get_with_retry(MASTERREMIT_API_URL, params=params, headers=MASTERREMIT_HEADERS)
        res_json = resp.json()

        if res_json.get("status") != 200 or "data" not in res_json:
            return {"name": "MasterRemit", "error": f"Corridor {source}->{target} unsupported"}

        data = res_json.get("data", {})
    except Exception as e:
        return {"name": "MasterRemit", "error": f"API Error: {str(e)}"}

    # Use regular exchange rate (avoid promo/special rates)
    rate_val = data.get("sell_exchange_rate") or data.get("exchange_rate") or 0.0
    rate = float(rate_val)
    if rate <= 0:
        return {"name": "MasterRemit", "error": f"No rate for {source}->{target}"}

    # Extract transfer fee
    fee_val = data.get("total_fees") if data.get("total_fees") is not None else data.get("service_fee", 0.0)
    fee = float(fee_val)

    conversion = max(0.0, total_payment - fee)
    receive = conversion * rate
    effective_rate = receive / total_payment if total_payment > 0 else 0.0

    return {
        "name": "MasterRemit",
        "method": "Standard",
        "rate": rate,
        "fee": fee,
        "conversion": conversion,
        "receive": receive,
        "total_payment": total_payment,
        "effective_rate": effective_rate,
        "rate_type": "Includes rate markup",
        "quote_time": parse_http_date(resp.headers.get("date", "")),
    }


# ============================================================
# Zimoney API
# ============================================================

ZIMONEY_API_URL = "https://zimoney.com/api/api/currency/convert"
ZIMONEY_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": "https://zimoney.com",
    "Referer": "https://zimoney.com/",
    "User-Agent": BROWSER_UA,
    # A generic CSRF token string to satisfy basic Laravel/Sanctum middleware
    # checks
    "X-CSRF-TOKEN": "dummy_csrf_token_for_public_api" 
}

# ============================================================
# Zimoney API
# ============================================================


ZIMONEY_API_URL = "https://zimoney.com/api/api/currency/convert"
ZIMONEY_HOME_URL = "https://zimoney.com/"

def query_zimoney(total_payment, source, target):
    """Queries Zimoney using curl_cffi session to bypass WAF and natively fetch CSRF tokens."""
    payload = {
        "from": source,
        "to": target,
        "amount": 1  # Request 1 unit to get the pure exchange rate
    }

    try:
        # Start a persistent Chrome session to keep cookies intact
        with cffi_requests.Session(impersonate="chrome110") as session:
            # 1. Fetch homepage to clear Cloudflare and get CSRF token/cookies
            home_resp = session.get(ZIMONEY_HOME_URL, timeout=15)
            
            # Extract CSRF token from HTML <meta name="csrf-token" content="...">
            csrf_token = ""
            match = re.search(r'<meta\s+name="csrf-token"\s+content="([^"]+)"', home_resp.text)
            if match:
                csrf_token = match.group(1)

            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Origin": "https://zimoney.com",
                "Referer": "https://zimoney.com/",
                "User-Agent": BROWSER_UA,
                "X-CSRF-TOKEN": csrf_token
            }
            
            # 2. Make the API POST request using the established session
            resp = session.post(ZIMONEY_API_URL, json=payload, headers=headers, timeout=15)
            res_json = resp.json()
            
            if not res_json.get("success"):
                return {"name": "Zimoney", "error": f"Corridor {source}->{target} unavailable"}
                
            data = res_json.get("data", {})
            if not data.get("success"):
                return {"name": "Zimoney", "error": "Conversion failed"}
                
            rate = float(data.get("info", {}).get("quote", 0.0))
            quote_time = parse_http_date(resp.headers.get("date", ""))
            
    except Exception as e:
        return {"name": "Zimoney", "error": f"API Error: {str(e)}"}

    if rate <= 0:
        return {"name": "Zimoney", "error": f"No valid rate for {source}->{target}"}

    # Process local Tiered Fee Structure
    fee = 0.0
    if total_payment >= 10001:
        fee = 0.0
    elif 4401 <= total_payment <= 10000:
        fee = total_payment * 0.02
    elif 1501 <= total_payment <= 4400:
        fee = total_payment * 0.03
    elif 51 <= total_payment <= 1500:
        fee = total_payment * 0.025
    elif 5 <= total_payment <= 50:
        fee = total_payment * 0.01
    elif 1 <= total_payment < 5:
        fee = total_payment * 0.02
    else:
        fee = 0.0

    conversion = max(0.0, total_payment - fee)
    receive = conversion * rate
    effective_rate = receive / total_payment if total_payment > 0 else 0.0

    return {
        "name": "Zimoney",
        "method": "Tiered Fee",
        "rate": rate,
        "fee": fee,
        "conversion": conversion,
        "receive": receive,
        "total_payment": total_payment,
        "effective_rate": effective_rate,
        "rate_type": "Includes rate markup",
        "quote_time": quote_time,
    }


# ============================================================
# Parallel Execution Engine
# ============================================================

MID_SOURCES = [
    ("Bank of China", query_chinamoney_mid_rate),
    ("AllRatesToday", query_allratestoday_mid_rate),
]

PLATFORM_SOURCES = [
    ("LemFi", query_lemfi),
    ("Remitly", query_remitly),
    ("Panda Remit", query_panda),
    ("Orbit Remit", query_orbit),
    ("WorldRemit", query_worldremit),
    ("TapTap Send", query_taptapsend),
    ("MasterRemit", query_masterremit),
    ("Zimoney", query_zimoney)
]


def run_queries(total_payment, source, target):
    """Executes queries concurrently to API sources."""
    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {}
        for name, fn in MID_SOURCES:
            futures[ex.submit(fn, source, target)] = name
        futures[ex.submit(query_wise, total_payment, source, target)] = "Wise"
        for name, fn in PLATFORM_SOURCES:
            futures[ex.submit(fn, total_payment, source, target)] = name

        for fut in as_completed(futures):
            name = futures[fut]
            try:
                result = fut.result()
                results[name] = result
                with _cache_lock:
                    _platform_cache[(name, source, target, total_payment)] = result
                    if len(_platform_cache) > 500:
                        _platform_cache.clear()
            except Exception as e:
                with _cache_lock:
                    cached = _platform_cache.get((name, source, target, total_payment))
                if cached:
                    results[name] = cached
                else:
                    results[name] = {"name": name, "error": str(e)}

    platform_order = ["Wise"] + [n for n, _ in PLATFORM_SOURCES if n in results]
    return [results[n] for n, _ in MID_SOURCES], [results[n] for n in platform_order]


# ============================================================
# Short-Term Caching
# ============================================================

CACHE_TTL = 45  # seconds
_cache = {}
_cache_lock = threading.Lock()
_last_successful_result = None
_platform_cache = {}

def get_cached(key, producer):
    now = time.time()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < CACHE_TTL:
            return hit[1]
    result = producer()
    with _cache_lock:
        _cache[key] = (now, result)
    return result

def build_result(amount, source, target):
    mid_rates, platform_results = run_queries(amount, source, target)
    ref_mid = max((mr["rate"] for mr in mid_rates if "error" not in mr), default=0)

    mid_market = []
    for mr in mid_rates:
        if "error" in mr:
            mid_market.append({"name": mr["name"], "error": mr["error"]})
        else:
            mid_market.append({
                "name": mr["name"],
                "rate": round(mr["rate"], 6),
                "bp": mr.get("bp"),
                "quoteTime": mr.get("quote_time", ""),
                "source": mr.get("source", ""),
            })

    platforms = []
    for r in platform_results:
        if "error" in r:
            platforms.append({"name": r["name"], "error": r["error"]})
            continue
        markup = (r["rate"] - ref_mid) / ref_mid * 100 if ref_mid > 0 else None
        platforms.append({
            "name": r["name"],
            "method": r.get("method", ""),
            "rate": round(r["rate"], 6),
            "fee": round(r["fee"], 2),
            "conversion": round(r["conversion"], 2),
            "receive": round(r["receive"], 2),
            "effectiveRate": round(r["effective_rate"], 6),
            "markupVsMidRate": round(markup, 2) if markup is not None else None,
            "rateType": r.get("rate_type", ""),
            "quoteTime": r.get("quote_time", ""),
        })

    return {
        "sourceCurrency": source,
        "targetCurrency": target,
        "totalPayment": amount,
        "midMarketRates": mid_market,
        "platforms": platforms,
        "queryTime": time.strftime("%Y-%m-%d %H:%M:%S"),
    }


# ============================================================
# Routing & API Endpoints
# ============================================================

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/compare")
def api_compare():
    host = request.headers.get("X-Forwarded-Host") or request.headers.get("Host", "")
    origin = request.headers.get("Origin", "")
    referer = request.headers.get("Referer", "")
    
    if not origin and not referer:
        return jsonify({"error": "Access Denied"}), 403
    if origin and host not in origin:
        return jsonify({"error": "Access Denied"}), 403
    if not origin and host not in referer:
        return jsonify({"error": "Access Denied"}), 403

    try:
        amount = float(request.args.get("amount", "1000"))
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid amount format"}), 400
        
    if amount <= 0 or amount > 1000000:
        return jsonify({"error": "Amount must be between 0 and 1,000,000"}), 400

    source = request.args.get("source", "AUD").upper()
    if source not in SOURCE_META:
        return jsonify({"error": f"Source currency {source} is unsupported"}), 400

    target = request.args.get("target", "CNY").upper()
    if target not in TARGET_META:
        return jsonify({"error": f"Target currency {target} is unsupported"}), 400

    global _last_successful_result

    try:
        payload = get_cached(
            ("compare", amount, source, target),
            lambda: build_result(amount, source, target),
        )
        _last_successful_result = payload
    except Exception as e:
        if (
            _last_successful_result
            and _last_successful_result.get("sourceCurrency") == source
            and _last_successful_result.get("targetCurrency") == target
            and _last_successful_result.get("totalPayment") == amount
        ):
            return jsonify(_last_successful_result)
        return jsonify({"error": f"Query failed: {e}"}), 500
        
    return jsonify(payload)

LOGO_WHITELIST = {"wise_logo.svg", "remitly_log.svg", "lemfi_logo.svg", "pandaRemit_logo.png", "orbitremit-logo.svg"}

@app.route("/logo/<path:name>")
def logo(name):
    if name not in LOGO_WHITELIST:
        return "Not Found", 404
    return send_from_directory(BASE_DIR, name)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remittance Compare Local Web Service")
    parser.add_argument("--port", type=int, default=5000, help="Listen Port (Default 5000)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Listen Address")
    args = parser.parse_args()

    print(f"Local Service Started: http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop")
    app.run(host=args.host, port=args.port, debug=False)
