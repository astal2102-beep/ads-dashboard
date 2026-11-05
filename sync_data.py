#!/usr/bin/env python3
"""
Meta Ads + Google Ads -> Google Sheets 多層級數據同步腳本

- 抓取過去 N 天（含今天，預設 7 天）的 Account / Campaign / AdGroup / Ad 四層數據
- 統一欄位、在 Python 中計算 CTR / CPC / ROAS / CVR / CPA
- 以 Date + Platform + Level + Campaign_Name + AdGroup_Name + Ad_Name 為唯一鍵 Upsert
- 各層級寫入獨立分頁：Raw_Account / Raw_Campaign / Raw_AdGroup / Raw_Ad

所有憑證皆由環境變數讀取（GitHub Actions 由 Secrets 注入）。
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
import numpy as np
import pandas as pd
import requests
from gspread.utils import ValueInputOption, ValueRenderOption

# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ads_sync")

TZ = ZoneInfo(os.getenv("REPORT_TIMEZONE", "Asia/Taipei"))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS") or "7")

# Ad 層級同名廣告的合併範圍：
#   global   -> 不分廣告活動/群組，只要廣告名稱相同就合併（Campaign_Name、AdGroup_Name 填 "All"）
#   campaign -> 同一廣告活動內、不同廣告群組的同名廣告合併（AdGroup_Name 填 "All"）
AD_MERGE_SCOPE = os.getenv("AD_MERGE_SCOPE", "global").strip().lower()

# Meta 轉換事件優先順序（取第一個存在的，避免 omni_purchase 與 purchase 重複加總）
META_PURCHASE_TYPES = [
    t.strip()
    for t in os.getenv(
        "META_PURCHASE_ACTION_TYPES",
        "omni_purchase,purchase,offsite_conversion.fb_pixel_purchase",
    ).split(",")
    if t.strip()
]
META_API_VERSION = os.getenv("META_API_VERSION", "v23.0")

SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")

DIM_COLS = ["Date", "Platform", "Level", "Campaign_Name", "AdGroup_Name", "Ad_Name"]
BASE_METRICS = ["Spend", "Impressions", "Clicks", "Conversions", "Conversion_Value"]
COLUMNS = DIM_COLS + [
    "Spend", "Impressions", "Clicks", "CTR", "CPC",
    "Conversions", "Conversion_Value", "ROAS", "CVR", "CPA",
]
LEVEL_TO_SHEET = {
    "Account": "Raw_Account",
    "Campaign": "Raw_Campaign",
    "AdGroup": "Raw_AdGroup",
    "Ad": "Raw_Ad",
}
UNNAMED = "(未命名)"
WRITE_CHUNK_ROWS = 20000


def _split_ids(value: str) -> list[str]:
    return [v.strip() for v in (value or "").split(",") if v.strip()]


def _record(date, platform, level, campaign="All", adgroup="All", ad="All",
            spend=0.0, impressions=0, clicks=0, conversions=0.0, value=0.0) -> dict:
    return {
        "Date": date,
        "Platform": platform,
        "Level": level,
        "Campaign_Name": campaign,
        "AdGroup_Name": adgroup,
        "Ad_Name": ad,
        "Spend": float(spend or 0),
        "Impressions": int(float(impressions or 0)),
        "Clicks": int(float(clicks or 0)),
        "Conversions": float(conversions or 0),
        "Conversion_Value": float(value or 0),
    }


# ---------------------------------------------------------------------------
# Meta Ads
# ---------------------------------------------------------------------------
META_LEVELS = {
    "Account": ("account", []),
    "Campaign": ("campaign", ["campaign_name"]),
    "AdGroup": ("adset", ["campaign_name", "adset_name"]),
    "Ad": ("ad", ["campaign_name", "adset_name", "ad_name"]),
}
META_BASE_FIELDS = ["spend", "impressions", "inline_link_clicks", "actions", "action_values"]
# 可重試的錯誤碼（rate limit / 暫時性錯誤）
META_RETRY_CODES = {1, 2, 4, 17, 32, 341, 613, 80000, 80003, 80004}


def _meta_get(url: str, params: dict | None) -> dict:
    max_attempts = 6
    for attempt in range(max_attempts):
        try:
            resp = requests.get(url, params=params, timeout=180)
        except requests.RequestException as exc:
            if attempt == max_attempts - 1:
                raise
            wait = min(300, 15 * 2 ** attempt)
            log.warning("Meta 連線錯誤 %s，%ss 後重試", exc, wait)
            time.sleep(wait)
            continue

        if resp.status_code == 200:
            return resp.json()

        try:
            err = resp.json().get("error", {})
        except ValueError:
            err = {}
        code = err.get("code")
        retryable = resp.status_code >= 500 or code in META_RETRY_CODES
        if not retryable or attempt == max_attempts - 1:
            raise RuntimeError(f"Meta API 錯誤 HTTP {resp.status_code}: {resp.text[:800]}")
        wait = min(300, 15 * 2 ** attempt)
        log.warning("Meta API 暫時性錯誤 (code=%s)，%ss 後重試", code, wait)
        time.sleep(wait)
    raise RuntimeError("Meta API 重試次數用盡")


def _meta_pick(action_list) -> float:
    if not action_list:
        return 0.0
    values = {a.get("action_type"): a.get("value") for a in action_list}
    for action_type in META_PURCHASE_TYPES:
        if action_type in values:
            return float(values[action_type] or 0)
    return 0.0


def fetch_meta(since: str, until: str) -> list[dict]:
    token = os.getenv("META_ACCESS_TOKEN", "")
    account_ids = _split_ids(os.getenv("META_AD_ACCOUNT_IDS", ""))
    records: list[dict] = []

    for raw_id in account_ids:
        act_id = raw_id if raw_id.startswith("act_") else f"act_{raw_id}"
        for level_name, (api_level, extra_fields) in META_LEVELS.items():
            url = f"https://graph.facebook.com/{META_API_VERSION}/{act_id}/insights"
            params = {
                "access_token": token,
                "level": api_level,
                "fields": ",".join(extra_fields + META_BASE_FIELDS),
                "time_range": json.dumps({"since": since, "until": until}),
                "time_increment": 1,
                "limit": 500,
                "use_unified_attribution_setting": "true",
            }
            count = 0
            while url:
                payload = _meta_get(url, params)
                for row in payload.get("data", []):
                    records.append(_record(
                        date=row.get("date_start"),
                        platform="Meta Ads",
                        level=level_name,
                        campaign=row.get("campaign_name", "All") if level_name != "Account" else "All",
                        adgroup=row.get("adset_name", "All") if level_name in ("AdGroup", "Ad") else "All",
                        ad=row.get("ad_name", "All") if level_name == "Ad" else "All",
                        spend=row.get("spend"),
                        impressions=row.get("impressions"),
                        clicks=row.get("inline_link_clicks"),
                        conversions=_meta_pick(row.get("actions")),
                        value=_meta_pick(row.get("action_values")),
                    ))
                    count += 1
                url = payload.get("paging", {}).get("next")
                params = None  # next URL 已含完整參數
            log.info("Meta %s %-8s 取得 %d 筆", act_id, level_name, count)
    return records


# ---------------------------------------------------------------------------
# Google Ads
# ---------------------------------------------------------------------------
_GADS_METRICS = (
    "metrics.cost_micros, metrics.impressions, metrics.clicks, "
    "metrics.conversions, metrics.conversions_value"
)
GADS_QUERIES = {
    "Account": f"SELECT segments.date, {_GADS_METRICS} FROM customer "
               "WHERE segments.date BETWEEN '{since}' AND '{until}'",
    "Campaign": f"SELECT segments.date, campaign.name, {_GADS_METRICS} FROM campaign "
                "WHERE segments.date BETWEEN '{since}' AND '{until}'",
    "AdGroup": f"SELECT segments.date, campaign.name, ad_group.name, {_GADS_METRICS} FROM ad_group "
               "WHERE segments.date BETWEEN '{since}' AND '{until}'",
    "Ad": f"SELECT segments.date, campaign.name, ad_group.name, ad_group_ad.ad.id, "
          f"ad_group_ad.ad.name, {_GADS_METRICS} FROM ad_group_ad "
          "WHERE segments.date BETWEEN '{since}' AND '{until}'",
}


def fetch_google(since: str, until: str) -> list[dict]:
    # 延遲匯入：只用 Meta 時不需要 Google Ads 憑證
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads.errors import GoogleAdsException

    config = {
        "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
        "client_id": os.environ["GOOGLE_ADS_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_ADS_CLIENT_SECRET"],
        "refresh_token": os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
        "use_proto_plus": True,
    }
    login_cid = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "").replace("-", "").strip()
    if login_cid:
        config["login_customer_id"] = login_cid

    client = GoogleAdsClient.load_from_dict(config)
    service = client.get_service("GoogleAdsService")
    customer_ids = [c.replace("-", "") for c in _split_ids(os.getenv("GOOGLE_ADS_CUSTOMER_IDS", ""))]
    records: list[dict] = []

    for cid in customer_ids:
        for level_name, template in GADS_QUERIES.items():
            query = template.format(since=since, until=until)
            count = 0
            try:
                stream = service.search_stream(customer_id=cid, query=query)
                for batch in stream:
                    for row in batch.results:
                        m = row.metrics
                        campaign = row.campaign.name if level_name != "Account" else "All"
                        adgroup = row.ad_group.name if level_name in ("AdGroup", "Ad") else "All"
                        ad = "All"
                        if level_name == "Ad":
                            # 回應式搜尋廣告等通常沒有名稱，以 ad id 區分避免被錯誤合併
                            ad = row.ad_group_ad.ad.name or f"{UNNAMED} ad_id:{row.ad_group_ad.ad.id}"
                        records.append(_record(
                            date=row.segments.date,
                            platform="Google Ads",
                            level=level_name,
                            campaign=campaign,
                            adgroup=adgroup,
                            ad=ad,
                            spend=m.cost_micros / 1_000_000,
                            impressions=m.impressions,
                            clicks=m.clicks,
                            conversions=m.conversions,
                            value=m.conversions_value,
                        ))
                        count += 1
            except GoogleAdsException as exc:
                details = "; ".join(e.message for e in exc.failure.errors)
                raise RuntimeError(
                    f"Google Ads API 錯誤 (customer {cid}, {level_name}): {details} "
                    f"[request_id={exc.request_id}]"
                ) from exc
            log.info("Google %s %-8s 取得 %d 筆", cid, level_name, count)
    return records


# ---------------------------------------------------------------------------
# 彙整與指標計算
# ---------------------------------------------------------------------------
def _ratio(numerator: pd.Series, denominator: pd.Series, ndigits: int) -> pd.Series:
    return (numerator / denominator.where(denominator != 0)).round(ndigits)


def build_frame(records: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(records, columns=DIM_COLS + BASE_METRICS)
    if df.empty:
        return pd.DataFrame(columns=COLUMNS)

    for col in DIM_COLS:
        df[col] = df[col].fillna("").astype(str).str.strip().replace("", UNNAMED)

    # Ad 層級：依廣告名稱合併跨廣告群組的同名廣告
    ad_mask = df["Level"] == "Ad"
    if AD_MERGE_SCOPE == "global":
        df.loc[ad_mask, ["Campaign_Name", "AdGroup_Name"]] = "All"
    elif AD_MERGE_SCOPE == "campaign":
        df.loc[ad_mask, "AdGroup_Name"] = "All"
    else:
        raise ValueError(f"AD_MERGE_SCOPE 不支援：{AD_MERGE_SCOPE}（可用 global / campaign）")

    # 同鍵值加總（同時處理同名廣告合併、多帳號同名實體）
    df = df.groupby(DIM_COLS, as_index=False)[BASE_METRICS].sum()

    df["Spend"] = df["Spend"].round(2)
    df["Conversions"] = df["Conversions"].round(2)
    df["Conversion_Value"] = df["Conversion_Value"].round(2)
    df["Impressions"] = df["Impressions"].astype(int)
    df["Clicks"] = df["Clicks"].astype(int)

    df["CTR"] = _ratio(df["Clicks"], df["Impressions"], 6)          # 小數，Sheets 可設為百分比格式
    df["CPC"] = _ratio(df["Spend"], df["Clicks"], 2)
    df["ROAS"] = _ratio(df["Conversion_Value"], df["Spend"], 4)
    df["CVR"] = _ratio(df["Conversions"], df["Clicks"], 6)
    df["CPA"] = _ratio(df["Spend"], df["Conversions"], 2)

    # 全為 0 的列（無花費、無曝光）不寫入，避免表格膨脹
    df = df[(df["Spend"] != 0) | (df["Impressions"] != 0) | (df["Conversions"] != 0)]
    return df[COLUMNS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Google Sheets Upsert
# ---------------------------------------------------------------------------
def get_gspread_client() -> gspread.Client:
    raw = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "").strip()
    if raw:
        return gspread.service_account_from_dict(json.loads(raw))
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
    return gspread.service_account(filename=path)


def _get_or_create_ws(sh: gspread.Spreadsheet, title: str) -> gspread.Worksheet:
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=2, cols=len(COLUMNS))
        ws.update(values=[COLUMNS], range_name="A1", value_input_option=ValueInputOption.raw)
        ws.freeze(rows=1)
        log.info("已建立分頁 %s", title)
        return ws


def _norm_date(value) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # 若有人手動把欄位轉成日期格式，UNFORMATTED_VALUE 會回傳序列值
        return (datetime(1899, 12, 30) + timedelta(days=float(value))).strftime("%Y-%m-%d")
    text = str(value).strip()
    try:
        return pd.to_datetime(text).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return text


def _to_cell(value):
    if value is None:
        return ""
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return "" if np.isnan(value) else float(value)
    if isinstance(value, float) and np.isnan(value):
        return ""
    return value


def _key_series(df: pd.DataFrame) -> pd.Series:
    return df[DIM_COLS].astype(str).agg("\x1f".join, axis=1)


def upsert_sheet(sh: gspread.Spreadsheet, title: str, new_df: pd.DataFrame) -> None:
    ws = _get_or_create_ws(sh, title)
    raw = ws.get_values(value_render_option=ValueRenderOption.unformatted)

    if len(raw) > 1:
        header = [str(h).strip() for h in raw[0]]
        body = [row + [""] * (len(header) - len(row)) for row in raw[1:]]
        old = pd.DataFrame(body, columns=header).reindex(columns=COLUMNS)
        old = old[old["Date"].astype(str).str.strip() != ""].copy()
        old["Date"] = old["Date"].map(_norm_date)
        for col in DIM_COLS:
            old[col] = old[col].fillna("").astype(str).str.strip()
    else:
        old = pd.DataFrame(columns=COLUMNS)

    new_keys = set(_key_series(new_df))
    if not old.empty:
        old_keys = _key_series(old)
        hit = old_keys.isin(new_keys)
        kept = old[~hit]
        updated = int(hit.sum())
    else:
        kept, updated = old, 0
    inserted = len(new_keys) - updated

    frames = [f for f in (kept, new_df[COLUMNS]) if not f.empty]
    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMNS)
    merged = merged.sort_values(
        ["Date", "Platform", "Campaign_Name", "AdGroup_Name", "Ad_Name"],
        ascending=[False, True, True, True, True],
        kind="stable",
    )

    rows = [[_to_cell(v) for v in r] for r in merged.astype(object).itertuples(index=False, name=None)]
    values = [COLUMNS] + rows

    ws.resize(rows=max(len(values), 2), cols=len(COLUMNS))
    for start in range(0, len(values), WRITE_CHUNK_ROWS):
        chunk = values[start:start + WRITE_CHUNK_ROWS]
        ws.update(values=chunk, range_name=f"A{start + 1}", value_input_option=ValueInputOption.raw)

    log.info("分頁 %-13s 更新 %d 筆、新增 %d 筆，總計 %d 筆", title, updated, inserted, len(rows))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    if not SHEET_ID:
        log.error("缺少 GOOGLE_SHEET_ID")
        return 1

    today = datetime.now(TZ).date()
    since = (today - timedelta(days=LOOKBACK_DAYS - 1)).isoformat()
    until = today.isoformat()
    log.info("同步區間 %s ~ %s（%s，%d 天），Ad 合併範圍：%s",
             since, until, TZ.key, LOOKBACK_DAYS, AD_MERGE_SCOPE)

    records: list[dict] = []
    errors: list[str] = []

    if os.getenv("META_ACCESS_TOKEN") and os.getenv("META_AD_ACCOUNT_IDS"):
        try:
            records += fetch_meta(since, until)
        except Exception as exc:  # 單一平台失敗不影響另一平台寫入
            log.exception("Meta Ads 抓取失敗")
            errors.append(f"Meta: {exc}")
    else:
        log.warning("未設定 Meta 憑證，略過 Meta Ads")

    if os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN") and os.getenv("GOOGLE_ADS_CUSTOMER_IDS"):
        try:
            records += fetch_google(since, until)
        except Exception as exc:
            log.exception("Google Ads 抓取失敗")
            errors.append(f"Google: {exc}")
    else:
        log.warning("未設定 Google Ads 憑證，略過 Google Ads")

    df = build_frame(records)
    log.info("彙整後共 %d 筆", len(df))

    if not df.empty:
        sh = get_gspread_client().open_by_key(SHEET_ID)
        for level, title in LEVEL_TO_SHEET.items():
            sub = df[df["Level"] == level]
            if sub.empty:
                log.info("分頁 %s 無新資料，略過", title)
                continue
            upsert_sheet(sh, title, sub)

    if errors:
        log.error("部分平台失敗：%s", " | ".join(errors))
        return 1
    log.info("同步完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
