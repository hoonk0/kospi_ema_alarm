# -*- coding: utf-8 -*-
"""
==============================================================================
 텔레그램 대화형 주식 EMA(지수이동평균) 알림 봇 (KOSPI / KOSDAQ)
==============================================================================

[기능 요약]
 - 코스피 지수, 섹터 대표주, 코스피 시총 1~30위, 코스닥 시총 1~5위 감시
 - 일봉(5/20/60/120), 주봉(5/20) **EMA** ±0.5% 진입 시 텔레그램 알람
 - 상승 돌파 / 하락 돌파 구분 (각 방향당 하루 1회)
 - 사람별 개인 워치리스트 : 각자 /watch 로 켠 종목 알람을 자기만 받음
 - /watch 인라인 버튼으로 종목 켜기✅/끄기⬜ (모두 켜기/끄기 지원)
 - python-telegram-bot 의 JobQueue 로 장중/장마감 주기 체크

[설치]
    pip install "python-telegram-bot[job-queue]" finance-datareader pandas pytz holidays

    # (선택) FinanceDataReader 가 코스피 종목 메타 조회가 막혀있을 때 백업용
    pip install pykrx

[실행 전 준비]
 1) @BotFather 에서 봇을 만들고 토큰 발급
 2) 본인 챗ID 확인 ( @userinfobot 등 사용 )
 3) 환경변수 설정:
       export TELEGRAM_BOT_TOKEN="발급받은_토큰"
       export TELEGRAM_CHAT_ID="본인_챗ID"
    또는 아래 CONFIG 섹션의 기본값을 직접 수정하세요.

[실행]
    python stock_alarm_bot.py

==============================================================================
"""

import os
import json
import logging
import asyncio
import socket
from datetime import datetime, timedelta, time as dtime
from typing import Dict, List, Optional, Tuple

# ----------------------------------------------------------------------------
# 네트워크: IPv4 우선 사용
#   일부 회선(예: 일부 라즈베리파이 환경)에서는 텔레그램 서버로 가는 IPv6 경로가
#   매우 느려(연결에만 5초 이상) 기본 타임아웃에 걸려 봇이 뜨지 않습니다.
#   getaddrinfo 결과에서 IPv4 주소를 앞으로 정렬해 IPv4 로 먼저 붙도록 합니다.
#   (IPv4 가 없으면 IPv6 로 자연스럽게 폴백)
# ----------------------------------------------------------------------------
_orig_getaddrinfo = socket.getaddrinfo


def _prefer_ipv4_getaddrinfo(*args, **kwargs):
    results = _orig_getaddrinfo(*args, **kwargs)
    results.sort(key=lambda r: 0 if r[0] == socket.AF_INET else 1)
    return results


socket.getaddrinfo = _prefer_ipv4_getaddrinfo

import pandas as pd
import pytz

# 한국 공휴일(휴장일) 판정
import holidays as pyholidays

# 데이터 수집: FinanceDataReader 사용 (간편함, 무료, 한국주식 잘 지원)
import FinanceDataReader as fdr

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)


# ============================================================================
# CONFIG : 환경변수 또는 직접 수정
# ============================================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "여기에_봇_토큰_입력")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "여기에_챗ID_입력")

# (참고) 이 봇은 '사람별 개인 워치리스트' 방식입니다.
#   - 누구든 봇에게 /start 후 /watch 로 종목을 켜면, 그 사람에게만 알람이 갑니다.
#   - 따로 수신자 목록을 관리할 필요 없이, 종목을 켠 사람이 곧 수신자입니다.
#   TELEGRAM_CHAT_ID 는 실행 설정이 됐는지 확인(검증)하는 용도로만 남겨둡니다.

# 이평선 ±N% 진입 시 알람 (요구사항: 0.5%)
TOUCH_THRESHOLD_PCT = 0.5

# 알람 기한 설정 정보를 저장할 파일
WATCHLIST_FILE = "watchlist.json"

# 같은 종목/같은 이평선 알람이 하루에 여러 번 가지 않도록 중복 방지
SENT_LOG_FILE = "sent_today.json"

# 사용자별로 '어떤 EMA 라인 알람을 받을지' 저장하는 파일
EMA_PREF_FILE = "ema_prefs.json"

# 한국 시간대
KST = pytz.timezone("Asia/Seoul")

# 체크할 지수이동평균(EMA) 정의
#   - 단순이동평균(SMA)이 아닌 지수이동평균(EMA) 기준으로 계산합니다.
#   - EMA 는 최근 봉에 더 큰 가중치를 부여하여 추세 변화에 빠르게 반응합니다.
DAILY_EMAS = [5, 20, 60, 120]      # 일봉 EMA (선택 가능한 전체 후보)
WEEKLY_EMAS = [5, 20]               # 주봉 EMA (5주, 20주)

# 한국 공휴일 객체 (연도 접근 시 자동 계산, 대체공휴일 포함)
_KR_HOLIDAYS = pyholidays.SouthKorea()


# ============================================================================
# 감시 대상 종목 정의
#   - key   : FinanceDataReader 에서 사용하는 코드 (지수는 'KS11' 등)
#   - value : 사람이 알아볼 종목명
#
#   ▶ 종목 추가는 아래 딕셔너리에 한 줄 추가만 하면 됩니다.
#     예) "012345": "추가종목명",
#   ▶ 지수는 fdr 코드(KS11=코스피, KQ11=코스닥) 그대로 사용합니다.
# ============================================================================
WATCH_TARGETS: Dict[str, str] = {
    # ----- 지수 -----
    "KS11": "코스피지수",
    "KQ11": "코스닥지수",

    # ----- 섹터별 대표주 (필요 시 자유롭게 수정/추가) -----
    "005930": "삼성전자(반도체)",
    "000660": "SK하이닉스(반도체)",
    "005380": "현대차(자동차)",
    "051910": "LG화학(2차전지/화학)",
    "005490": "POSCO홀딩스(철강)",
    "035420": "NAVER(인터넷)",
    "035720": "카카오(인터넷)",
    "055550": "신한지주(금융)",
    "105560": "KB금융(금융)",
    "068270": "셀트리온(바이오)",
    "207940": "삼성바이오로직스(바이오)",
    "017670": "SK텔레콤(통신)",
    "030200": "KT(통신)",
    "015760": "한국전력(유틸리티)",
    "009150": "삼성전기(전자부품)",
    "012330": "현대모비스(자동차부품)",
    "034730": "SK(지주)",
    "010130": "고려아연(비철)",
    "096770": "SK이노베이션(정유)",
    "267260": "HD현대일렉트릭(전력기기)",

    # ----- 코스피 시총 상위 (대략) : 위 섹터주와 중복 시 자동 dedupe -----
    "000270": "기아",
    "373220": "LG에너지솔루션",
    "006400": "삼성SDI",
    "066570": "LG전자",
    "003550": "LG",
    "032830": "삼성생명",
    "086790": "하나금융지주",
    "316140": "우리금융지주",
    "018260": "삼성에스디에스",
    "011200": "HMM",

    # ----- 코스닥 시총 상위 5 (대략) -----
    "247540": "에코프로비엠",
    "086520": "에코프로",
    "091990": "셀트리온헬스케어",   # 합병 이슈 시 종목코드 확인 필요
    "196170": "알테오젠",
    "041510": "에스엠",
}


# ============================================================================
# 로깅 설정
# ============================================================================
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("stock-alarm-bot")


# ============================================================================
# 영속 저장소 (워치리스트 / 일간 중복 알람 방지)
# ============================================================================
def load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"{path} 로드 실패: {e}")
        return default


def save_json(path: str, data) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_all_watchlists() -> Dict[str, Dict[str, Dict]]:
    """
    사람별 개인 워치리스트. 반환 구조:
    {
      "1567993608": {                                  # 챗ID
          "005930": {"name": "삼성전자", "expire": "2026-06-01" or null}
      },
      "8848174466": { ... }
    }
    expire 가 None 이면 무제한 감시
    """
    return load_json(WATCHLIST_FILE, {})


def save_all_watchlists(data: Dict[str, Dict[str, Dict]]) -> None:
    save_json(WATCHLIST_FILE, data)


def get_user_watchlist(chat_id) -> Dict[str, Dict]:
    """특정 사용자(챗ID)의 워치리스트 {종목코드: 정보} 반환 (없으면 빈 dict)."""
    return load_all_watchlists().get(str(chat_id), {})


def save_user_watchlist(chat_id, wl: Dict[str, Dict]) -> None:
    """특정 사용자의 워치리스트를 저장. 빈 dict 면 해당 사용자 항목을 제거."""
    data = load_all_watchlists()
    if wl:
        data[str(chat_id)] = wl
    else:
        data.pop(str(chat_id), None)
    save_all_watchlists(data)


def load_sent_log() -> Dict[str, List[str]]:
    """오늘 날짜에 이미 보낸 (종목,MA) 키 기록. 날짜가 바뀌면 초기화."""
    data = load_json(SENT_LOG_FILE, {"date": "", "keys": []})
    today_str = datetime.now(KST).strftime("%Y-%m-%d")
    if data.get("date") != today_str:
        data = {"date": today_str, "keys": []}
        save_json(SENT_LOG_FILE, data)
    return data


def save_sent_log(data: Dict) -> None:
    save_json(SENT_LOG_FILE, data)


def already_sent_today(chat_id, code: str, ma_label: str) -> bool:
    """사용자별로 중복 방지. (같은 알람이라도 사람마다 따로 1회씩 발송)"""
    data = load_sent_log()
    return f"{chat_id}:{code}:{ma_label}" in data["keys"]


def mark_sent_today(chat_id, code: str, ma_label: str) -> None:
    data = load_sent_log()
    key = f"{chat_id}:{code}:{ma_label}"
    if key not in data["keys"]:
        data["keys"].append(key)
        save_sent_log(data)


# ----------------------------------------------------------------------------
# 사용자별 EMA 라인 선택 (6개 중 받고 싶은 것만)
#   저장 구조: { "챗ID": {"daily": [5,20,...], "weekly": [5,20]} }
#   설정이 없으면 기본값 = 전체 ON
# ----------------------------------------------------------------------------
def get_user_ema_pref(chat_id) -> Dict[str, List[int]]:
    prefs = load_json(EMA_PREF_FILE, {}).get(str(chat_id))
    if not prefs:
        return {"daily": list(DAILY_EMAS), "weekly": list(WEEKLY_EMAS)}
    return {
        "daily": prefs.get("daily", list(DAILY_EMAS)),
        "weekly": prefs.get("weekly", list(WEEKLY_EMAS)),
    }


def save_user_ema_pref(chat_id, pref: Dict[str, List[int]]) -> None:
    data = load_json(EMA_PREF_FILE, {})
    data[str(chat_id)] = pref
    save_json(EMA_PREF_FILE, data)


# ============================================================================
# 워치리스트 방식 메모 (opt-in, 사람별)
#   - 기본은 '빈 목록' : 아무도/아무 종목도 감시하지 않음
#   - 각 사용자가 /watch 로 켠 종목만 그 사람 워치리스트에 들어가고, 그 사람만 받음
#   - WATCH_TARGETS 는 '고를 수 있는 전체 후보 목록(카탈로그)' 역할만 함
# ============================================================================


# ============================================================================
# 주가 데이터 조회 & 이평선 계산
# ============================================================================
def fetch_price_df(code: str, days: int = 400) -> Optional[pd.DataFrame]:
    """
    최근 days 일 분량의 OHLCV 데이터를 가져온다.
    주봉 20주선까지 계산하려면 약 100영업일 + 여유분이면 충분(400일이면 넉넉).
    """
    end = datetime.now(KST).date()
    start = end - timedelta(days=days)
    try:
        df = fdr.DataReader(code, start, end)
        if df is None or df.empty:
            return None
        # 컬럼 표준화
        df = df.rename(columns=str.capitalize)
        return df
    except Exception as e:
        logger.warning(f"[{code}] 데이터 조회 실패: {e}")
        return None


def compute_daily_emas(df: pd.DataFrame, windows: List[int]) -> Dict[int, pd.Series]:
    """일봉 종가 기준 지수이동평균(EMA) 시계열 반환

    pandas 의 ewm(span=w, adjust=False) 는 일반적인 차트 도구(HTS, TradingView 등)
    가 사용하는 EMA 정의와 동일합니다.
        alpha = 2 / (span + 1)

    돌파 방향(상승/하락) 판정을 위해 마지막 값만이 아니라 시계열 전체를 반환합니다.
    """
    result: Dict[int, pd.Series] = {}
    close = df["Close"]
    for w in windows:
        if len(close) >= w:
            result[w] = close.ewm(span=w, adjust=False).mean()
    return result


def compute_weekly_emas(df: pd.DataFrame, windows: List[int]) -> Dict[int, pd.Series]:
    """주봉(금요일 종가) 기준 지수이동평균(EMA) 시계열 반환"""
    weekly_close = df["Close"].resample("W-FRI").last().dropna()
    result: Dict[int, pd.Series] = {}
    for w in windows:
        if len(weekly_close) >= w:
            result[w] = weekly_close.ewm(span=w, adjust=False).mean()
    return result


def detect_breakout(
    prev_close: float,
    prev_ema: float,
    curr_close: float,
    curr_ema: float,
    threshold_pct: float,
) -> Optional[str]:
    """현재 종가가 EMA ±threshold_pct% 범위 내일 때,
    직전 종가의 위치를 보고 돌파 방향을 판정한다.

    Returns:
        "상승"  - 직전엔 EMA 아래(밴드 밖)였다가 오늘 EMA 근처/위로 진입
        "하락"  - 직전엔 EMA 위(밴드 밖)였다가 오늘 EMA 근처/아래로 진입
        None    - 닿지 않았거나, 직전에도 이미 밴드 안이라 새 돌파가 아님
    """
    if curr_ema <= 0 or prev_ema <= 0:
        return None

    band = threshold_pct / 100.0
    # 1) 현재가가 ±threshold% 밴드 안에 있어야 함
    if abs(curr_close - curr_ema) / curr_ema > band:
        return None

    # 2) 직전 종가의 위치로 방향 판정
    if prev_close < prev_ema * (1 - band):
        return "상승"   # 아래에서 위로 올라와 EMA 에 도달
    if prev_close > prev_ema * (1 + band):
        return "하락"   # 위에서 아래로 내려와 EMA 에 도달

    return None  # 직전에도 밴드 안 → 신규 돌파 아님


# ============================================================================
# 종목 검색 (전체 KRX 목록에서 이름/코드로 찾기) — /add 용
# ============================================================================
_stock_listing_cache: Optional[Dict[str, str]] = None


def get_stock_listing() -> Dict[str, str]:
    """KRX 전체 종목 {코드: 이름}. 최초 1회 조회 후 메모리 캐시."""
    global _stock_listing_cache
    if _stock_listing_cache is not None:
        return _stock_listing_cache
    try:
        df = fdr.StockListing("KRX")
    except Exception as e:
        logger.warning(f"종목 목록 조회 실패: {e}")
        return {}
    mapping: Dict[str, str] = {}
    if "Code" in df.columns and "Name" in df.columns:
        for code, name in zip(df["Code"], df["Name"]):
            code = str(code).strip()
            name = str(name).strip()
            if code.isdigit() and name:
                mapping[code.zfill(6)] = name
    _stock_listing_cache = mapping
    return mapping


def search_stocks(query: str) -> List[Tuple[str, str]]:
    """이름(부분일치) 또는 6자리 코드로 종목 검색 → [(코드, 이름)] (최대 20개)."""
    listing = get_stock_listing()
    q = query.strip()
    if not q or not listing:
        return []
    if q.isdigit():
        code = q.zfill(6)
        return [(code, listing[code])] if code in listing else []
    ql = q.lower()
    matches = [(c, n) for c, n in listing.items() if ql in n.lower()]
    # 이름 정확히 일치 우선, 그다음 이름 짧은 순
    matches.sort(key=lambda cn: (cn[1].lower() != ql, len(cn[1])))
    return matches[:20]


# ============================================================================
# 핵심 체크 로직 : 한 종목의 돌파 알람 계산
#   - 사용자와 무관하게 '이 종목이 지금 어떤 EMA 를 돌파했는지'만 계산한다.
#   - 중복방지/발송/EMA라인 필터는 호출하는 쪽(scheduled_check)에서 사용자별로 처리.
#   - 반환: [(kind, w, sent_key, 메시지), ...]
#       kind = "daily" | "weekly", w = 기간(int)
#       sent_key = "라벨|방향" (예: "일봉 EMA20|상승")
# ============================================================================
def compute_alerts_for_code(code: str, name: str) -> List[Tuple[str, int, str, str]]:
    df = fetch_price_df(code, days=400)
    if df is None or df.empty:
        return []

    # 직전/현재 종가 (일봉)
    close = df["Close"]
    if len(close) < 2:
        return []
    last_close = float(close.iloc[-1])
    prev_close = float(close.iloc[-2])

    daily_emas = compute_daily_emas(df, DAILY_EMAS)
    weekly_emas = compute_weekly_emas(df, WEEKLY_EMAS)

    alerts: List[Tuple[str, int, str, str]] = []

    def fmt_msg(ema_label: str, ema_val: float, direction: str) -> str:
        diff_pct = (last_close - ema_val) / ema_val * 100.0
        if direction == "상승":
            head = f"🚀 *{name}* (`{code}`) {ema_label} *상승 돌파*"
        else:
            head = f"📉 *{name}* (`{code}`) {ema_label} *하락 돌파*"
        return (
            f"{head}\n"
            f"   - 현재가: {last_close:,.2f}\n"
            f"   - {ema_label}: {ema_val:,.2f}\n"
            f"   - 이격도: {diff_pct:+.2f}%"
        )

    # ----- 일봉 EMA 돌파 체크 -----
    for w, ema_series in daily_emas.items():
        if len(ema_series) < 2:
            continue
        curr_ema = float(ema_series.iloc[-1])
        prev_ema = float(ema_series.iloc[-2])
        direction = detect_breakout(
            prev_close, prev_ema, last_close, curr_ema, TOUCH_THRESHOLD_PCT
        )
        if direction is None:
            continue
        label = f"일봉 EMA{w}"
        alerts.append(("daily", w, f"{label}|{direction}", fmt_msg(label, curr_ema, direction)))

    # ----- 주봉 EMA 돌파 체크 -----
    # 주봉 EMA 는 주중에는 값이 거의 고정되므로, 일일 종가 변화로 돌파 여부를 판정한다.
    for w, ema_series in weekly_emas.items():
        if len(ema_series) < 1:
            continue
        curr_ema = float(ema_series.iloc[-1])
        # 주봉 EMA 와 비교할 때는 일봉 직전/현재 종가를 사용 (실시간성 확보)
        direction = detect_breakout(
            prev_close, curr_ema, last_close, curr_ema, TOUCH_THRESHOLD_PCT
        )
        if direction is None:
            continue
        label = f"주봉 EMA{w}"
        alerts.append(("weekly", w, f"{label}|{direction}", fmt_msg(label, curr_ema, direction)))

    return alerts


# ============================================================================
# 만료된 워치리스트 정리 & 사용자별 활성 종목 반환
#   반환: { 챗ID: { 종목코드: 종목명 } }  — 만료되지 않은 활성 종목만
# ============================================================================
def get_active_user_targets() -> Dict[str, Dict[str, str]]:
    all_wls = load_all_watchlists()
    today = datetime.now(KST).date()
    changed = False
    result: Dict[str, Dict[str, str]] = {}

    for chat_id, wl in list(all_wls.items()):
        active: Dict[str, str] = {}
        for code, info in list(wl.items()):
            name = info.get("name", WATCH_TARGETS.get(code, code))
            expire = info.get("expire")
            if expire is None:
                active[code] = name
                continue
            try:
                exp_date = datetime.strptime(expire, "%Y-%m-%d").date()
            except Exception:
                # 잘못된 날짜는 무제한으로 처리
                info["expire"] = None
                changed = True
                active[code] = name
                continue
            if exp_date >= today:
                active[code] = name
            else:
                logger.info(f"만료 종목 제거: [{chat_id}] {name}({code}) - {expire}")
                del wl[code]
                changed = True

        if wl:
            all_wls[chat_id] = wl
        else:
            all_wls.pop(chat_id, None)
        if active:
            result[chat_id] = active

    if changed:
        save_all_watchlists(all_wls)
    return result


# ============================================================================
# JobQueue 콜백 : 주기적 체크
# ============================================================================
def is_market_holiday(d) -> bool:
    """한국 증시 휴장일인지 판정.

    - 공공 공휴일(설날/추석 연휴, 대체공휴일 등)은 holidays 라이브러리로 판정
    - 증시 추가 휴장: 근로자의 날(5/1), 연말 마지막 영업일(12/31)
    """
    if d in _KR_HOLIDAYS:
        return True
    if (d.month, d.day) in [(5, 1), (12, 31)]:
        return True
    return False


def is_market_window() -> bool:
    """체크를 실행할 시간대인지 판정.

    - 한국 주식 정규장: 평일 09:00 ~ 15:30 KST
    - 장마감 정밀 체크(15:35)도 동시에 허용하기 위해 16:00 까지 윈도우를 둠
    - 토/일·공휴일(휴장일)은 무조건 False
    """
    now = datetime.now(KST)
    if now.weekday() >= 5:        # 5=토, 6=일
        return False
    if is_market_holiday(now.date()):
        return False
    t = now.time()
    return dtime(9, 0) <= t <= dtime(16, 0)


async def scheduled_check(context: ContextTypes.DEFAULT_TYPE) -> None:
    # 장 시간이 아니면 즉시 종료 (API 호출 절약)
    if not is_market_window():
        now = datetime.now(KST)
        if now.weekday() < 5 and is_market_holiday(now.date()):
            logger.info("공휴일(휴장) — 체크 건너뜀")
        else:
            logger.info("장 시간 외 — 체크 건너뜀")
        return

    # 사용자별 활성 종목 { 챗ID: { 종목: 종목명 } }
    user_targets = get_active_user_targets()
    if not user_targets:
        logger.info("감시 대상 없음 (아무도 /watch 로 종목을 켜지 않음)")
        return

    # 모든 사용자가 보는 종목의 합집합 → 종목당 데이터는 한 번만 조회
    all_codes: Dict[str, str] = {}
    for active in user_targets.values():
        all_codes.update(active)

    logger.info(
        f"EMA 체크 시작 ({len(all_codes)} 종목, 사용자 {len(user_targets)}명)"
    )
    bot = context.bot

    alerts_total = 0
    for code, name in all_codes.items():
        try:
            alerts = compute_alerts_for_code(code, name)  # [(kind, w, sent_key, 메시지)]
            if not alerts:
                continue
            # 이 종목을 켠 사용자에게만, (EMA라인 선택 + 사용자별 중복방지) 적용해 발송
            for chat_id, active in user_targets.items():
                if code not in active:
                    continue
                ema_pref = get_user_ema_pref(chat_id)
                for kind, w, sent_key, msg in alerts:
                    if w not in ema_pref.get(kind, []):
                        continue  # 이 사용자가 끈 EMA 라인 → 건너뜀
                    if already_sent_today(chat_id, code, sent_key):
                        continue
                    try:
                        await bot.send_message(
                            chat_id=chat_id,
                            text=msg,
                            parse_mode="Markdown",
                        )
                        mark_sent_today(chat_id, code, sent_key)
                        alerts_total += 1
                        await asyncio.sleep(0.3)  # 텔레그램 rate-limit 보호
                    except Exception as e:
                        logger.warning(f"메시지 전송 실패 (chat {chat_id}): {e}")
        except Exception as e:
            logger.warning(f"[{code}] 체크 중 오류: {e}")

    logger.info(f"체크 완료. 발송된 알람: {alerts_total}건")


# ============================================================================
# 텔레그램 명령어 핸들러
# ============================================================================
HELP_TEXT = (
    "🤖 *주식 EMA 알림 봇*\n\n"
    "*명령어*\n"
    "/start - 봇 시작 & 도움말\n"
    "/help - 도움말\n"
    "/myid - 내 챗ID / 켜둔 종목 수 확인\n"
    "/watch - 알람 받을 종목 선택 (켜기✅/끄기⬜, 모두 켜기/끄기)\n"
    "/add - 원하는 종목 추가 (이름/코드로 검색, 예: /add 한미반도체)\n"
    "/ema - 받을 이평선 선택 (일봉5/20/60/120, 주봉5/20 중)\n"
    "/list - 내가 감시 중인 종목 보기\n"
    "/check - 지금 즉시 1회 EMA 체크 실행\n\n"
    f"*조건*: 현재가가 EMA 라인 ±{TOUCH_THRESHOLD_PCT}% 범위 진입 시 알람\n"
    "  - 일봉 EMA 5/20/60/120\n"
    "  - 주봉 EMA 5/20\n"
    "  - 🚀상승 돌파 / 📉하락 돌파 구분 (각 방향당 하루 1회)"
)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(HELP_TEXT, parse_mode="Markdown")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(HELP_TEXT, parse_mode="Markdown")


async def cmd_myid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """본인 챗ID와 현재 켜둔 종목 수를 알려준다."""
    chat = update.effective_chat
    user = update.effective_user
    name = user.full_name if user else ""
    count = len(get_user_watchlist(chat.id))
    await update.message.reply_text(
        f"🆔 *당신의 챗ID*: `{chat.id}`\n"
        f"   이름: {name}\n"
        f"   켜둔 종목: {count}개\n\n"
        "이 봇은 사람별로 알람이 따로 갑니다. /watch 로 원하는 종목을 켜면 "
        "당신에게만 그 종목 알람이 옵니다.",
        parse_mode="Markdown",
    )


# ----------------------------------------------------------------------------
# /watch : 종목 ON/OFF 토글 (체크박스형 인라인 키보드)
#   - WATCH_TARGETS(전체 후보) 를 버튼으로 펼쳐 보여주고
#   - 켜진 종목엔 ✅, 꺼진 종목엔 ⬜ 표시
#   - 버튼을 탭하면 즉시 ON↔OFF 전환되고 키보드가 갱신됨
# ----------------------------------------------------------------------------
def build_watch_keyboard(chat_id) -> InlineKeyboardMarkup:
    wl = get_user_watchlist(chat_id)
    # 35개 기본 카탈로그 + 사용자가 /add 로 추가한 커스텀 종목
    catalog = dict(WATCH_TARGETS)
    for code, info in wl.items():
        catalog.setdefault(code, info.get("name", code))
    items = sorted(catalog.items(), key=lambda kv: kv[1])
    # 맨 위에 일괄 켜기/끄기 버튼
    buttons = [[
        InlineKeyboardButton("✅ 모두 켜기", callback_data="watchall|on"),
        InlineKeyboardButton("⬜ 모두 끄기", callback_data="watchall|off"),
    ]]
    row = []
    for code, name in items:
        mark = "✅" if code in wl else "⬜"
        row.append(InlineKeyboardButton(f"{mark} {name}", callback_data=f"watch|{code}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(buttons)


async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    wl = get_user_watchlist(chat_id)
    await update.message.reply_text(
        f"🔔 *알람 받을 종목 선택* (현재 {len(wl)}개 켜짐)\n"
        "탭하면 켜짐 ✅ / 꺼짐 ⬜ 으로 전환됩니다.\n"
        "_여기서 켠 종목은 당신에게만 알람이 옵니다._",
        reply_markup=build_watch_keyboard(chat_id),
        parse_mode="Markdown",
    )


async def cb_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    chat_id = update.effective_chat.id
    _, code = query.data.split("|", 1)
    name = WATCH_TARGETS.get(code, code)

    wl = get_user_watchlist(chat_id)
    if code in wl:
        del wl[code]
        await query.answer(f"⬜ {name} 알람 꺼짐")
    else:
        wl[code] = {"name": name, "expire": None}  # None = 무제한
        await query.answer(f"✅ {name} 알람 켜짐")
    save_user_watchlist(chat_id, wl)

    # 키보드(체크표시) 갱신 + 상단 카운트 갱신
    try:
        await query.edit_message_text(
            f"🔔 *알람 받을 종목 선택* (현재 {len(wl)}개 켜짐)\n"
            "탭하면 켜짐 ✅ / 꺼짐 ⬜ 으로 전환됩니다.\n"
            "_여기서 켠 종목은 당신에게만 알람이 옵니다._",
            reply_markup=build_watch_keyboard(chat_id),
            parse_mode="Markdown",
        )
    except Exception:
        pass


async def cb_watch_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """모두 켜기 / 모두 끄기"""
    query = update.callback_query
    chat_id = update.effective_chat.id
    _, mode = query.data.split("|", 1)

    if mode == "on":
        # 기본 35개를 모두 켜되, /add 로 추가한 커스텀 종목은 유지
        wl = get_user_watchlist(chat_id)
        for code, name in WATCH_TARGETS.items():
            wl.setdefault(code, {"name": name, "expire": None})
        await query.answer(f"✅ 모두 켰습니다 (총 {len(wl)}개)")
    else:
        wl = {}
        await query.answer("⬜ 전체 종목 껐습니다")
    save_user_watchlist(chat_id, wl)

    try:
        await query.edit_message_text(
            f"🔔 *알람 받을 종목 선택* (현재 {len(wl)}개 켜짐)\n"
            "탭하면 켜짐 ✅ / 꺼짐 ⬜ 으로 전환됩니다.\n"
            "_여기서 켠 종목은 당신에게만 알람이 옵니다._",
            reply_markup=build_watch_keyboard(chat_id),
            parse_mode="Markdown",
        )
    except Exception:
        pass


# ----------------------------------------------------------------------------
# /ema : 받을 EMA 라인 선택 (6개 중 ON/OFF, 사용자별)
# ----------------------------------------------------------------------------
def build_ema_keyboard(chat_id) -> InlineKeyboardMarkup:
    pref = get_user_ema_pref(chat_id)
    buttons = []
    # 일봉 4개 → 2열
    row = []
    for w in DAILY_EMAS:
        mark = "✅" if w in pref["daily"] else "⬜"
        row.append(InlineKeyboardButton(f"{mark} 일봉{w}", callback_data=f"ema|daily|{w}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    # 주봉 → 한 줄
    wrow = []
    for w in WEEKLY_EMAS:
        mark = "✅" if w in pref["weekly"] else "⬜"
        wrow.append(InlineKeyboardButton(f"{mark} 주봉{w}", callback_data=f"ema|weekly|{w}"))
    buttons.append(wrow)
    return InlineKeyboardMarkup(buttons)


def ema_pref_summary(chat_id) -> str:
    pref = get_user_ema_pref(chat_id)
    d = "/".join(str(w) for w in pref["daily"]) or "없음"
    wk = "/".join(str(w) for w in pref["weekly"]) or "없음"
    return f"일봉 EMA {d}, 주봉 EMA {wk}"


async def cmd_ema(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    await update.message.reply_text(
        "📊 *받을 이평선(EMA) 선택*\n"
        "탭하면 켜짐 ✅ / 꺼짐 ⬜ 으로 전환됩니다.\n"
        f"현재: {ema_pref_summary(chat_id)}",
        reply_markup=build_ema_keyboard(chat_id),
        parse_mode="Markdown",
    )


async def cb_ema(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    chat_id = update.effective_chat.id
    _, kind, w_str = query.data.split("|", 2)
    w = int(w_str)

    pref = get_user_ema_pref(chat_id)
    lst = pref.get(kind, [])
    if w in lst:
        lst.remove(w)
        await query.answer(f"⬜ {kind} EMA{w} 끔")
    else:
        lst.append(w)
        lst.sort()
        await query.answer(f"✅ {kind} EMA{w} 켬")
    pref[kind] = lst
    save_user_ema_pref(chat_id, pref)

    try:
        await query.edit_message_text(
            "📊 *받을 이평선(EMA) 선택*\n"
            "탭하면 켜짐 ✅ / 꺼짐 ⬜ 으로 전환됩니다.\n"
            f"현재: {ema_pref_summary(chat_id)}",
            reply_markup=build_ema_keyboard(chat_id),
            parse_mode="Markdown",
        )
    except Exception:
        pass


# ----------------------------------------------------------------------------
# /add : 원하는 종목을 이름/코드로 검색해 내 목록에 추가 (35개 카탈로그 밖도 가능)
# ----------------------------------------------------------------------------
def _add_stock_to_user(chat_id, code: str, name: str) -> None:
    wl = get_user_watchlist(chat_id)
    wl[code] = {"name": name, "expire": None}
    save_user_watchlist(chat_id, wl)


async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "사용법: /add 종목이름 또는 코드\n예) /add 한미반도체   또는   /add 042700"
        )
        return
    query = " ".join(context.args).strip()
    await update.message.reply_text(f"🔎 '{query}' 검색 중...")
    matches = await asyncio.to_thread(search_stocks, query)
    if not matches:
        await update.message.reply_text(
            f"'{query}' 종목을 찾지 못했습니다. 이름 일부나 6자리 코드로 다시 시도해보세요."
        )
        return

    chat_id = update.effective_chat.id
    if len(matches) == 1:
        code, name = matches[0]
        _add_stock_to_user(chat_id, code, name)
        await update.message.reply_text(
            f"✅ *{name}* (`{code}`) 추가됐습니다.\n/watch 에서 켜짐 상태를 확인할 수 있어요.",
            parse_mode="Markdown",
        )
        return

    buttons = [
        [InlineKeyboardButton(f"{n} ({c})", callback_data=f"add|{c}")]
        for c, n in matches
    ]
    await update.message.reply_text(
        f"🔎 '{query}' 검색 결과 {len(matches)}건 — 추가할 종목을 고르세요:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def cb_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    chat_id = update.effective_chat.id
    _, code = query.data.split("|", 1)
    name = get_stock_listing().get(code, code)
    _add_stock_to_user(chat_id, code, name)
    await query.answer(f"✅ {name} 추가됨")
    try:
        await query.edit_message_text(
            f"✅ *{name}* (`{code}`) 추가됐습니다.\n/watch 에서 확인/끄기 할 수 있어요.",
            parse_mode="Markdown",
        )
    except Exception:
        pass


async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    wl = get_user_watchlist(update.effective_chat.id)
    today = datetime.now(KST).date()
    lines = ["*📋 내가 감시 중인 종목*\n"]
    for code, info in sorted(wl.items(), key=lambda kv: kv[1]["name"]):
        expire = info.get("expire")
        if expire is None:
            status = "무제한"
        else:
            try:
                left = (datetime.strptime(expire, "%Y-%m-%d").date() - today).days
                status = f"~{expire} ({left}일 남음)" if left >= 0 else f"~{expire} (만료)"
            except Exception:
                status = expire
        lines.append(f"• `{code}` {info['name']} — {status}")
    text = (
        "\n".join(lines)
        if len(lines) > 1
        else "감시 중인 종목이 없습니다.\n/watch 로 알람 받을 종목을 켜보세요."
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("⏳ 지금 즉시 EMA 체크를 실행합니다...")
    await scheduled_check(context)
    await update.message.reply_text("✅ 체크 완료.")


# ============================================================================
# 봇 실행
# ============================================================================
def main() -> None:
    if (
        not TELEGRAM_BOT_TOKEN
        or TELEGRAM_BOT_TOKEN == "여기에_봇_토큰_입력"
        or not TELEGRAM_CHAT_ID
        or TELEGRAM_CHAT_ID == "여기에_챗ID_입력"
    ):
        raise SystemExit(
            "❌ TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID 가 설정되지 않았습니다.\n"
            "   환경변수로 지정하거나 코드 상단 CONFIG 영역을 수정하세요."
        )

    # connect_timeout 을 넉넉히 둬서 회선이 느려도 부팅 실패하지 않도록 함
    app = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .connect_timeout(30.0)
        .pool_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .get_updates_connect_timeout(30.0)
        .get_updates_pool_timeout(30.0)
        .build()
    )

    # 명령어 핸들러 등록
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("myid", cmd_myid))
    app.add_handler(CommandHandler("watch", cmd_watch))
    app.add_handler(CommandHandler("add", cmd_add))
    app.add_handler(CommandHandler("ema", cmd_ema))
    app.add_handler(CommandHandler("list", cmd_list))
    app.add_handler(CommandHandler("check", cmd_check))

    # 인라인 버튼 콜백
    app.add_handler(CallbackQueryHandler(cb_watch_all, pattern=r"^watchall\|"))
    app.add_handler(CallbackQueryHandler(cb_watch, pattern=r"^watch\|"))
    app.add_handler(CallbackQueryHandler(cb_ema, pattern=r"^ema\|"))
    app.add_handler(CallbackQueryHandler(cb_add, pattern=r"^add\|"))

    # ----- 스케줄러 (JobQueue) -----
    # 한국시간 평일에만 의미가 있으므로 시간만 지정하고
    # scheduled_check 내부에서 휴장일 데이터가 비면 자연스럽게 스킵됨.
    jq = app.job_queue

    # 1) 장중 주기 체크 : 09:30 ~ 15:30 KST 사이 30분 간격
    #    (간단히 30분 간격으로 하루 종일 돌리되, 휴장일/장외시간은 데이터가
    #     변하지 않아 중복 알람 방지 로직이 막아줍니다.)
    jq.run_repeating(
        scheduled_check,
        interval=timedelta(minutes=30),
        first=timedelta(seconds=10),   # 봇 시작 10초 후 첫 실행
        name="periodic_check",
    )

    # 2) 장 마감 직후 정밀 체크 : 매일 15:35 KST
    jq.run_daily(
        scheduled_check,
        time=dtime(hour=15, minute=35, tzinfo=KST),
        name="post_close_check",
    )

    logger.info("🤖 봇 시작. /start 를 보내 도움말을 확인하세요.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
