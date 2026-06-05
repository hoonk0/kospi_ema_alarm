# -*- coding: utf-8 -*-
"""
==============================================================================
 텔레그램 대화형 주식 EMA(지수이동평균) 알림 봇 (KOSPI / KOSDAQ)
==============================================================================

[기능 요약]
 - 코스피 지수, 섹터 대표주, 코스피 시총 1~30위, 코스닥 시총 1~5위 감시
 - 일봉(5/20/60/120), 주봉(5/20) **EMA** ±0.5% 진입 시 텔레그램 알람
 - 상승 돌파 / 하락 돌파 구분 (각 방향당 하루 1회)
 - 사용자가 종목별로 알람 기한(3/5/7일 또는 무제한)을 인라인 버튼으로 설정
 - 만료일이 지난 종목은 자동 감시 제외
 - python-telegram-bot 의 JobQueue 로 장중/장마감 주기 체크

[설치]
    pip install python-telegram-bot==20.7 finance-datareader pandas pytz

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

# 알람을 받을 챗ID 목록.
#   여러 명에게 보내려면 환경변수에 쉼표로 구분해 넣으면 됩니다.
#     예) TELEGRAM_CHAT_ID="1567993608,123456789,987654321"
#   ※ 각 사람은 먼저 봇에게 /start 를 한 번 보내야 메시지를 받을 수 있습니다.
#     (봇한테 /myid 를 보내면 본인 챗ID를 알려줍니다.)
CHAT_IDS = [c.strip() for c in TELEGRAM_CHAT_ID.split(",") if c.strip()]

# 이평선 ±N% 진입 시 알람 (요구사항: 0.5%)
TOUCH_THRESHOLD_PCT = 0.5

# 알람 기한 설정 정보를 저장할 파일
WATCHLIST_FILE = "watchlist.json"

# 같은 종목/같은 이평선 알람이 하루에 여러 번 가지 않도록 중복 방지
SENT_LOG_FILE = "sent_today.json"

# 한국 시간대
KST = pytz.timezone("Asia/Seoul")

# 체크할 지수이동평균(EMA) 정의
#   - 단순이동평균(SMA)이 아닌 지수이동평균(EMA) 기준으로 계산합니다.
#   - EMA 는 최근 봉에 더 큰 가중치를 부여하여 추세 변화에 빠르게 반응합니다.
DAILY_EMAS = [5, 20, 60, 120]      # 일봉 EMA
WEEKLY_EMAS = [5, 20]               # 주봉 EMA (5주, 20주)


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


def load_watchlist() -> Dict[str, Dict]:
    """
    반환 구조:
    {
      "005930": {"name": "삼성전자", "expire": "2026-06-01" or null}
    }
    expire 가 None 이면 무제한 감시
    """
    return load_json(WATCHLIST_FILE, {})


def save_watchlist(wl: Dict[str, Dict]) -> None:
    save_json(WATCHLIST_FILE, wl)


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


def already_sent_today(code: str, ma_label: str) -> bool:
    data = load_sent_log()
    return f"{code}:{ma_label}" in data["keys"]


def mark_sent_today(code: str, ma_label: str) -> None:
    data = load_sent_log()
    key = f"{code}:{ma_label}"
    if key not in data["keys"]:
        data["keys"].append(key)
        save_sent_log(data)


# ============================================================================
# 워치리스트 초기화 : WATCH_TARGETS 의 모든 종목을 '무제한'으로 기본 등록
#   (사용자가 /list, /set 등으로 개별 조정 가능)
# ============================================================================
def ensure_default_watchlist() -> Dict[str, Dict]:
    wl = load_watchlist()
    changed = False
    for code, name in WATCH_TARGETS.items():
        if code not in wl:
            wl[code] = {"name": name, "expire": None}  # None = 무제한
            changed = True
    if changed:
        save_watchlist(wl)
    return wl


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
# 핵심 체크 로직 : 알람을 보낼 메시지 리스트 생성
# ============================================================================
def build_alerts_for_code(code: str, name: str) -> List[str]:
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

    messages: List[str] = []

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
        sent_key = f"{label}|{direction}"   # 방향까지 키에 포함 → 상승/하락 각 1회
        if not already_sent_today(code, sent_key):
            messages.append(fmt_msg(label, curr_ema, direction))
            mark_sent_today(code, sent_key)

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
        sent_key = f"{label}|{direction}"
        if not already_sent_today(code, sent_key):
            messages.append(fmt_msg(label, curr_ema, direction))
            mark_sent_today(code, sent_key)

    return messages


# ============================================================================
# 만료된 워치리스트 정리 & 활성 종목 반환
# ============================================================================
def get_active_targets() -> List[Tuple[str, str]]:
    wl = ensure_default_watchlist()
    today = datetime.now(KST).date()
    active: List[Tuple[str, str]] = []
    changed = False

    for code, info in list(wl.items()):
        expire = info.get("expire")
        if expire is None:
            active.append((code, info["name"]))
            continue
        try:
            exp_date = datetime.strptime(expire, "%Y-%m-%d").date()
        except Exception:
            # 잘못된 날짜는 무제한으로 처리
            info["expire"] = None
            changed = True
            active.append((code, info["name"]))
            continue
        if exp_date >= today:
            active.append((code, info["name"]))
        else:
            logger.info(f"만료된 감시 종목 제거: {info['name']}({code}) - {expire}")
            del wl[code]
            changed = True

    if changed:
        save_watchlist(wl)
    return active


# ============================================================================
# JobQueue 콜백 : 주기적 체크
# ============================================================================
def is_market_window() -> bool:
    """체크를 실행할 시간대인지 판정.

    - 한국 주식 정규장: 평일 09:00 ~ 15:30 KST
    - 장마감 정밀 체크(15:35)도 동시에 허용하기 위해 16:00 까지 윈도우를 둠
    - 토/일은 무조건 False
    - 공휴일은 별도 체크 안 함 (휴장일엔 데이터가 갱신되지 않아 자연스럽게 알람 없음)
    """
    now = datetime.now(KST)
    if now.weekday() >= 5:        # 5=토, 6=일
        return False
    t = now.time()
    return dtime(9, 0) <= t <= dtime(16, 0)


async def scheduled_check(context: ContextTypes.DEFAULT_TYPE) -> None:
    # 장 시간이 아니면 즉시 종료 (API 호출 절약)
    if not is_market_window():
        logger.info("장 시간 외 — 체크 건너뜀")
        return

    targets = get_active_targets()
    if not targets:
        logger.info("감시 대상 없음")
        return

    logger.info(f"EMA 체크 시작 ({len(targets)} 종목, 수신자 {len(CHAT_IDS)}명)")
    bot = context.bot

    alerts_total = 0
    for code, name in targets:
        try:
            msgs = build_alerts_for_code(code, name)
            for m in msgs:
                # 같은 알람을 등록된 모든 수신자에게 발송
                for cid in CHAT_IDS:
                    try:
                        await bot.send_message(
                            chat_id=cid,
                            text=m,
                            parse_mode="Markdown",
                        )
                        alerts_total += 1
                        await asyncio.sleep(0.3)  # 텔레그램 rate-limit 보호
                    except Exception as e:
                        logger.warning(f"메시지 전송 실패 (chat {cid}): {e}")
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
    "/myid - 내 챗ID 확인 (알람 수신자 추가용)\n"
    "/list - 현재 감시 중인 종목 보기\n"
    "/set - 종목 알람 기한 설정 (대화형)\n"
    "/check - 지금 즉시 1회 EMA 체크 실행\n"
    "/stop\\_code <종목코드> - 해당 종목 감시 즉시 해제\n\n"
    f"*조건*: 현재가가 EMA 라인 ±{TOUCH_THRESHOLD_PCT}% 범위 진입 시 알람\n"
    "  - 일봉 EMA 5/20/60/120\n"
    "  - 주봉 EMA 5/20\n"
    "  - 🚀상승 돌파 / 📉하락 돌파 구분 (각 방향당 하루 1회)"
)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ensure_default_watchlist()
    await update.message.reply_text(HELP_TEXT, parse_mode="Markdown")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(HELP_TEXT, parse_mode="Markdown")


async def cmd_myid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """본인 챗ID를 알려준다. 새 수신자를 추가할 때 이 ID를 관리자에게 전달하면 됩니다."""
    chat = update.effective_chat
    user = update.effective_user
    name = user.full_name if user else ""
    registered = "✅ 이미 등록됨" if str(chat.id) in CHAT_IDS else "❌ 아직 미등록"
    await update.message.reply_text(
        f"🆔 *당신의 챗ID*: `{chat.id}`\n"
        f"   이름: {name}\n"
        f"   알람 수신: {registered}\n\n"
        "이 ID를 관리자에게 알려주면 알람 수신자로 추가할 수 있습니다.",
        parse_mode="Markdown",
    )


async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    wl = ensure_default_watchlist()
    today = datetime.now(KST).date()
    lines = ["*📋 감시 중인 종목 목록*\n"]
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
    text = "\n".join(lines) if len(lines) > 1 else "감시 중인 종목이 없습니다."
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_set(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /set : 종목 선택 → 기한 선택 (인라인 키보드 2단계)
    """
    wl = ensure_default_watchlist()
    if not wl:
        await update.message.reply_text("등록된 종목이 없습니다.")
        return

    # 종목이 많을 수 있으므로 2열로 배치
    items = sorted(wl.items(), key=lambda kv: kv[1]["name"])
    buttons = []
    row = []
    for code, info in items:
        row.append(
            InlineKeyboardButton(
                f"{info['name']}", callback_data=f"pick|{code}"
            )
        )
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    await update.message.reply_text(
        "기한을 설정할 종목을 선택하세요:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def cb_pick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """종목 선택 후 → 기한 버튼 표시"""
    query = update.callback_query
    await query.answer()

    _, code = query.data.split("|", 1)
    wl = ensure_default_watchlist()
    name = wl.get(code, {}).get("name", code)

    keyboard = [
        [
            InlineKeyboardButton("3일", callback_data=f"days|{code}|3"),
            InlineKeyboardButton("5일", callback_data=f"days|{code}|5"),
        ],
        [
            InlineKeyboardButton("7일", callback_data=f"days|{code}|7"),
            InlineKeyboardButton("무제한", callback_data=f"days|{code}|0"),
        ],
        [
            InlineKeyboardButton("❌ 감시 해제", callback_data=f"days|{code}|-1"),
        ],
    ]
    await query.edit_message_text(
        text=f"*{name}* (`{code}`) — 며칠 동안 알람을 받으시겠습니까?",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )


async def cb_days(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """기한 선택 콜백 처리"""
    query = update.callback_query
    await query.answer()

    try:
        _, code, days_str = query.data.split("|", 2)
        days = int(days_str)
    except Exception:
        await query.edit_message_text("잘못된 입력입니다.")
        return

    wl = ensure_default_watchlist()
    if code not in wl:
        wl[code] = {"name": WATCH_TARGETS.get(code, code), "expire": None}

    name = wl[code]["name"]

    if days == -1:
        # 감시 해제
        del wl[code]
        save_watchlist(wl)
        await query.edit_message_text(
            f"❌ *{name}* (`{code}`) 감시를 해제했습니다.",
            parse_mode="Markdown",
        )
        return

    if days == 0:
        wl[code]["expire"] = None
        msg = f"✅ *{name}* (`{code}`) — *무제한* 감시로 설정했습니다."
    else:
        expire_date = datetime.now(KST).date() + timedelta(days=days)
        wl[code]["expire"] = expire_date.strftime("%Y-%m-%d")
        msg = (
            f"✅ *{name}* (`{code}`) — 앞으로 *{days}일*간 감시합니다.\n"
            f"   만료일: {wl[code]['expire']}"
        )

    save_watchlist(wl)
    await query.edit_message_text(msg, parse_mode="Markdown")


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("⏳ 지금 즉시 EMA 체크를 실행합니다...")
    await scheduled_check(context)
    await update.message.reply_text("✅ 체크 완료.")


async def cmd_stop_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("사용법: /stop_code <종목코드>\n예) /stop_code 005930")
        return
    code = context.args[0].strip()
    wl = ensure_default_watchlist()
    if code in wl:
        name = wl[code]["name"]
        del wl[code]
        save_watchlist(wl)
        await update.message.reply_text(f"❌ {name}({code}) 감시 해제 완료.")
    else:
        await update.message.reply_text(f"`{code}` 는 감시 목록에 없습니다.", parse_mode="Markdown")


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

    # 워치리스트 기본값 채워두기
    ensure_default_watchlist()

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
    app.add_handler(CommandHandler("list", cmd_list))
    app.add_handler(CommandHandler("set", cmd_set))
    app.add_handler(CommandHandler("check", cmd_check))
    app.add_handler(CommandHandler("stop_code", cmd_stop_code))

    # 인라인 버튼 콜백
    app.add_handler(CallbackQueryHandler(cb_pick, pattern=r"^pick\|"))
    app.add_handler(CallbackQueryHandler(cb_days, pattern=r"^days\|"))

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
