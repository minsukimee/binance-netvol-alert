import asyncio
import aiohttp
import os
import json
from datetime import datetime

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
GIST_TOKEN = os.environ["GIST_TOKEN"]
GIST_ID = os.environ["GIST_ID"]
GIST_FILENAME = "binance_netvol_state.json"

EXCLUDE_SYMBOLS = {
    "BTCUSDT", "ETHUSDT",
    "XAUUSDT", "XAGUSDT",
}

EXCLUDE_KEYWORDS = ["1000", "NEIRO"]

SPIKE_MULTIPLIER = 10.0
HOLD_MULTIPLIER = 5.0


async def get_all_futures_symbols(session):
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    async with session.get(url) as resp:
        if resp.status != 200:
            print(f"바이낸스 API 오류: {resp.status}")
            return []
        data = await resp.json()

    symbols = []
    for s in data["symbols"]:
        sym = s["symbol"]
        if s["status"] != "TRADING":
            continue
        if not sym.endswith("USDT"):
            continue
        if sym in EXCLUDE_SYMBOLS:
            continue
        if s.get("contractType") != "PERPETUAL":
            continue
        if any(k in sym for k in EXCLUDE_KEYWORDS):
            continue
        symbols.append(sym)

    return symbols


async def get_1min_klines(session, symbol, limit=250):
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": "1m",
        "limit": limit,
    }
    async with session.get(url, params=params) as resp:
        if resp.status != 200:
            return None
        return await resp.json()


def calculate_current_4h_net_volume(klines_1m):
    if not klines_1m or len(klines_1m) < 241:
        return None

    completed = klines_1m[:-1]
    current_4h = completed[-240:]

    net = 0
    for c in current_4h:
        open_price = float(c[1])
        close_price = float(c[4])
        volume = float(c[5])
        if close_price > open_price:
            net += volume
        elif close_price < open_price:
            net -= volume

    return net


async def load_state(session):
    url = f"https://api.github.com/gists/{GIST_ID}"
    headers = {
        "Authorization": f"token {GIST_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    async with session.get(url, headers=headers) as resp:
        if resp.status != 200:
            print(f"Gist 로드 오류: {resp.status}")
            return {}
        data = await resp.json()
        content = data["files"].get(GIST_FILENAME, {}).get("content", "{}")
        try:
            return json.loads(content)
        except Exception as e:
            print(f"Gist 파싱 오류: {e}")
            return {}


async def save_state(session, state):
    url = f"https://api.github.com/gists/{GIST_ID}"
    headers = {
        "Authorization": f"token {GIST_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    payload = {
        "files": {
            GIST_FILENAME: {
                "content": json.dumps(state, ensure_ascii=False, indent=2)
            }
        }
    }
    async with session.patch(url, headers=headers, json=payload) as resp:
        if resp.status not in [200, 201]:
            print(f"Gist 저장 오류: {resp.status}")
        return resp.status


async def send_telegram(session, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }
    async with session.post(url, json=payload) as resp:
        return resp.status


async def process_symbol(session, symbol, state, semaphore):
    async with semaphore:
        try:
            klines = await get_1min_klines(session, symbol, limit=250)
            if not klines:
                return None

            current_net = calculate_current_4h_net_volume(klines)
            if current_net is None:
                return None

            current_abs = abs(current_net)
            if current_abs == 0:
                return None

            now_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
            prev = state.get(symbol)

            if not prev:
                state[symbol] = {
                    "base_net_vol": current_abs,
                    "base_time": now_str,
                    "alerted": False,
                    "alert_net_vol": None,
                }
                return None

            base = prev["base_net_vol"]
            alerted = prev["alerted"]
            alert_net_vol = prev.get("alert_net_vol")

            ratio = current_abs / base if base > 0 else 0

            if alerted and alert_net_vol:
                if current_abs > alert_net_vol:
                    state[symbol] = {
                        "base_net_vol": current_abs,
                        "base_time": now_str,
                        "alerted": True,
                        "alert_net_vol": current_abs,
                    }
                    direction = "🟢 매수" if current_net > 0 else "🔴 매도"
                    return {
                        "symbol": symbol,
                        "current_net": current_net,
                        "ratio": ratio,
                        "direction": direction,
                        "reason": "연속 상승",
                    }
                else:
                    state[symbol] = {
                        "base_net_vol": current_abs,
                        "base_time": now_str,
                        "alerted": False,
                        "alert_net_vol": None,
                    }
                    return None

            if ratio >= SPIKE_MULTIPLIER:
                state[symbol] = {
                    "base_net_vol": current_abs,
                    "base_time": now_str,
                    "alerted": True,
                    "alert_net_vol": current_abs,
                }
                direction = "🟢 매수" if current_net > 0 else "🔴 매도"
                return {
                    "symbol": symbol,
                    "current_net": current_net,
                    "ratio": ratio,
                    "direction": direction,
                    "reason": f"기준봉 대비 {ratio:.1f}배",
                }

            elif ratio >= HOLD_MULTIPLIER:
                state[symbol]["alerted"] = False
                return None

            else:
                state[symbol] = {
                    "base_net_vol": current_abs,
                    "base_time": now_str,
                    "alerted": False,
                    "alert_net_vol": None,
                }
                return None

        except Exception as e:
            print(f"[{symbol}] 오류: {e}")
            return None


async def main():
    print(f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC] 스캔 시작")

    semaphore = asyncio.Semaphore(20)
    connector = aiohttp.TCPConnector(limit=50)

    async with aiohttp.ClientSession(connector=connector) as session:
        state = await load_state(session)
        print(f"상태 로드: {len(state)}개 코인 기록")

        symbols = await get_all_futures_symbols(session)
        print(f"감시 대상: {len(symbols)}개 코인")

        tasks = [process_symbol(session, sym, state, semaphore) for sym in symbols]
        results = await asyncio.gather(*tasks)

        alerts = [r for r in results if r is not None]

        save_status = await save_state(session, state)
        print(f"상태 저장: HTTP {save_status}")

        if not alerts:
            print("급등 신호 없음")
            return

        alerts.sort(key=lambda x: x["ratio"], reverse=True)

        for alert in alerts:
            msg = (
                f"<b>⚡ 넷볼륨 급등 감지</b>\n"
                f"코인: <b>{alert['symbol']}</b>\n"
                f"방향: {alert['direction']}\n"
                f"현재 넷볼륨: {alert['current_net']:,.0f}\n"
                f"기준 대비: <b>{alert['ratio']:.1f}배</b>\n"
                f"사유: {alert['reason']}\n"
                f"시각: {datetime.utcnow().strftime('%H:%M UTC')}"
            )
            status = await send_telegram(session, msg)
            print(f"알림: {alert['symbol']} ({alert['ratio']:.1f}배) → HTTP {status}")
            await asyncio.sleep(0.3)

    print("스캔 완료")


if __name__ == "__main__":
    asyncio.run(main())
