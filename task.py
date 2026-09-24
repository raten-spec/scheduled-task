#!/usr/bin/env python3
"""
Basit bir otomatik emir yönetim betiği (maliyet tabanlı sürüm).

Her çalıştırmada:
  1. Cüzdanda satılmayı bekleyen bir bakiye var mı bakılır.
       -> Varsa: açık alış emri iptal edilir. Elimizdeki stoğun ORTALAMA
          MALİYETİ geçmiş işlemlerden hesaplanır ve satış fiyatı asla
          maliyet * (1 + MIN_PROFIT_PCT/100) altına inmez. Satış emri
          defterin tepesinde (best_ask - tick) durur; tepe fiyat taban
          fiyatın altındaysa emir TABAN fiyatta bekler (iptal/yeniden
          fiyatlama yapılmaz).
  2. Bakiye yoksa: alış emri defterin tepesine (best_bid + tick) konur.

ORTAM DEĞİŞKENLERİ:
  MIN_PROFIT_PCT (varsayılan 0.3): satış, ortalama maliyetin en az bu % üstünde.
  STOP_LOSS_PCT  (varsayılan 0 = KAPALI): 0'dan büyükse ve best_bid maliyetin
                 bu % altına düşerse taban devre dışı kalır ve zararına çıkılır.
                 Bu, sınırsız beklemeye karşı bilinçli bir sigortadır; değeri
                 siz belirleyin (ör. 3).

Diğer tüm ayarlar (DRY_RUN, BUNDLE_MODE, SEND_DELAY_SECS, ORDER_SIZE_QUOTE,
MAX_ALLOCATION_PCT, TOP_TICKS, DUST_THRESHOLD, KEEP_BASE_RESERVE) öncekiyle aynıdır.
Önce DRY_RUN=true ile logları izleyin.

Ağ ücret kuralı: her blokta ilk işlem ücretsizdir, aynı bloktaki ek işlemler
küçük bir ücret keser. Bundle kapalıyken işlemler SEND_DELAY_SECS aralıkla
ayrı bloklara gönderilir.
"""

import os
import sys
import json
import math
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("task-runner")

ACCOUNT_NAME = os.environ.get("ACCOUNT_NAME", "").strip()
ACCOUNT_KEY = os.environ.get("ACCOUNT_KEY", "").strip()

DRY_RUN = os.environ.get("DRY_RUN", "true").strip().lower() != "false"
BUNDLE_MODE = os.environ.get("BUNDLE_MODE", "false").strip().lower() == "true"
SEND_DELAY_SECS = float(os.environ.get("SEND_DELAY_SECS", "6"))

QUOTE_SYMBOL = "SWAP.HIVE"
BASE_SYMBOL = os.environ.get("BASE_SYMBOL", "SWAP.LTC").strip().upper()

ORDER_SIZE_QUOTE = float(os.environ.get("ORDER_SIZE_QUOTE", "1"))
MAX_ALLOCATION_PCT = float(os.environ.get("MAX_ALLOCATION_PCT", "20"))
TOP_TICKS = int(os.environ.get("TOP_TICKS", "1"))
DUST_THRESHOLD = float(os.environ.get("DUST_THRESHOLD", "0.00000100"))
KEEP_BASE_RESERVE = float(os.environ.get("KEEP_BASE_RESERVE", "0"))

MIN_PROFIT_PCT = float(os.environ.get("MIN_PROFIT_PCT", "0.3"))
STOP_LOSS_PCT = float(os.environ.get("STOP_LOSS_PCT", "0"))
HISTORY_URL = os.environ.get("HISTORY_URL", "https://history.hive-engine.com/accountHistory")

NODES = [
    "https://api.hive.blog",
    "https://anyx.io",
    "https://api.deathwing.me",
]

SENT_TXS = []
SEND_ERRORS = []
SENT_WORD = "kuyruğa eklendi" if BUNDLE_MODE else "gönderildi"


def fail(msg):
    log.error(msg)
    sys.exit(1)


def floor_to(value, precision):
    factor = 10 ** precision
    return math.floor(value * factor + 1e-9) / factor


_last_send_at = None


def pace():
    global _last_send_at
    if BUNDLE_MODE:
        return
    if _last_send_at is not None:
        wait = SEND_DELAY_SECS - (time.monotonic() - _last_send_at)
        if wait > 0:
            time.sleep(wait)
    _last_send_at = time.monotonic()


def record_tx(label, tx):
    if BUNDLE_MODE:
        return None
    trx_id = tx.get("trx_id") if isinstance(tx, dict) else None
    if trx_id:
        SENT_TXS.append((label, trx_id))
    else:
        log.warning("%s için işlem kimliği alınamadı, sonuç kontrol edilemeyecek.", label)
    return trx_id


def load_clients():
    try:
        from nectar import Hive
        from nectarengine.api import Api
        from nectarengine.market import Market
        from nectarengine.wallet import Wallet
    except ImportError:
        fail("Gerekli bağımlılıklar kurulu değil. `pip install -r requirements.txt` çalıştırın.")

    keys = [] if DRY_RUN else [ACCOUNT_KEY]
    hive = Hive(node=NODES, keys=keys, bundle=(BUNDLE_MODE and not DRY_RUN))
    api = Api()
    market = Market(blockchain_instance=hive)
    wallet = Wallet(ACCOUNT_NAME, api=api)
    return hive, api, market, wallet


BOOK_SAMPLE_LIMIT = int(os.environ.get("BOOK_SAMPLE_LIMIT", "1000"))


def get_book_top(api, symbol):
    """En iyi alış/satış fiyatları. Fiyat alanı string olduğu için sıralama
    API'ye bırakılmaz; min/max Python'da float ile bulunur."""
    buy_orders = api.find("market", "buyBook", query={"symbol": symbol}, limit=BOOK_SAMPLE_LIMIT)
    sell_orders = api.find("market", "sellBook", query={"symbol": symbol}, limit=BOOK_SAMPLE_LIMIT)

    if buy_orders and len(buy_orders) >= BOOK_SAMPLE_LIMIT:
        log.warning("%s alış defterinde %d+ emir var (BOOK_SAMPLE_LIMIT'e takıldı); "
                    "gerçek en iyi alış bu örneklemin dışında kalmış olabilir.", symbol, BOOK_SAMPLE_LIMIT)
    if sell_orders and len(sell_orders) >= BOOK_SAMPLE_LIMIT:
        log.warning("%s satış defterinde %d+ emir var (BOOK_SAMPLE_LIMIT'e takıldı); "
                    "gerçek en iyi satış bu örneklemin dışında kalmış olabilir.", symbol, BOOK_SAMPLE_LIMIT)

    best_bid = max((float(o["price"]) for o in buy_orders), default=None) if buy_orders else None
    best_ask = min((float(o["price"]) for o in sell_orders), default=None) if sell_orders else None
    return best_bid, best_ask


def get_precision(api, symbol):
    info = api.find_one("tokens", "tokens", query={"symbol": symbol})
    if not info:
        return 8
    return int(info["precision"])


# ---------------------------------------------------------------------------
# MALİYET HESABI (durum dosyası yok: her şey geçmiş işlemlerden hesaplanır)
# ---------------------------------------------------------------------------
def fetch_history(account, symbol, pages=8, limit=500):
    import requests
    rows = []
    for p in range(pages):
        r = requests.get(
            HISTORY_URL,
            params={"account": account, "symbol": symbol, "limit": limit, "offset": p * limit},
            timeout=20,
        )
        r.raise_for_status()
        chunk = r.json()
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < limit:
            break
    return rows


def avg_cost_from_rows(rows, symbol):
    """Hareketli ortalama maliyet (birim başına). Stok yoksa None.
    Satışlar stoğu ortalama maliyetle düşer; kalan stoğun ort. maliyeti değişmez."""
    qty = cost = 0.0
    for r in sorted(rows, key=lambda x: x.get("timestamp", 0)):
        if r.get("symbol") != symbol or r.get("operation") not in ("market_buy", "market_sell"):
            continue
        try:
            q = float(r["quantityTokens"])
            h = float(r["quantityHive"])
        except (KeyError, TypeError, ValueError):
            continue
        if r["operation"] == "market_buy":
            qty += q
            cost += h
        elif qty > 0:
            sold = min(q, qty)
            cost -= cost * sold / qty
            qty -= sold
    return cost / qty if qty > 1e-12 else None


def plan_sell(best_bid, best_ask, our_top_ask, cost, tick):
    """Saf karar fonksiyonu. Dönüş: (eylem, fiyat, not)
       eylem: "keep" | "place" | "wait"."""
    eps = 1e-12
    if cost is None:
        return "wait", None, "maliyet bulunamadı"

    floor = cost * (1 + MIN_PROFIT_PCT / 100.0)
    note = ""
    if STOP_LOSS_PCT > 0 and best_bid is not None and best_bid < cost * (1 - STOP_LOSS_PCT / 100.0):
        floor = None
        note = "STOP-LOSS devrede, taban kaldırıldı"

    at_top = (our_top_ask is not None and best_ask is not None and our_top_ask <= best_ask + eps)

    if our_top_ask is not None:
        if floor is None:
            if at_top:
                return "keep", our_top_ask, note
        else:
            ok_vs_floor = our_top_ask >= floor * (1 - 1e-9)
            if ok_vs_floor and (at_top or our_top_ask <= floor * (1 + 1e-9)):
                return "keep", our_top_ask, "taban/tepe fiyatında bekleniyor"

    reference = best_ask if best_ask is not None else (best_bid + tick if best_bid is not None else None)
    if reference is None:
        return "wait", None, "defterde referans yok"
    target = reference - TOP_TICKS * tick
    if best_bid is not None:
        min_price = best_bid + tick
        if min_price >= reference:
            return "wait", None, "spread çok dar/çakışık"
        target = max(target, min_price)
    if floor is not None:
        target = max(target, floor)
    if target <= 0:
        return "wait", None, "geçersiz fiyat"
    return "place", round(target, 8), note


def get_our_open_buy(market, symbol):
    try:
        return market.get_buy_book(symbol, account=ACCOUNT_NAME) or []
    except Exception as e:
        log.warning("Açık alış emirleri okunamadı (%s): %s", symbol, e)
        return []


def get_our_open_sell(market, symbol):
    try:
        return market.get_sell_book(symbol, account=ACCOUNT_NAME) or []
    except Exception as e:
        log.warning("Açık satış emirleri okunamadı (%s): %s", symbol, e)
        return []


def cancel_order(market, order_type, symbol, order, reason):
    oid = order.get("txId") or order.get("_id")
    if not oid:
        log.warning("%s %s emrinde id bulunamadı, atlandı: %r", symbol, order_type, order)
        return
    if DRY_RUN:
        log.info("[DRY_RUN] %s %s emri iptal edilirdi (id=%s) — sebep: %s", symbol, order_type, oid, reason)
        return
    pace()
    try:
        tx = market.cancel(ACCOUNT_NAME, order_type, oid)
        log.info("%s %s iptali %s (id=%s) — sebep: %s", symbol, order_type, SENT_WORD, oid, reason)
        record_tx("%s %s iptali" % (symbol, order_type), tx)
    except Exception as e:
        log.warning("%s %s iptali gönderilemedi (id=%s): %r", symbol, order_type, oid, e)
        SEND_ERRORS.append("%s %s iptali" % (symbol, order_type))


def get_balance(wallet, symbol):
    try:
        bal = wallet.get_token(symbol)
        if not bal:
            return 0.0
        return float(bal.get("balance", 0))
    except Exception as e:
        log.warning("%s bakiyesi okunamadı: %s", symbol, e)
        return 0.0


def place_top_sell(market, symbol, precision, sell_price, amount):
    sell_amount = floor_to(amount, precision)
    if sell_amount <= 0:
        log.info("%s: satılacak miktar 0 (precision/dust nedeniyle), atlandı.", symbol)
        return
    if DRY_RUN:
        log.info("[DRY_RUN] SATIŞ %s %s @ %s — açılır ve BEKLENİR", sell_amount, symbol, sell_price)
        return
    pace()
    try:
        tx = market.sell(ACCOUNT_NAME, sell_amount, symbol, sell_price)
        log.info("SATIŞ %s: %s %s @ %s — açıldı, bekleniyor", SENT_WORD, sell_amount, symbol, sell_price)
        record_tx("%s satış" % symbol, tx)
    except Exception as e:
        log.error("Satış emri gönderilemedi (%s): %r", symbol, e)
        SEND_ERRORS.append("%s satış" % symbol)


def place_top_buy(market, symbol, precision, buy_price, quote_budget):
    spend = min(ORDER_SIZE_QUOTE, quote_budget)
    buy_amount = floor_to(spend / buy_price, precision) if buy_price > 0 else 0
    if buy_amount <= 0:
        log.info("%s için alış emri atlandı (bütçe/precision nedeniyle miktar 0).", symbol)
        return
    if DRY_RUN:
        log.info("[DRY_RUN] TEPE ALIŞ %s %s @ %s (harcanacak ~%.8f) — açılır ve BEKLENİR",
                 buy_amount, symbol, buy_price, spend)
        return
    pace()
    try:
        tx = market.buy(ACCOUNT_NAME, buy_amount, symbol, buy_price)
        log.info("TEPE ALIŞ %s: %s %s @ %s (~%.8f) — açıldı, bekleniyor",
                 SENT_WORD, buy_amount, symbol, buy_price, spend)
        record_tx("%s tepe alış" % symbol, tx)
    except Exception as e:
        log.error("Alış emri gönderilemedi (%s): %r", symbol, e)
        SEND_ERRORS.append("%s alış" % symbol)


def verify_signing_key(hive):
    try:
        from nectar.account import Account
        from nectargraphenebase.account import PrivateKey

        my_pub = str(PrivateKey(ACCOUNT_KEY).pubkey)[3:]
        acc = Account(ACCOUNT_NAME, blockchain_instance=hive)
        signing_keys = set()
        for role in ("active", "owner"):
            for entry in acc[role]["key_auths"]:
                signing_keys.add(str(entry[0])[3:])
        posting_keys = {str(e[0])[3:] for e in acc["posting"]["key_auths"]}
        memo_key = str(acc["memo_key"])[3:]
    except Exception as e:
        log.warning("Anahtar ön kontrolü yapılamadı, devam ediliyor: %r", e)
        return

    if my_pub in signing_keys:
        log.info("Anahtar doğrulandı: ACCOUNT_KEY, %s hesabının yetkili anahtarına ait.", ACCOUNT_NAME)
        return
    if my_pub in posting_keys:
        fail("ACCOUNT_KEY olarak yetkisiz bir anahtar girilmiş (posting). Active/owner düzeyinde anahtar gerekir.")
    if my_pub == memo_key:
        fail("ACCOUNT_KEY olarak yetkisiz bir anahtar girilmiş (memo). Active/owner düzeyinde anahtar gerekir.")
    fail("ACCOUNT_KEY, %s hesabının yetkili anahtarlarından hiçbiriyle eşleşmiyor. "
         "Anahtarı ve ACCOUNT_NAME değerini kontrol edin." % ACCOUNT_NAME)


def pending_op_count(hive):
    try:
        ops = getattr(getattr(hive, "txbuffer", None), "ops", None)
        return None if ops is None else len(ops)
    except Exception:
        return None


def check_sidechain_results(api, tx_ids, wait_rounds=6, wait_secs=5):
    pending = list(tx_ids)
    rejected = False
    for _ in range(wait_rounds):
        time.sleep(wait_secs)
        still_pending = []
        for label, txid in pending:
            try:
                info = api.get_transaction_info(txid)
            except Exception as e:
                log.warning("%s sonucu sorgulanamadı (%s): %r", label, txid, e)
                still_pending.append((label, txid))
                continue
            if not info:
                still_pending.append((label, txid))
                continue
            logs = info.get("logs") if isinstance(info, dict) else None
            if isinstance(logs, str):
                try:
                    logs = json.loads(logs)
                except ValueError:
                    logs = {"errors": [logs]}
            errors = logs.get("errors") if isinstance(logs, dict) else None
            if errors:
                log.error("REDDEDİLDİ: %s (%s): %s", label, txid, "; ".join(str(x) for x in errors))
                rejected = True
            else:
                log.info("Kabul edildi: %s (%s)", label, txid)
        pending = still_pending
        if not pending:
            break
    for label, txid in pending:
        log.warning("Sonuç henüz görünmedi: %s (%s). Daha sonra kontrol edin.", label, txid)
    return rejected


def run():
    if not ACCOUNT_NAME:
        fail("ACCOUNT_NAME ortam değişkeni / secret ayarlanmamış.")
    if not DRY_RUN and not ACCOUNT_KEY:
        fail("DRY_RUN=false iken ACCOUNT_KEY zorunludur.")

    log.info("Başlıyor. Hesap=%s Sembol=%s/%s DRY_RUN=%s BUNDLE=%s ORDER_SIZE=%s MAX_ALLOC=%%%s MIN_PROFIT=%%%s STOP_LOSS=%%%s",
             ACCOUNT_NAME, BASE_SYMBOL, QUOTE_SYMBOL, DRY_RUN, BUNDLE_MODE, ORDER_SIZE_QUOTE,
             MAX_ALLOCATION_PCT, MIN_PROFIT_PCT, STOP_LOSS_PCT)

    hive, api, market, wallet = load_clients()
    if not DRY_RUN:
        verify_signing_key(hive)

    precision = get_precision(api, BASE_SYMBOL)
    tick = 10 ** (-precision)
    best_bid, best_ask = get_book_top(api, BASE_SYMBOL)
    log.info("%s/%s defteri: best_bid=%s best_ask=%s (precision=%s)", BASE_SYMBOL, QUOTE_SYMBOL, best_bid, best_ask, precision)

    base_balance = get_balance(wallet, BASE_SYMBOL)
    sellable = max(0.0, base_balance - KEEP_BASE_RESERVE)
    log.info("%s bakiyesi: %.8f (rezerv hariç satılabilir: %.8f)", BASE_SYMBOL, base_balance, sellable)

    failed = False
    open_buys = get_our_open_buy(market, BASE_SYMBOL)
    open_sells = get_our_open_sell(market, BASE_SYMBOL)
    log.info("%s için %d açık alış, %d açık satış emrimiz var.", BASE_SYMBOL, len(open_buys), len(open_sells))

    if sellable > DUST_THRESHOLD:
        for o in open_buys:
            cancel_order(market, "buy", BASE_SYMBOL, o, "envanteri satmadan önce bekleyen alış emri temizleniyor")

        # Maliyet okunamazsa (ağ hatası) ZARARINA SATMAMAK için bu tur hiçbir şey yapma.
        try:
            cost = avg_cost_from_rows(fetch_history(ACCOUNT_NAME, BASE_SYMBOL), BASE_SYMBOL)
        except Exception as e:
            log.error("Geçmiş okunamadı, maliyet bilinmiyor; bu tur satış emrine dokunulmuyor: %r", e)
            cost = None
        if cost is not None:
            log.info("%s ort. maliyet: %.8f | taban (min kâr %%%s): %.8f",
                     BASE_SYMBOL, cost, MIN_PROFIT_PCT, cost * (1 + MIN_PROFIT_PCT / 100.0))

        our_top_ask = min((float(o["price"]) for o in open_sells), default=None) if open_sells else None
        action, price, note = plan_sell(best_bid, best_ask, our_top_ask, cost, tick)
        log.info("Satış planı: %s @ %s %s", action, price, ("(" + note + ")") if note else "")

        if action == "place":
            for o in open_sells:
                cancel_order(market, "sell", BASE_SYMBOL, o, "taban/tepe fiyatına göre yeniden fiyatlanıyor")
            place_top_sell(market, BASE_SYMBOL, precision, price, sellable)
    else:
        our_top = max((float(o["price"]) for o in open_buys), default=None) if open_buys else None
        at_top = our_top is not None and best_bid is not None and our_top >= best_bid - 1e-12

        if at_top:
            log.info("%s: mevcut alış emrimiz (%.8f) zaten defterin tepesinde, bekleniyor.", BASE_SYMBOL, our_top)
        else:
            for o in open_buys:
                cancel_order(market, "buy", BASE_SYMBOL, o, "artık defterin tepesinde değil, yeniden fiyatlanıyor")

            reference = best_bid if best_bid is not None else (best_ask - tick if best_ask else None)
            if reference is None:
                log.warning("%s defterinde hiç emir yok; bu tur alış açılmıyor.", BASE_SYMBOL)
            else:
                target_price = reference + TOP_TICKS * tick
                if best_ask is not None:
                    max_price = best_ask - tick
                    if max_price <= 0:
                        log.warning("%s: spread çok dar/çakışık (best_ask=%s), alış açılmıyor.", BASE_SYMBOL, best_ask)
                        target_price = None
                    else:
                        target_price = min(target_price, max_price)
                if target_price is not None and target_price > 0:
                    target_price = round(target_price, 8)
                    quote_balance = get_balance(wallet, QUOTE_SYMBOL)
                    quote_budget = quote_balance * (MAX_ALLOCATION_PCT / 100.0)
                    log.info("%s bakiyesi: %.8f | bu tur bütçe: %.8f", QUOTE_SYMBOL, quote_balance, quote_budget)
                    place_top_buy(market, BASE_SYMBOL, precision, target_price, quote_budget)

    tx_ids = []
    if DRY_RUN:
        log.info("[DRY_RUN] Hiçbir şey gönderilmedi.")
    elif BUNDLE_MODE:
        count = pending_op_count(hive)
        if count:
            log.info("Kuyruktaki %d işlem tek seferde gönderiliyor…", count)
            try:
                result = hive.broadcast()
                trx_id = result.get("trx_id") if isinstance(result, dict) else None
                log.info("Gönderildi. id=%s", trx_id)
                if trx_id:
                    tx_ids = [("işlem #%d" % (i + 1), trx_id if i == 0 else "%s-%d" % (trx_id, i))
                              for i in range(count)]
            except Exception as e:
                log.exception("Toplu gönderim başarısız: %r", e)
                failed = True
        else:
            log.info("Kuyruğa eklenecek bir şey olmadı, gönderim yapılmadı.")
    else:
        tx_ids = list(SENT_TXS)
        log.info("Ayrı ayrı gönderilen işlem sayısı: %d", len(tx_ids))

    if tx_ids:
        log.info("Sonuçlar kontrol ediliyor…")
        if check_sidechain_results(api, tx_ids):
            failed = True

    if SEND_ERRORS:
        log.error("Gönderilemeyen işlemler: %s", ", ".join(SEND_ERRORS))
        failed = True

    log.info("Tamamlandı.")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    run()
