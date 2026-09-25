#!/usr/bin/env python3
"""
Otomatik emir yöneticisi.

MALİYET SİSTEMİ
---------------
Bu sürüm hareketli ortalama yerine FIFO lot maliyeti kullanır.

Her alış ayrı bir lot olarak tutulur:

    1.000 LTC @ 1000
    0.500 LTC @ 1200
    0.200 LTC @ 1500

Satışlarda önce en eski lot tüketilir.

Maliyet hesabında:
    alış fiyatı
    + BUY_FEE_PCT
    = gerçek alış maliyeti

Satış tabanı:

    gerekli brüt satış fiyatı =
        gerçek alış maliyeti
        * (1 + MIN_PROFIT_PCT / 100)
        / (1 - SELL_FEE_PCT / 100)

Böylece satış işlem ücretinden sonra da minimum kâr korunur.

GÜVENLİK
--------
Maliyet hesaplanamazsa SATIŞ YAPILMAZ.

STOP_LOSS_PCT = 0 ise zararına satış kesinlikle yapılmaz.

STOP_LOSS_PCT > 0 ise yalnızca kullanıcı açıkça etkinleştirmişse
zararına satış tabanı kaldırılabilir.

ENV:
    ACCOUNT_NAME
    ACCOUNT_KEY

    DRY_RUN=true
    BUNDLE_MODE=false

    BASE_SYMBOL=SWAP.LTC
    ORDER_SIZE_QUOTE=1
    MAX_ALLOCATION_PCT=20
    TOP_TICKS=1

    MIN_PROFIT_PCT=1
    BUY_FEE_PCT=0.75
    SELL_FEE_PCT=0.75

    STOP_LOSS_PCT=0

    DUST_THRESHOLD=0.000001
    KEEP_BASE_RESERVE=0

    SEND_DELAY_SECS=6

    HISTORY_URL=https://history.hive-engine.com/accountHistory
    HISTORY_PAGE_SIZE=500

ÖNEMLİ:
DRY_RUN=false yapmadan önce mutlaka DRY_RUN=true ile test edin.
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


# ============================================================================
# AYARLAR
# ============================================================================

ACCOUNT_NAME = os.environ.get("ACCOUNT_NAME", "").strip()
ACCOUNT_KEY = os.environ.get("ACCOUNT_KEY", "").strip()

DRY_RUN = os.environ.get("DRY_RUN", "true").strip().lower() != "false"
BUNDLE_MODE = os.environ.get("BUNDLE_MODE", "false").strip().lower() == "true"

SEND_DELAY_SECS = float(os.environ.get("SEND_DELAY_SECS", "6"))

QUOTE_SYMBOL = "SWAP.HIVE"
BASE_SYMBOL = os.environ.get(
    "BASE_SYMBOL",
    "SWAP.LTC"
).strip().upper()

ORDER_SIZE_QUOTE = float(
    os.environ.get("ORDER_SIZE_QUOTE", "1")
)

MAX_ALLOCATION_PCT = float(
    os.environ.get("MAX_ALLOCATION_PCT", "20")
)

TOP_TICKS = int(
    os.environ.get("TOP_TICKS", "1")
)

DUST_THRESHOLD = float(
    os.environ.get("DUST_THRESHOLD", "0.00000100")
)

KEEP_BASE_RESERVE = float(
    os.environ.get("KEEP_BASE_RESERVE", "0")
)

MIN_PROFIT_PCT = float(
    os.environ.get("MIN_PROFIT_PCT", "1")
)

# ---------------------------------------------------------------------------
# YENİ MALİYET AYARLARI
# ---------------------------------------------------------------------------

# Hive Engine işlemlerinde kullanılacak maliyet oranları.
#
# Varsayılan %0.75.
# Gerekirse GitHub Actions Secrets/Variables üzerinden değiştirilebilir.
#
# Örnek:
# BUY_FEE_PCT=0.75
# SELL_FEE_PCT=0.75

BUY_FEE_PCT = float(
    os.environ.get("BUY_FEE_PCT", "0.75")
)

SELL_FEE_PCT = float(
    os.environ.get("SELL_FEE_PCT", "0.75")
)

STOP_LOSS_PCT = float(
    os.environ.get("STOP_LOSS_PCT", "0")
)

HISTORY_URL = os.environ.get(
    "HISTORY_URL",
    "https://history.hive-engine.com/accountHistory"
)

HISTORY_PAGE_SIZE = int(
    os.environ.get("HISTORY_PAGE_SIZE", "500")
)

BOOK_SAMPLE_LIMIT = int(
    os.environ.get("BOOK_SAMPLE_LIMIT", "1000")
)


NODES = [
    "https://api.hive.blog",
    "https://anyx.io",
    "https://api.deathwing.me",
]


SENT_TXS = []
SEND_ERRORS = []

SENT_WORD = (
    "kuyruğa eklendi"
    if BUNDLE_MODE
    else "gönderildi"
)


# ============================================================================
# GENEL YARDIMCI FONKSİYONLAR
# ============================================================================

def fail(msg):
    log.error(msg)
    sys.exit(1)


def floor_to(value, precision):
    factor = 10 ** precision
    return math.floor(value * factor + 1e-9) / factor


def safe_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


_last_send_at = None


def pace():
    global _last_send_at

    if BUNDLE_MODE:
        return

    if _last_send_at is not None:
        wait = SEND_DELAY_SECS - (
            time.monotonic() - _last_send_at
        )

        if wait > 0:
            time.sleep(wait)

    _last_send_at = time.monotonic()


def record_tx(label, tx):
    if BUNDLE_MODE:
        return None

    trx_id = (
        tx.get("trx_id")
        if isinstance(tx, dict)
        else None
    )

    if trx_id:
        SENT_TXS.append((label, trx_id))
    else:
        log.warning(
            "%s için işlem kimliği alınamadı, "
            "sonuç kontrol edilemeyecek.",
            label,
        )

    return trx_id


# ============================================================================
# CLIENT
# ============================================================================

def load_clients():

    try:
        from nectar import Hive
        from nectarengine.api import Api
        from nectarengine.market import Market
        from nectarengine.wallet import Wallet

    except ImportError:
        fail(
            "Gerekli bağımlılıklar kurulu değil. "
            "`pip install -r requirements.txt` çalıştırın."
        )

    keys = [] if DRY_RUN else [ACCOUNT_KEY]

    hive = Hive(
        node=NODES,
        keys=keys,
        bundle=(BUNDLE_MODE and not DRY_RUN),
    )

    api = Api()

    market = Market(
        blockchain_instance=hive
    )

    wallet = Wallet(
        ACCOUNT_NAME,
        api=api
    )

    return hive, api, market, wallet


# ============================================================================
# ORDER BOOK
# ============================================================================

def get_book_top(api, symbol):

    buy_orders = api.find(
        "market",
        "buyBook",
        query={"symbol": symbol},
        limit=BOOK_SAMPLE_LIMIT,
    )

    sell_orders = api.find(
        "market",
        "sellBook",
        query={"symbol": symbol},
        limit=BOOK_SAMPLE_LIMIT,
    )

    if buy_orders and len(buy_orders) >= BOOK_SAMPLE_LIMIT:
        log.warning(
            "%s alış defterinde %d+ emir var; "
            "BOOK_SAMPLE_LIMIT'e takılmış olabilir.",
            symbol,
            BOOK_SAMPLE_LIMIT,
        )

    if sell_orders and len(sell_orders) >= BOOK_SAMPLE_LIMIT:
        log.warning(
            "%s satış defterinde %d+ emir var; "
            "BOOK_SAMPLE_LIMIT'e takılmış olabilir.",
            symbol,
            BOOK_SAMPLE_LIMIT,
        )

    best_bid = (
        max(
            (
                safe_float(o.get("price"))
                for o in buy_orders
                if safe_float(o.get("price")) is not None
            ),
            default=None,
        )
        if buy_orders
        else None
    )

    best_ask = (
        min(
            (
                safe_float(o.get("price"))
                for o in sell_orders
                if safe_float(o.get("price")) is not None
            ),
            default=None,
        )
        if sell_orders
        else None
    )

    return best_bid, best_ask


def get_precision(api, symbol):

    info = api.find_one(
        "tokens",
        "tokens",
        query={"symbol": symbol},
    )

    if not info:
        return 8

    return int(info["precision"])


# ============================================================================
# HISTORY
# ============================================================================

def fetch_history(
    account,
    symbol,
    limit=None,
    max_pages=None,
):
    """
    Hesap geçmişini sayfa sayfa okur.

    Eski sürümde sabit 8 sayfa vardı:
        8 x 500 = 4000 kayıt.

    Bu sürüm stok geçmişini kesmemek için varsayılan olarak
    son sayfaya kadar devam eder.

    max_pages verilirse güvenlik amacıyla sınır koyulabilir.
    """

    import requests

    if limit is None:
        limit = HISTORY_PAGE_SIZE

    rows = []

    offset = 0
    page = 0

    while True:

        if max_pages is not None and page >= max_pages:
            log.warning(
                "History max_pages sınırına ulaşıldı: %s",
                max_pages,
            )
            break

        r = requests.get(
            HISTORY_URL,
            params={
                "account": account,
                "symbol": symbol,
                "limit": limit,
                "offset": offset,
            },
            timeout=30,
        )

        r.raise_for_status()

        chunk = r.json()

        if not chunk:
            break

        if not isinstance(chunk, list):
            raise ValueError(
                "History API beklenmeyen format döndürdü."
            )

        rows.extend(chunk)

        log.debug(
            "History sayfa=%d offset=%d kayıt=%d toplam=%d",
            page,
            offset,
            len(chunk),
            len(rows),
        )

        if len(chunk) < limit:
            break

        offset += limit
        page += 1

    log.info(
        "%s geçmişinden %d işlem kaydı okundu.",
        symbol,
        len(rows),
    )

    return rows


# ============================================================================
# FIFO MALİYET SİSTEMİ
# ============================================================================

def get_timestamp(row):
    """
    History timestamp formatı farklı gelebileceği için
    mümkün olduğunca güvenli sıralama anahtarı üretir.
    """

    value = row.get("timestamp", 0)

    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def extract_trade_values(row):

    q = safe_float(
        row.get("quantityTokens")
    )

    h = safe_float(
        row.get("quantityHive")
    )

    if q is None or h is None:
        return None, None

    if q <= 0 or h <= 0:
        return None, None

    return q, h


def fifo_lots_from_rows(rows, symbol):
    """
    Geçmiş market_buy / market_sell kayıtlarından
    elde kalan lotları FIFO ile hesaplar.

    Her lot:

        {
            "qty": kalan token,
            "unit_cost": gerçek alış maliyeti,
            "total_cost": kalan toplam maliyet
        }

    Alış:
        unit_cost = (ödenen HIVE / alınan token)
                    * (1 + BUY_FEE_PCT / 100)

    Satış:
        en eski lotlardan düşülür.

    Böylece hareketli ortalama kullanılmaz.
    """

    lots = []

    relevant = []

    for row in rows:

        if row.get("symbol") != symbol:
            continue

        operation = row.get("operation")

        if operation not in (
            "market_buy",
            "market_sell",
        ):
            continue

        relevant.append(row)

    relevant.sort(key=get_timestamp)

    for row in relevant:

        operation = row.get("operation")

        q, h = extract_trade_values(row)

        if q is None or h is None:
            continue

        # ------------------------------------------------------------------
        # ALIŞ
        # ------------------------------------------------------------------

        if operation == "market_buy":

            gross_unit_cost = h / q

            # Alış maliyetine işlem ücretini ekle.
            unit_cost = (
                gross_unit_cost
                * (1 + BUY_FEE_PCT / 100.0)
            )

            lots.append(
                {
                    "qty": q,
                    "unit_cost": unit_cost,
                    "total_cost": q * unit_cost,
                    "timestamp": get_timestamp(row),
                }
            )

        # ------------------------------------------------------------------
        # SATIŞ
        # ------------------------------------------------------------------

        elif operation == "market_sell":

            remaining_to_sell = q

            while (
                remaining_to_sell > 1e-12
                and lots
            ):

                lot = lots[0]

                consumed = min(
                    remaining_to_sell,
                    lot["qty"],
                )

                lot["qty"] -= consumed

                lot["total_cost"] = (
                    lot["qty"]
                    * lot["unit_cost"]
                )

                remaining_to_sell -= consumed

                if lot["qty"] <= 1e-12:
                    lots.pop(0)

            # History'de satış, kayıtlı alışlardan fazla ise:
            #
            # Bu durum:
            #   - history eksikliği
            #   - transfer
            #   - deposit
            #   - başka bir kaynak
            #
            # anlamına gelebilir.
            #
            # Eksik maliyetli kısmı uydurmuyoruz.
            if remaining_to_sell > 1e-10:

                log.warning(
                    "FIFO uyarısı: %.12f %s satışının "
                    "karşılığı geçmiş alışlarda bulunamadı. "
                    "History eksik olabilir.",
                    remaining_to_sell,
                    symbol,
                )

    return lots


def fifo_cost_from_rows(rows, symbol):
    """
    Geçmiş trade kayıtlarından FIFO ile kalan lotları hesaplar.

    ÖNEMLİ: Bu fonksiyon yalnızca HISTORY'nin gösterdiği
    envanteri hesaplar. Cüzdandaki gerçek bakiye ile eşleşip
    eşleşmediğine burada karar verilmez. Bu kontrol run() içinde
    ayrıca yapılır.

    Böylece history'de 0.0439 token kalmış görünürken cüzdanda
    0.000993 token varsa, bu iki sayı sessizce birbirine
    dönüştürülmez ve sahte bir maliyet üretilmez.
    """

    lots = fifo_lots_from_rows(rows, symbol)

    if not lots:
        return None

    total_qty = sum(lot["qty"] for lot in lots)
    total_cost = sum(lot["total_cost"] for lot in lots)

    if total_qty <= 1e-12:
        return None

    return {
        "qty": total_qty,
        "total_cost": total_cost,
        "avg_cost": total_cost / total_qty,
        "lots": lots,
    }


def order_quantity(order):
    """Açık emirdeki token miktarını mümkün formatlardan okur."""
    for key in ("quantity", "amount", "tokens"):
        value = safe_float(order.get(key))
        if value is not None and value > 0:
            return value
    return 0.0


def open_sell_quantity(orders):
    """Açık satış emirlerinde kilitli olabilecek token miktarını toplar."""
    return sum(order_quantity(o) for o in orders)


def reconcile_fifo_with_wallet(cost_info, wallet_qty, locked_sell_qty=0.0, tolerance=1e-8):
    """
    History FIFO envanterini gerçek cüzdan envanteriyle doğrular.

    Normal durumda:
        FIFO kalan = kullanılabilir bakiye + açık satışlarda kilitli miktar

    Eşleşmiyorsa maliyet bilinmiyor kabul edilir. Eksik/fazla kısmın
    hangi lota ait olduğu tahmin edilmez. Bu, yanlış ortalama maliyet
    üretmekten daha güvenlidir.
    """
    if cost_info is None:
        return None, "history'de maliyetlendirilebilir lot yok"

    actual_qty = max(0.0, wallet_qty) + max(0.0, locked_sell_qty)
    history_qty = max(0.0, cost_info["qty"])
    difference = history_qty - actual_qty

    if abs(difference) > tolerance:
        return None, (
            "envanter uyuşmazlığı: "
            f"history={history_qty:.12f}, "
            f"cüzdan+kilitli_satış={actual_qty:.12f}, "
            f"fark={difference:.12f}"
        )

    # Küçük floating-point farklarını gerçek bakiye ile orantılı
    # şekilde ölçeklendiriyoruz. Maliyet oranı aynı kaldığı için
    # mevcut gerçek miktarın toplam maliyeti buna göre küçültülür.
    scale = (
        actual_qty / history_qty
        if history_qty > 1e-12
        else 0.0
    )

    matched_qty = actual_qty
    matched_cost = cost_info["total_cost"] * scale
    matched_avg = (
        matched_cost / matched_qty
        if matched_qty > 1e-12
        else None
    )

    return {
        "qty": matched_qty,
        "total_cost": matched_cost,
        "avg_cost": matched_avg,
        "lots": cost_info["lots"],
    }, None


# ============================================================================
# SATIŞ MALİYETİ
# ============================================================================

def calculate_safe_sell_price(cost, min_profit_pct=None):
    """
    Satıştan sonra SELL_FEE düşüldüğünde bile
    gerçek maliyet + minimum kâr kalmasını sağlayan
    minimum brüt satış fiyatı.

    Örnek:

        maliyet = 1000
        kâr     = %1
        satış ücreti = %0.75

        net gerekli = 1010

        brüt satış =
            1010 / 0.9925
            ≈ 1017.63
    """

    if cost is None or cost <= 0:
        return None

    if min_profit_pct is None:
        min_profit_pct = MIN_PROFIT_PCT

    target_net = (
        cost
        * (1 + min_profit_pct / 100.0)
    )

    sell_factor = (
        1 - SELL_FEE_PCT / 100.0
    )

    if sell_factor <= 0:
        raise ValueError(
            "SELL_FEE_PCT %100 veya daha yüksek olamaz."
        )

    return target_net / sell_factor


def calculate_net_after_sell(
    sell_price,
    cost,
):
    """
    Birim başına satıştan sonra kalan net HIVE
    ve maliyet sonrası kâr.
    """

    net = (
        sell_price
        * (1 - SELL_FEE_PCT / 100.0)
    )

    profit = net - cost

    profit_pct = (
        profit / cost * 100.0
        if cost > 0
        else None
    )

    return net, profit, profit_pct


# ============================================================================
# SATIŞ PLANLAMA
# ============================================================================

def plan_sell(
    best_bid,
    best_ask,
    our_top_ask,
    cost,
    tick,
):
    """
    Güvenli satış planı.

    KRİTİK:
    Buradaki 'floor' gerçek güvenli minimum satış fiyatıdır.

    STOP_LOSS_PCT=0:
        floor hiçbir koşulda kaldırılmaz.

    STOP_LOSS_PCT>0:
        best_bid yeterince aşağıdaysa kullanıcı tarafından
        etkinleştirilmiş stop-loss mekanizması devreye girebilir.
    """

    eps = 1e-12

    if cost is None or cost <= 0:
        return (
            "wait",
            None,
            "maliyet bulunamadı; güvenlik nedeniyle satış yok",
        )

    floor = calculate_safe_sell_price(cost)

    if floor is None:
        return (
            "wait",
            None,
            "güvenli satış tabanı hesaplanamadı",
        )

    note = ""

    # ------------------------------------------------------------------
    # STOP LOSS
    # ------------------------------------------------------------------

    if (
        STOP_LOSS_PCT > 0
        and best_bid is not None
        and best_bid
        < cost * (
            1 - STOP_LOSS_PCT / 100.0
        )
    ):
        floor = None
        note = (
            "STOP-LOSS devrede, "
            "kullanıcı ayarı nedeniyle taban kaldırıldı"
        )

    # ------------------------------------------------------------------
    # MEVCUT AÇIK SATIŞ
    # ------------------------------------------------------------------

    at_top = (
        our_top_ask is not None
        and best_ask is not None
        and our_top_ask
        <= best_ask + eps
    )

    if our_top_ask is not None:

        if floor is None:

            if at_top:
                return (
                    "keep",
                    our_top_ask,
                    note,
                )

        else:

            # Mevcut emir güvenli tabanın altındaysa
            # KESİNLİKLE korunmaz.
            if our_top_ask < floor * (
                1 - 1e-9
            ):
                return (
                    "place",
                    None,
                    "mevcut satış güvenli maliyet tabanının altında",
                )

            if (
                our_top_ask >= floor * (
                    1 - 1e-9
                )
                and (
                    at_top
                    or our_top_ask
                    <= floor * (
                        1 + 1e-9
                    )
                )
            ):
                return (
                    "keep",
                    our_top_ask,
                    "güvenli taban/tepe fiyatında bekleniyor",
                )

    # ------------------------------------------------------------------
    # DEFTER REFERANSI
    # ------------------------------------------------------------------

    reference = (
        best_ask
        if best_ask is not None
        else (
            best_bid + tick
            if best_bid is not None
            else None
        )
    )

    if reference is None:
        return (
            "wait",
            None,
            "defterde referans yok",
        )

    target = (
        reference
        - TOP_TICKS * tick
    )

    # Satış fiyatı best bid'in altında olamaz.
    if best_bid is not None:

        min_price = best_bid + tick

        if min_price >= reference:

            # Eğer güvenli taban spread'in üzerinde ise,
            # beklemek daha güvenlidir.
            if floor is not None and floor > reference:
                return (
                    "wait",
                    None,
                    "spread dar ve güvenli satış tabanı üstte",
                )

            return (
                "wait",
                None,
                "spread çok dar/çakışık",
            )

        target = max(
            target,
            min_price,
        )

    # ------------------------------------------------------------------
    # EN ÖNEMLİ KONTROL
    # ------------------------------------------------------------------

    if floor is not None:

        target = max(
            target,
            floor,
        )

        # İkinci bağımsız güvenlik kontrolü.
        if target < floor * (
            1 - 1e-9
        ):
            return (
                "wait",
                None,
                "GÜVENLİK: hedef fiyat maliyet tabanının altında",
            )

    if target <= 0:
        return (
            "wait",
            None,
            "geçersiz fiyat",
        )

    return (
        "place",
        round(target, 8),
        note,
    )


# ============================================================================
# OPEN ORDERS
# ============================================================================

def get_our_open_buy(market, symbol):

    try:
        return (
            market.get_buy_book(
                symbol,
                account=ACCOUNT_NAME,
            )
            or []
        )

    except Exception as e:

        log.warning(
            "Açık alış emirleri okunamadı (%s): %s",
            symbol,
            e,
        )

        return []


def get_our_open_sell(market, symbol):

    try:
        return (
            market.get_sell_book(
                symbol,
                account=ACCOUNT_NAME,
            )
            or []
        )

    except Exception as e:

        log.warning(
            "Açık satış emirleri okunamadı (%s): %s",
            symbol,
            e,
        )

        return []


def cancel_order(
    market,
    order_type,
    symbol,
    order,
    reason,
):

    oid = (
        order.get("txId")
        or order.get("_id")
    )

    if not oid:

        log.warning(
            "%s %s emrinde id bulunamadı, atlandı: %r",
            symbol,
            order_type,
            order,
        )

        return

    if DRY_RUN:

        log.info(
            "[DRY_RUN] %s %s emri iptal edilirdi "
            "(id=%s) — sebep: %s",
            symbol,
            order_type,
            oid,
            reason,
        )

        return

    pace()

    try:

        tx = market.cancel(
            ACCOUNT_NAME,
            order_type,
            oid,
        )

        log.info(
            "%s %s iptali %s (id=%s) — sebep: %s",
            symbol,
            order_type,
            SENT_WORD,
            oid,
            reason,
        )

        record_tx(
            "%s %s iptali"
            % (symbol, order_type),
            tx,
        )

    except Exception as e:

        log.warning(
            "%s %s iptali gönderilemedi "
            "(id=%s): %r",
            symbol,
            order_type,
            oid,
            e,
        )

        SEND_ERRORS.append(
            "%s %s iptali"
            % (symbol, order_type)
        )


# ============================================================================
# BALANCE
# ============================================================================

def get_balance(wallet, symbol):

    try:

        bal = wallet.get_token(symbol)

        if not bal:
            return 0.0

        return float(
            bal.get("balance", 0)
        )

    except Exception as e:

        log.warning(
            "%s bakiyesi okunamadı: %s",
            symbol,
            e,
        )

        return 0.0


# ============================================================================
# ORDER PLACEMENT
# ============================================================================

def place_top_sell(
    market,
    symbol,
    precision,
    sell_price,
    amount,
    safe_floor=None,
):
    """
    Satış gönderilmeden hemen önce ikinci maliyet kontrolü.

    Bu kontrol plan_sell'den bağımsızdır.
    """

    if sell_price is None:
        log.error(
            "%s: satış fiyatı None; satış iptal.",
            symbol,
        )
        return False

    if safe_floor is not None:

        if sell_price < safe_floor * (
            1 - 1e-9
        ):

            log.error(
                "GÜVENLİK ENGELİ: %s satış fiyatı %.12f "
                "güvenli taban %.12f altında. "
                "SATIŞ GÖNDERİLMEDİ.",
                symbol,
                sell_price,
                safe_floor,
            )

            return False

    sell_amount = floor_to(
        amount,
        precision,
    )

    if sell_amount <= 0:

        log.info(
            "%s: satılacak miktar 0 "
            "(precision/dust nedeniyle), atlandı.",
            symbol,
        )

        return False

    if DRY_RUN:

        log.info(
            "[DRY_RUN] SATIŞ %s %s @ %s "
            "— açılır ve BEKLENİR",
            sell_amount,
            symbol,
            sell_price,
        )

        return True

    pace()

    try:

        tx = market.sell(
            ACCOUNT_NAME,
            sell_amount,
            symbol,
            sell_price,
        )

        log.info(
            "SATIŞ %s: %s %s @ %s "
            "— açıldı, bekleniyor",
            SENT_WORD,
            sell_amount,
            symbol,
            sell_price,
        )

        record_tx(
            "%s satış" % symbol,
            tx,
        )

        return True

    except Exception as e:

        log.error(
            "Satış emri gönderilemedi (%s): %r",
            symbol,
            e,
        )

        SEND_ERRORS.append(
            "%s satış" % symbol
        )

        return False


def place_top_buy(
    market,
    symbol,
    precision,
    buy_price,
    quote_budget,
):

    spend = min(
        ORDER_SIZE_QUOTE,
        quote_budget,
    )

    buy_amount = (
        floor_to(
            spend / buy_price,
            precision,
        )
        if buy_price > 0
        else 0
    )

    if buy_amount <= 0:

        log.info(
            "%s için alış emri atlandı "
            "(bütçe/precision nedeniyle miktar 0).",
            symbol,
        )

        return

    if DRY_RUN:

        log.info(
            "[DRY_RUN] TEPE ALIŞ %s %s @ %s "
            "(harcanacak ~%.8f) "
            "— açılır ve BEKLENİR",
            buy_amount,
            symbol,
            buy_price,
            spend,
        )

        return

    pace()

    try:

        tx = market.buy(
            ACCOUNT_NAME,
            buy_amount,
            symbol,
            buy_price,
        )

        log.info(
            "TEPE ALIŞ %s: %s %s @ %s "
            "(~%.8f) — açıldı, bekleniyor",
            SENT_WORD,
            buy_amount,
            symbol,
            buy_price,
            spend,
        )

        record_tx(
            "%s tepe alış" % symbol,
            tx,
        )

    except Exception as e:

        log.error(
            "Alış emri gönderilemedi (%s): %r",
            symbol,
            e,
        )

        SEND_ERRORS.append(
            "%s alış" % symbol
        )


# ============================================================================
# KEY CHECK
# ============================================================================

def verify_signing_key(hive):

    try:

        from nectar.account import Account
        from nectargraphenebase.account import PrivateKey

        my_pub = str(
            PrivateKey(ACCOUNT_KEY).pubkey
        )[3:]

        acc = Account(
            ACCOUNT_NAME,
            blockchain_instance=hive,
        )

        signing_keys = set()

        for role in (
            "active",
            "owner",
        ):

            for entry in acc[role]["key_auths"]:

                signing_keys.add(
                    str(entry[0])[3:]
                )

        posting_keys = {
            str(e[0])[3:]
            for e in acc["posting"]["key_auths"]
        }

        memo_key = str(
            acc["memo_key"]
        )[3:]

    except Exception as e:

        log.warning(
            "Anahtar ön kontrolü yapılamadı, "
            "devam ediliyor: %r",
            e,
        )

        return

    if my_pub in signing_keys:

        log.info(
            "Anahtar doğrulandı: ACCOUNT_KEY, "
            "%s hesabının yetkili anahtarına ait.",
            ACCOUNT_NAME,
        )

        return

    if my_pub in posting_keys:

        fail(
            "ACCOUNT_KEY olarak yetkisiz bir anahtar "
            "girilmiş (posting). Active/owner düzeyinde "
            "anahtar gerekir."
        )

    if my_pub == memo_key:

        fail(
            "ACCOUNT_KEY olarak yetkisiz bir anahtar "
            "girilmiş (memo). Active/owner düzeyinde "
            "anahtar gerekir."
        )

    fail(
        "ACCOUNT_KEY, %s hesabının yetkili "
        "anahtarlarından hiçbiriyle eşleşmiyor. "
        "Anahtarı ve ACCOUNT_NAME değerini kontrol edin."
        % ACCOUNT_NAME
    )


# ============================================================================
# SIDECHAIN RESULT
# ============================================================================

def pending_op_count(hive):

    try:

        ops = getattr(
            getattr(hive, "txbuffer", None),
            "ops",
            None,
        )

        return (
            None
            if ops is None
            else len(ops)
        )

    except Exception:

        return None


def check_sidechain_results(
    api,
    tx_ids,
    wait_rounds=6,
    wait_secs=5,
):

    pending = list(tx_ids)

    rejected = False

    for _ in range(wait_rounds):

        time.sleep(wait_secs)

        still_pending = []

        for label, txid in pending:

            try:

                info = api.get_transaction_info(
                    txid
                )

            except Exception as e:

                log.warning(
                    "%s sonucu sorgulanamadı "
                    "(%s): %r",
                    label,
                    txid,
                    e,
                )

                still_pending.append(
                    (label, txid)
                )

                continue

            if not info:

                still_pending.append(
                    (label, txid)
                )

                continue

            logs = (
                info.get("logs")
                if isinstance(info, dict)
                else None
            )

            if isinstance(logs, str):

                try:
                    logs = json.loads(logs)

                except ValueError:

                    logs = {
                        "errors": [logs]
                    }

            errors = (
                logs.get("errors")
                if isinstance(logs, dict)
                else None
            )

            if errors:

                log.error(
                    "REDDEDİLDİ: %s (%s): %s",
                    label,
                    txid,
                    "; ".join(
                        str(x)
                        for x in errors
                    ),
                )

                rejected = True

            else:

                log.info(
                    "Kabul edildi: %s (%s)",
                    label,
                    txid,
                )

        pending = still_pending

        if not pending:
            break

    for label, txid in pending:

        log.warning(
            "Sonuç henüz görünmedi: %s (%s). "
            "Daha sonra kontrol edin.",
            label,
            txid,
        )

    return rejected


# ============================================================================
# ANA PROGRAM
# ============================================================================

def run():

    if not ACCOUNT_NAME:
        fail(
            "ACCOUNT_NAME ortam değişkeni / secret ayarlanmamış."
        )

    if not DRY_RUN and not ACCOUNT_KEY:
        fail(
            "DRY_RUN=false iken ACCOUNT_KEY zorunludur."
        )

    # Fee doğrulaması
    if BUY_FEE_PCT < 0:
        fail("BUY_FEE_PCT negatif olamaz.")

    if SELL_FEE_PCT < 0 or SELL_FEE_PCT >= 100:
        fail(
            "SELL_FEE_PCT 0 ile 100 arasında olmalıdır."
        )

    log.info(
        "Başlıyor. Hesap=%s Sembol=%s/%s "
        "DRY_RUN=%s BUNDLE=%s "
        "ORDER_SIZE=%s MAX_ALLOC=%%%s "
        "MIN_PROFIT=%%%s STOP_LOSS=%%%s "
        "BUY_FEE=%%%s SELL_FEE=%%%s",
        ACCOUNT_NAME,
        BASE_SYMBOL,
        QUOTE_SYMBOL,
        DRY_RUN,
        BUNDLE_MODE,
        ORDER_SIZE_QUOTE,
        MAX_ALLOCATION_PCT,
        MIN_PROFIT_PCT,
        STOP_LOSS_PCT,
        BUY_FEE_PCT,
        SELL_FEE_PCT,
    )

    hive, api, market, wallet = load_clients()

    if not DRY_RUN:
        verify_signing_key(hive)

    precision = get_precision(
        api,
        BASE_SYMBOL,
    )

    tick = 10 ** (-precision)

    best_bid, best_ask = get_book_top(
        api,
        BASE_SYMBOL,
    )

    log.info(
        "%s/%s defteri: "
        "best_bid=%s best_ask=%s "
        "(precision=%s)",
        BASE_SYMBOL,
        QUOTE_SYMBOL,
        best_bid,
        best_ask,
        precision,
    )

    base_balance = get_balance(
        wallet,
        BASE_SYMBOL,
    )

    sellable = max(
        0.0,
        base_balance - KEEP_BASE_RESERVE,
    )

    log.info(
        "%s bakiyesi: %.8f "
        "(rezerv hariç satılabilir: %.8f)",
        BASE_SYMBOL,
        base_balance,
        sellable,
    )

    failed = False

    open_buys = get_our_open_buy(
        market,
        BASE_SYMBOL,
    )

    open_sells = get_our_open_sell(
        market,
        BASE_SYMBOL,
    )

    log.info(
        "%s için %d açık alış, "
        "%d açık satış emrimiz var.",
        BASE_SYMBOL,
        len(open_buys),
        len(open_sells),
    )

    # ========================================================================
    # SATIŞ
    # ========================================================================

    if sellable > DUST_THRESHOLD:

        # Elimizde stok varken yeni alış emri bırakma.
        for o in open_buys:

            cancel_order(
                market,
                "buy",
                BASE_SYMBOL,
                o,
                "envanteri satmadan önce "
                "bekleyen alış emri temizleniyor",
            )

        # --------------------------------------------------------------------
        # FIFO MALİYETİNİ HESAPLA
        # --------------------------------------------------------------------

        cost_info = None

        try:

            rows = fetch_history(
                ACCOUNT_NAME,
                BASE_SYMBOL,
            )

            raw_cost_info = fifo_cost_from_rows(
                rows,
                BASE_SYMBOL,
            )

            locked_sell_qty = open_sell_quantity(
                open_sells
            )

            cost_info, reconcile_error = reconcile_fifo_with_wallet(
                raw_cost_info,
                sellable,
                locked_sell_qty=locked_sell_qty,
            )

            if raw_cost_info is not None:
                log.info(
                    "%s envanter mutabakatı: history FIFO=%.12f | "
                    "cüzdan=%.12f | açık satış kilidi=%.12f",
                    BASE_SYMBOL,
                    raw_cost_info["qty"],
                    sellable,
                    locked_sell_qty,
                )

            if reconcile_error:
                log.error(
                    "GÜVENLİK: %s %s",
                    BASE_SYMBOL,
                    reconcile_error,
                )
                log.error(
                    "History ile gerçek bakiye eşleşmediği için "
                    "maliyet güvenilir değil. SATIŞ YAPILMAYACAK."
                )

        except Exception as e:

            log.error(
                "Geçmiş okunamadı, "
                "maliyet bilinmiyor; "
                "bu tur SATIŞ YAPILMAYACAK: %r",
                e,
            )

            cost_info = None

        # --------------------------------------------------------------------
        # MALİYET BULUNAMADI
        # --------------------------------------------------------------------

        if cost_info is None:

            log.error(
                "GÜVENLİK: %s için FIFO maliyeti "
                "hesaplanamadı. SATIŞ EMRİ GÖNDERİLMİYOR.",
                BASE_SYMBOL,
            )

            action = "wait"
            price = None

        else:

            fifo_qty = cost_info["qty"]
            total_cost = cost_info["total_cost"]
            cost = cost_info["avg_cost"]

            if cost is None or cost <= 0:
                log.error(
                    "GÜVENLİK: %s için geçerli maliyet bulunamadı. "
                    "SATIŞ YAPILMAYACAK.",
                    BASE_SYMBOL,
                )
                action = "wait"
                price = None
                note = "geçerli maliyet yok"
                safe_floor = None
            else:
                safe_floor = calculate_safe_sell_price(cost)

            log.info(
                "%s FIFO maliyeti (doğrulanmış mevcut envanter): "
                "kalan stok=%.12f "
                "toplam maliyet=%.8f "
                "ortalama gerçek maliyet=%.8f",
                BASE_SYMBOL,
                fifo_qty,
                total_cost,
                cost,
            )

            log.info(
                "%s maliyet hesabı: "
                "alış ücreti=%%%s "
                "satış ücreti=%%%s "
                "min kâr=%%%s",
                BASE_SYMBOL,
                BUY_FEE_PCT,
                SELL_FEE_PCT,
                MIN_PROFIT_PCT,
            )

            if safe_floor is not None:

                net_at_floor, profit_at_floor, profit_pct = (
                    calculate_net_after_sell(
                        safe_floor,
                        cost,
                    )
                )

                log.info(
                    "%s GÜVENLİ SATIŞ TABANI: %.8f "
                    "| satış sonrası net: %.8f "
                    "| kâr: %.8f "
                    "(%%%s)",
                    BASE_SYMBOL,
                    safe_floor,
                    net_at_floor,
                    profit_at_floor,
                    (
                        "%.4f" % profit_pct
                        if profit_pct is not None
                        else "?"
                    ),
                )

            our_top_ask = (
                min(
                    (
                        safe_float(
                            o.get("price")
                        )
                        for o in open_sells
                        if safe_float(
                            o.get("price")
                        ) is not None
                    ),
                    default=None,
                )
                if open_sells
                else None
            )

            action, price, note = plan_sell(
                best_bid,
                best_ask,
                our_top_ask,
                cost,
                tick,
            )

            log.info(
                "Satış planı: %s @ %s %s",
                action,
                price,
                (
                    "(" + note + ")"
                    if note
                    else ""
                ),
            )

            # ----------------------------------------------------------------
            # SATIŞ EMRİ
            # ----------------------------------------------------------------

            if action == "place":

                # plan_sell bazı durumlarda None döndürür.
                if price is None:

                    # Burada mevcut açık emirleri
                    # güvenli taban altındaysa iptal etmek
                    # yerine satıştan tamamen çıkıyoruz.
                    #
                    # Böylece yanlışlıkla eski emri iptal edip
                    # daha kötü bir emir açma riski oluşmaz.
                    log.warning(
                        "%s: satış planı fiyat üretmedi. "
                        "Açık satış emirlerine dokunulmuyor.",
                        BASE_SYMBOL,
                    )

                else:

                    # --------------------------------------------------------
                    # KRİTİK İKİNCİ GÜVENLİK
                    # --------------------------------------------------------

                    safe_floor = calculate_safe_sell_price(
                        cost
                    )

                    if (
                        safe_floor is None
                        or price < safe_floor * (
                            1 - 1e-9
                        )
                    ):

                        log.error(
                            "GÜVENLİK: planlanan satış "
                            "%.12f güvenli taban "
                            "%.12f altında. "
                            "EMİR GÖNDERİLMEDİ.",
                            price,
                            safe_floor
                            if safe_floor is not None
                            else -1,
                        )

                    else:

                        # Mevcut satışları yeniden fiyatla.
                        for o in open_sells:

                            cancel_order(
                                market,
                                "sell",
                                BASE_SYMBOL,
                                o,
                                "güvenli maliyet tabanı "
                                "ve defter tepesine göre "
                                "yeniden fiyatlanıyor",
                            )

                        # ----------------------------------------------------
                        # KRİTİK ÜÇÜNCÜ GÜVENLİK
                        # ----------------------------------------------------

                        place_top_sell(
                            market,
                            BASE_SYMBOL,
                            precision,
                            price,
                            sellable,
                            safe_floor=safe_floor,
                        )

            elif action == "keep":

                log.info(
                    "%s: mevcut satış emri korunuyor.",
                    BASE_SYMBOL,
                )

            else:

                log.info(
                    "%s: satış bekletiliyor.",
                    BASE_SYMBOL,
                )

    # ========================================================================
    # ALIŞ
    # ========================================================================

    else:

        our_top = (
            max(
                (
                    safe_float(
                        o.get("price")
                    )
                    for o in open_buys
                    if safe_float(
                        o.get("price")
                    ) is not None
                ),
                default=None,
            )
            if open_buys
            else None
        )

        at_top = (
            our_top is not None
            and best_bid is not None
            and our_top >= best_bid - 1e-12
        )

        if at_top:

            log.info(
                "%s: mevcut alış emrimiz "
                "(%.8f) zaten defterin tepesinde, "
                "bekleniyor.",
                BASE_SYMBOL,
                our_top,
            )

        else:

            for o in open_buys:

                cancel_order(
                    market,
                    "buy",
                    BASE_SYMBOL,
                    o,
                    "artık defterin tepesinde değil, "
                    "yeniden fiyatlanıyor",
                )

            reference = (
                best_bid
                if best_bid is not None
                else (
                    best_ask - tick
                    if best_ask
                    else None
                )
            )

            if reference is None:

                log.warning(
                    "%s defterinde hiç emir yok; "
                    "bu tur alış açılmıyor.",
                    BASE_SYMBOL,
                )

            else:

                target_price = (
                    reference
                    + TOP_TICKS * tick
                )

                if best_ask is not None:

                    max_price = (
                        best_ask - tick
                    )

                    if max_price <= 0:

                        log.warning(
                            "%s: spread çok dar/çakışık "
                            "(best_ask=%s), alış açılmıyor.",
                            BASE_SYMBOL,
                            best_ask,
                        )

                        target_price = None

                    else:

                        target_price = min(
                            target_price,
                            max_price,
                        )

                if (
                    target_price is not None
                    and target_price > 0
                ):

                    target_price = round(
                        target_price,
                        8,
                    )

                    quote_balance = get_balance(
                        wallet,
                        QUOTE_SYMBOL,
                    )

                    quote_budget = (
                        quote_balance
                        * (
                            MAX_ALLOCATION_PCT
                            / 100.0
                        )
                    )

                    log.info(
                        "%s bakiyesi: %.8f "
                        "| bu tur bütçe: %.8f",
                        QUOTE_SYMBOL,
                        quote_balance,
                        quote_budget,
                    )

                    place_top_buy(
                        market,
                        BASE_SYMBOL,
                        precision,
                        target_price,
                        quote_budget,
                    )

    # ========================================================================
    # TRANSACTION RESULTS
    # ========================================================================

    tx_ids = []

    if DRY_RUN:

        log.info(
            "[DRY_RUN] Hiçbir şey gönderilmedi."
        )

    elif BUNDLE_MODE:

        count = pending_op_count(hive)

        if count:

            log.info(
                "Kuyruktaki %d işlem tek seferde "
                "gönderiliyor…",
                count,
            )

            try:

                result = hive.broadcast()

                trx_id = (
                    result.get("trx_id")
                    if isinstance(result, dict)
                    else None
                )

                log.info(
                    "Gönderildi. id=%s",
                    trx_id,
                )

                if trx_id:

                    tx_ids = [
                        (
                            "işlem #%d" % (i + 1),
                            (
                                trx_id
                                if i == 0
                                else "%s-%d"
                                % (trx_id, i)
                            ),
                        )
                        for i in range(count)
                    ]

            except Exception as e:

                log.exception(
                    "Toplu gönderim başarısız: %r",
                    e,
                )

                failed = True

        else:

            log.info(
                "Kuyruğa eklenecek bir şey olmadı, "
                "gönderim yapılmadı."
            )

    else:

        tx_ids = list(SENT_TXS)

        log.info(
            "Ayrı ayrı gönderilen işlem sayısı: %d",
            len(tx_ids),
        )

    if tx_ids:

        log.info(
            "Sonuçlar kontrol ediliyor…"
        )

        if check_sidechain_results(
            api,
            tx_ids,
        ):

            failed = True

    if SEND_ERRORS:

        log.error(
            "Gönderilemeyen işlemler: %s",
            ", ".join(SEND_ERRORS),
        )

        failed = True

    log.info("Tamamlandı.")

    if failed:
        sys.exit(1)


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    run()
