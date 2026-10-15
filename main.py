import os
import re
import requests
import sqlite3
import secrets
import logging
import asyncio
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks

MSK = timezone(timedelta(hours=3))
REPORT_HOUR = 20
REPORT_MINUTE = 30


@asynccontextmanager
async def lifespan(app):
    # The scheduler lives in the existing Amvera app; no outside scheduler is needed.
    task = asyncio.create_task(daily_report_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)

# Pending button actions are stored on Amvera's persistent /data volume.
READY_ACTIONS_DB = os.getenv("READY_ACTIONS_DB", "/data/ready_actions.sqlite3")
logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_SECRET_TOKEN = os.getenv("TELEGRAM_SECRET_TOKEN", "").strip()
BITRIX_WEBHOOK_BASE = os.getenv("BITRIX_WEBHOOK_BASE", "").rstrip("/")

FIELD_TOTAL_ITEMS = os.getenv("FIELD_TOTAL_ITEMS", "UF_CRM_1603897171608").strip()
FIELD_READY_LOG = os.getenv("FIELD_READY_LOG", "UF_CRM_1737815245").strip()
FIELD_DEBT = os.getenv("FIELD_DEBT", "UF_CRM_1607545265860").strip()
FIELD_DUE_DATE = os.getenv("FIELD_DUE_DATE", "CLOSEDATE").strip()

BITRIX_STAGE_DONE = os.getenv("BITRIX_STAGE_DONE", "EXECUTING").strip()

ALLOWED_STAGES = [
    x.strip()
    for x in os.getenv(
        "ALLOWED_STAGES",
        "PREPAYMENT_INVOICE,UC_XUA0KH,UC_CMK8UN,UC_V2F091,UC_UL1L0V,NEW"
    ).split(",")
    if x.strip()
]

USE_MANAGER_WHITELIST = os.getenv("USE_MANAGER_WHITELIST", "false").lower() == "true"
MANAGERS_CHAT_IDS = {
    x.strip() for x in os.getenv("MANAGERS_CHAT_IDS", "").split(",") if x.strip()
}

STAGE_NAMES = {
    "PREPAYMENT_INVOICE": "ПЕРЕДАН МАСТЕРУ",
    "UC_XUA0KH": "ЗАКАЗ СКОРО ПРОСРОЧИТСЯ",
    "UC_CMK8UN": "ПРОСРОЧЕННЫЙ ЗАКАЗ",
    "UC_V2F091": "ЗАКАЗ ВЕРНУЛИ НА ДОРАБОТКУ",
    "UC_UL1L0V": "ПОДРЯДЧИКИ",
    "NEW": "ПОДБОР",
    "EXECUTING": "ЗАКАЗ ВЫПОЛНЕН",
    "UC_FMERNA": "ПОВТОРНЫЕ ПРОДАЖИ",
    "WON": "СДЕЛКА УСПЕШНА",
    "LOSE": "ВОЗВРАТ",
}

MONTHS_RU = {
    1: "января",
    2: "февраля",
    3: "марта",
    4: "апреля",
    5: "мая",
    6: "июня",
    7: "июля",
    8: "августа",
    9: "сентября",
    10: "октября",
    11: "ноября",
    12: "декабря",
}


def tg(chat_id, text):
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=20,
    )


def telegram_action(method, payload):
    """Check new Telegram API operations instead of silently ignoring failures."""
    response = requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}",
        json=payload,
        timeout=20,
    )
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise RuntimeError("Telegram API did not accept the request")
    return data.get("result")


@contextmanager
def ready_actions_db():
    os.makedirs(os.path.dirname(os.path.abspath(READY_ACTIONS_DB)), exist_ok=True)
    connection = sqlite3.connect(READY_ACTIONS_DB, timeout=10)
    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS ready_actions (
                token TEXT PRIMARY KEY,
                chat_id INTEGER NOT NULL,
                message_id INTEGER,
                deal_title TEXT NOT NULL,
                item_number INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Upgrade safely if a prior pre-report version of the bot created the DB.
        cols = {row[1] for row in connection.execute("PRAGMA table_info(ready_actions)")}
        if "created_at" not in cols:
            connection.execute("ALTER TABLE ready_actions ADD COLUMN created_at TEXT")
            connection.execute("UPDATE ready_actions SET created_at = updated_at WHERE created_at IS NULL")
        connection.execute("""
            CREATE TABLE IF NOT EXISTS report_chats (
                chat_id INTEGER PRIMARY KEY,
                registered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        connection.execute("""
            CREATE TABLE IF NOT EXISTS daily_reports (
                chat_id INTEGER NOT NULL,
                report_date TEXT NOT NULL,
                sent_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(chat_id, report_date)
            )
        """)
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def register_report_chat(chat_id):
    """Remember chats using the bot, including days without missing deals."""
    try:
        with ready_actions_db() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO report_chats (chat_id) VALUES (?)",
                (chat_id,),
            )
    except Exception:
        # Reporting must never stop the existing ready-item workflow.
        logger.exception("Could not register report chat %s", chat_id)


def answer_ready_callback(callback_id, message, alert=False):
    try:
        telegram_action("answerCallbackQuery", {
            "callback_query_id": callback_id,
            "text": message[:200],
            "show_alert": alert,
        })
    except Exception:
        logger.exception("Could not answer Telegram button callback")


def change_ready_message(chat_id, message_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
    }
    # Keep action buttons for unresolved checks; omit the keyboard for closed requests.
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    telegram_action("editMessageText", payload)


def get_ready_action(token):
    with ready_actions_db() as connection:
        return connection.execute(
            "SELECT chat_id, message_id, deal_title, item_number, status "
            "FROM ready_actions WHERE token = ?", (token,)
        ).fetchone()


def claim_ready_action(token, chat_id, message_id):
    """At most one worker handles a button press; recover after worker crashes."""
    with ready_actions_db() as connection:
        cursor = connection.execute("""
            UPDATE ready_actions
               SET status = 'processing', updated_at = CURRENT_TIMESTAMP
             WHERE token = ? AND chat_id = ? AND message_id = ?
               AND (status IN ('pending', 'unresolved') OR
                    (status = 'processing' AND updated_at < datetime('now', '-3 minutes')))
        """, (token, chat_id, message_id))
        return cursor.rowcount == 1


def finish_ready_action(token, status):
    with ready_actions_db() as connection:
        connection.execute(
            "UPDATE ready_actions SET status = ?, updated_at = CURRENT_TIMESTAMP "
            "WHERE token = ? AND status = 'processing'",
            (status, token),
        )


def completed_item_message(deal_title, item_number):
    """Read-only resolution if another attempt already finished the whole deal.

    The ordinary command never reopens completed deals. Only a pending button can
    acknowledge an item *already recorded* in a single completed deal.
    """
    candidates = bx("crm.deal.list", {
        "filter[TITLE]": str(deal_title),
        "select[]": ["ID", "TITLE", "STAGE_ID", FIELD_TOTAL_ITEMS, FIELD_READY_LOG, FIELD_DEBT],
    })
    if len(candidates) != 1:
        return None
    deal = candidates[0]
    if str(deal.get("STAGE_ID", "")).strip() != BITRIX_STAGE_DONE:
        return None
    ready_items = extract_ready_items(deal.get(FIELD_READY_LOG))
    if item_number not in ready_items:
        return None
    total_raw = deal.get(FIELD_TOTAL_ITEMS)
    if total_raw in (None, "", "0", 0):
        return None
    total_items = int(float(str(total_raw).replace(",", ".")))
    result = (
        f"🟢 По заказу {deal_title} изделие №{item_number} уже отмечено в Битрикс.\n"
        f"Готово {len(ready_items)} из {total_items}.\n\n"
        "Сделка находится в стадии «ЗАКАЗ ВЫПОЛНЕН»."
    )
    warning = debt_warning(deal.get(FIELD_DEBT))
    if warning:
        result += f"\n\n{warning}"
    return result


def retry_ready_item(deal_title, item_number):
    """Repeat the original validity checks, then write only if needed.

    Return (success, response). On failure no Bitrix fields are modified.
    """
    deal = get_deal_by_title_in_allowed_stages(deal_title)
    if deal is None:
        return False, "Сделка ещё не найдена в Битрикс. Проверьте, что она создана."
    if deal == "NOT_IN_ALLOWED_STAGES":
        completed = completed_item_message(deal_title, item_number)
        if completed:
            return True, completed
        return False, "Сделка найдена, но находится вне рабочих стадий бота."
    if deal == "MULTIPLE_IN_ALLOWED_STAGES":
        return False, "В работе несколько сделок с этим номером. Требуется проверка."

    total_raw = deal.get(FIELD_TOTAL_ITEMS)
    if total_raw in (None, "", "0", 0):
        return False, "Сначала заполните количество изделий в сделке Битрикс."
    total_items = int(float(str(total_raw).replace(",", ".")))
    if item_number < 1 or item_number > total_items:
        return False, f"Недопустимый номер изделия. В сделке всего {total_items}."

    ready_items = extract_ready_items(deal.get(FIELD_READY_LOG))
    already_ready = item_number in ready_items
    if not already_ready:
        ready_items = sorted(set(ready_items + [item_number]))
        update_fields = {FIELD_READY_LOG: build_ready_lines(ready_items)}
        if len(ready_items) == total_items:
            update_fields["STAGE_ID"] = BITRIX_STAGE_DONE
        update_deal(int(deal["ID"]), update_fields)

    ready_count = len(ready_items)
    if already_ready:
        message = (f"🟢 По заказу {deal_title} изделие №{item_number} "
                   f"уже было отмечено в Битрикс.\nГотово {ready_count} из {total_items}.")
    else:
        message = (f"🔵 В заказе {deal_title} изделие №{item_number} отмечено.\n"
                   f"Готово {ready_count} из {total_items}.")
        if ready_count == total_items:
            message = (f"🟢 По заказу {deal_title} изделие №{item_number} отмечено.\n"
                       f"Готово {ready_count} из {total_items}.\n\n"
                       "Сделка переведена в стадию «ЗАКАЗ ВЫПОЛНЕН».")
            warning = debt_warning(deal.get(FIELD_DEBT))
            if warning:
                message += f"\n\n{warning}"

    return True, message


def process_ready_callback(callback):
    callback_id = callback.get("id")
    payload = callback.get("data") or ""
    message = callback.get("message") or {}
    chat_id = message.get("chat", {}).get("id")
    message_id = message.get("message_id")

    if not callback_id or not payload.startswith(("ready_retry:", "ready_cancel:")):
        return
    if not chat_id or message_id is None or not validate_manager(chat_id):
        return

    token = payload.split(":", 1)[1]
    try:
        action = get_ready_action(token)
        if not action or str(action[0]) != str(chat_id) or action[1] != message_id:
            return
        if action[4] in ("done", "cancelled"):
            return
        if not claim_ready_action(token, chat_id, message_id):
            return
    except Exception:
        logger.exception("Could not load ready callback")
        return

    deal_title, item_number = action[2], action[3]
    try:
        if payload.startswith("ready_cancel:"):
            user = callback.get("from") or {}
            user_name = ("@" + user["username"]) if user.get("username") else (user.get("first_name") or "Сотрудник")
            change_ready_message(
                chat_id, message_id,
                f"⚪ Заказ {deal_title}, изделие №{item_number}.\n"
                f"Запрос отменён – ошибка при вводе.\nОтменил: {user_name}",
            )
            finish_ready_action(token, "cancelled")
            return

        success, result_message = retry_ready_item(deal_title, item_number)
        if not success:
            # Clearly show that a colleague has checked the request, but the
            # item is still not marked in Bitrix. Preserve both action buttons.
            unresolved_text = (
                f"🟡 Заказ {deal_title}, изделие №{item_number}.\n"
                f"Повторная проверка: {result_message}\n\n"
                "Изделие не отмечено. Требуется решение сотрудника.\n"
                "Ответственный: @pretty_jam1"
            )
            keyboard = {"inline_keyboard": [
                [{"text": "🔄 Проверить повторно", "callback_data": f"ready_retry:{token}"}],
                [{"text": "✖️ Ошибка при вводе", "callback_data": f"ready_cancel:{token}"}],
            ]}
            # Editing an unchanged message returns 400 in Telegram. Repeated
            # attempts still count as unresolved without producing an error.
            if message.get("text") != unresolved_text:
                change_ready_message(chat_id, message_id, unresolved_text, reply_markup=keyboard)
            finish_ready_action(token, "unresolved")
            return

        change_ready_message(chat_id, message_id, result_message)
        finish_ready_action(token, "done")
    except Exception:
        logger.exception("Could not handle ready callback")
        try:
            finish_ready_action(token, "unresolved")
        except Exception:
            logger.exception("Could not release ready callback lock")




def daily_report_data(chat_id, report_date):
    """Count all requests since the previous scheduled report.

    Use the 20:30–20:30 Moscow window so incidents after 20:30 aren't lost.
    """
    end_msk = datetime.combine(report_date, datetime.min.time(), tzinfo=MSK).replace(
        hour=REPORT_HOUR, minute=REPORT_MINUTE,
    )
    start_utc = (end_msk - timedelta(days=1)).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    end_utc = end_msk.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with ready_actions_db() as connection:
        rows = connection.execute("""
            SELECT deal_title, item_number, status
              FROM ready_actions
             WHERE chat_id = ? AND message_id IS NOT NULL
               AND created_at >= ? AND created_at < ?
             ORDER BY created_at, rowid
        """, (chat_id, start_utc, end_utc)).fetchall()
    return rows


def format_daily_report(chat_id, report_date):
    """A report counts requests, not unique deal numbers."""
    rows = daily_report_data(chat_id, report_date)
    completed = [row for row in rows if row[2] == "done"]
    cancelled = [row for row in rows if row[2] == "cancelled"]
    unresolved = [row for row in rows if row[2] in ("unresolved", "processing")]
    untouched = [row for row in rows if row[2] == "pending"]

    message = (
        f"📊 Отчёт на {report_date.strftime('%d.%m.%Y')} – 20:30 МСК\n"
        "Период: последние 24 часа\n\n"
        f"Всего запросов без найденной сделки: {len(rows)}\n"
        f"🟢 Решено: {len(completed) + len(cancelled)} "
        f"(изделие отмечено: {len(completed)}, ошибка ввода: {len(cancelled)})\n"
        f"🟡 Не решено: {len(unresolved)}\n"
        f"🔴 Без реакции: {len(untouched)}"
    )
    for heading, cases in (("🔴 Без реакции", untouched), ("🟡 Не решено", unresolved)):
        if not cases:
            continue
        message += f"\n\n{heading}:"
        displayed = 0
        for deal_title, item_number, _ in cases:
            line = f"\n• {deal_title}-{item_number}"
            # Telegram sendMessage has a 4096-character limit.
            if len(message) + len(line) + 75 > 3900:
                break
            message += line
            displayed += 1
        if displayed < len(cases):
            message += f"\n… и ещё {len(cases) - displayed} запросов"
    return message


def send_due_reports(now_msk=None):
    """Send each source chat at most one daily report, including a zero report.

    On restart after 20:30, it can catch up before Moscow midnight.
    Requires the one Amvera replica already used by this bot.
    """
    now_msk = now_msk or datetime.now(MSK)
    if (now_msk.hour, now_msk.minute) < (REPORT_HOUR, REPORT_MINUTE):
        return
    day = now_msk.date()
    day_s = day.isoformat()
    with ready_actions_db() as connection:
        chats = [row[0] for row in connection.execute("SELECT chat_id FROM report_chats")]
    for chat_id in chats:
        try:
            with ready_actions_db() as connection:
                done = connection.execute(
                    "SELECT 1 FROM daily_reports WHERE chat_id = ? AND report_date = ?",
                    (chat_id, day_s),
                ).fetchone()
            if done:
                continue
            telegram_action("sendMessage", {
                "chat_id": chat_id,
                "text": format_daily_report(chat_id, day),
            })
            with ready_actions_db() as connection:
                connection.execute(
                    "INSERT OR IGNORE INTO daily_reports (chat_id, report_date) VALUES (?, ?)",
                    (chat_id, day_s),
                )
        except Exception:
            logger.exception("Could not send daily report for chat %s", chat_id)


async def daily_report_loop():
    # Run a blocking Telegram request on a worker thread, not the ASGI event loop.
    while True:
        try:
            await asyncio.to_thread(send_due_reports)
        except Exception:
            logger.exception("Daily report scheduler failed")
        await asyncio.sleep(10)


def bx(method, data):
    r = requests.post(
        f"{BITRIX_WEBHOOK_BASE}/{method}.json",
        data=data,
        timeout=30,
    )
    j = r.json()

    if "result" not in j:
        raise Exception(f"Bitrix error: {j}")

    return j["result"]


def parse_command(text):
    text = (text or "").strip()

    # Формат: 0001-1
    m = re.fullmatch(r"(.+)-(\d+)", text)
    if m:
        return {
            "deal_title": m.group(1).strip(),
            "item_number": int(m.group(2)),
            "mode": "full",
        }

    # Формат: 0001
    if re.fullmatch(r"\d+", text):
        return {
            "deal_title": text,
            "item_number": None,
            "mode": "deal_only",
        }

    return None


def normalize_value(value):
    if isinstance(value, list):
        return value[0] if value else ""
    return value


def normalize_log(value):
    if isinstance(value, list):
        return "\n".join(str(x) for x in value if str(x).strip())
    return str(value or "")


def extract_ready_items(log_text):
    log_text = normalize_log(log_text)
    found = re.findall(r"\[(\d+)\]", log_text)
    return sorted({int(x) for x in found})


def build_ready_lines(items):
    return [f"[{i}] изделие в заказе готово." for i in sorted(items)]


def clean_money_value(value):
    value = normalize_value(value)

    if value in (None, "", "0", 0, "0.00"):
        return "0"

    value = str(value).strip()

    # Битрикс денежные поля отдаёт так: 4500|RUB
    if "|" in value:
        value = value.split("|")[0]

    return value.strip()


def format_money(value):
    value = clean_money_value(value)

    if value in (None, "", "0", "0.00"):
        return "0"

    try:
        number = float(str(value).replace(" ", "").replace(",", "."))
    except Exception:
        return str(value)

    if number.is_integer():
        return f"{int(number):,}".replace(",", " ")

    return f"{number:,.2f}".replace(",", " ").replace(".", ",")


def parse_money(value):
    value = clean_money_value(value)

    if value in (None, "", "0", "0.00"):
        return 0.0

    try:
        return float(str(value).replace(" ", "").replace(",", "."))
    except Exception:
        return 0.0


def format_date(value):
    value = normalize_value(value)

    if not value:
        return "не заполнена"

    value = str(value)

    try:
        clean = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean)
        return f"{dt.day} {MONTHS_RU.get(dt.month, '')} {dt.year}".strip()
    except Exception:
        return value[:10]


def stage_name(stage_id):
    stage_id = str(stage_id or "").strip()
    return STAGE_NAMES.get(stage_id, stage_id or "не указана")


def debt_warning(value):
    debt = parse_money(value)

    if debt > 0:
        return f"‼️По заказу имеется доплата в размере {format_money(value)} рублей."

    return ""


def validate_manager(chat_id):
    if not USE_MANAGER_WHITELIST:
        return True
    return str(chat_id) in MANAGERS_CHAT_IDS


def get_contact_name(contact_id):
    contact_id = normalize_value(contact_id)

    if not contact_id:
        return "не указан"

    try:
        contact = bx("crm.contact.get", {"id": contact_id})
        name_parts = [
            contact.get("NAME", ""),
            contact.get("LAST_NAME", ""),
        ]
        name = " ".join([x for x in name_parts if x]).strip()
        return name or "не указан"
    except Exception:
        return "не указан"


def get_deal_by_title_in_allowed_stages(deal_title):
    result = bx(
        "crm.deal.list",
        {
            "filter[TITLE]": str(deal_title),
            "select[]": [
                "ID",
                "TITLE",
                "STAGE_ID",
                "DATE_CREATE",
                "CONTACT_ID",
                FIELD_TOTAL_ITEMS,
                FIELD_READY_LOG,
                FIELD_DEBT,
                FIELD_DUE_DATE,
            ],
        },
    )

    if not result:
        return None

    filtered = []
    for deal in result:
        stage_id = str(deal.get("STAGE_ID", "")).strip()

        if not ALLOWED_STAGES or stage_id in ALLOWED_STAGES:
            filtered.append(deal)

    if not filtered:
        return "NOT_IN_ALLOWED_STAGES"

    if len(filtered) > 1:
        return "MULTIPLE_IN_ALLOWED_STAGES"

    return filtered[0]


def update_deal(deal_id, fields):
    payload = {"id": deal_id}

    for k, v in fields.items():
        if k == FIELD_READY_LOG and isinstance(v, list):
            for index, line in enumerate(v):
                payload[f"fields[{k}][{index}]"] = line
        else:
            payload[f"fields[{k}]"] = v

    bx("crm.deal.update", payload)


def send_not_found(chat_id, deal_title, item_number=None):
    # For information-only requests, keep the old message unchanged.
    if item_number is None:
        tg(
            chat_id,
            f"🔴 Не удалось определить заказ {deal_title} среди сделок, которые сейчас находятся в работе.\n\n"
            "Проверьте, пожалуйста, правильно ли указан номер заказа.\n"
            "Также возможно, что в работе одновременно находится несколько сделок с одинаковым номером.\n\n"
            "Если потребуется скорректировать сделку в Битрикс, пожалуйста, обратитесь к Руководителю отдела продаж @pretty_jam1"
        )
        return

    content = (
        f"🔴 Заказ {deal_title}, изделие №{item_number} не найдено среди сделок в работе.\n\n"
        "Проверьте номер заказа. Возможно, сделка ещё не внесена в Битрикс "
        "или находится вне рабочих стадий.\n\n"
        "Ответственный: @pretty_jam1"
    )
    token = secrets.token_hex(12)
    try:
        with ready_actions_db() as connection:
            connection.execute(
                "INSERT INTO ready_actions (token, chat_id, deal_title, item_number) "
                "VALUES (?, ?, ?, ?)", (token, chat_id, deal_title, item_number),
            )
        message = telegram_action("sendMessage", {
            "chat_id": chat_id,
            "text": content,
            "reply_markup": {"inline_keyboard": [
                [{"text": "🔄 Проверить повторно", "callback_data": f"ready_retry:{token}"}],
                [{"text": "✖️ Ошибка при вводе", "callback_data": f"ready_cancel:{token}"}],
            ]},
        })
        with ready_actions_db() as connection:
            connection.execute(
                "UPDATE ready_actions SET message_id = ? WHERE token = ?",
                (message["message_id"], token),
            )
            connection.execute(
                "INSERT OR IGNORE INTO report_chats (chat_id) VALUES (?)",
                (chat_id,),
            )
    except Exception:
        # If new storage/keyboard fails, preserve the original bot response.
        logger.exception("Could not send message with ready buttons")
        tg(chat_id, content)


def send_multiple_found(chat_id):
    tg(
        chat_id,
        "‼️ Сейчас в работе одновременно находится несколько заказов с таким номером.\n\n"
        "Проверьте, пожалуйста, правильно ли указан номер заказа, или обратитесь к руководителю отдела продаж @pretty_jam1, чтобы решить ситуацию"
    )


def send_deal_info(chat_id, deal_title, deal):
    total_raw = deal.get(FIELD_TOTAL_ITEMS)

    if total_raw not in (None, "", "0", 0):
        total_items = int(float(str(total_raw).replace(",", ".")))
    else:
        total_items = 0

    ready_items = extract_ready_items(deal.get(FIELD_READY_LOG))
    ready_count = len(ready_items)

    client_name = get_contact_name(deal.get("CONTACT_ID"))
    stage = stage_name(deal.get("STAGE_ID"))
    date_create = format_date(deal.get("DATE_CREATE"))
    due_date = format_date(deal.get(FIELD_DUE_DATE))

    debt_value = deal.get(FIELD_DEBT)
    debt = parse_money(debt_value)

    if total_items:
        ready_line = f"{ready_count} из {total_items}"
    else:
        ready_line = "количество изделий не заполнено"

    message = (
        f"📋 Информация по заказу {deal_title}\n\n"
        f"Клиент: {client_name}\n"
        f"Стадия: {stage}\n"
        f"Дата принятия: {date_create}\n"
        f"Дата выдачи: {due_date}\n\n"
        f"Изделий в заказе: {total_items if total_items else 'не заполнено'}\n"
        f"Готово: {ready_line}"
    )

    if debt > 0:
        message += f"\n\n‼️По заказу имеется доплата в размере {format_money(debt_value)} рублей."

    tg(chat_id, message)


@app.get("/")
def root():
    return {"ok": True, "service": "telegram-bitrix-ready-bot"}


def handle_ready_message(data):
    """Existing message handling, run off the ASGI event loop."""
    msg = data.get("message") or data.get("edited_message") or {}
    chat_id = msg.get("chat", {}).get("id")
    text = (msg.get("text") or "").strip()

    if not chat_id:
        return {"ok": True}

    if not validate_manager(chat_id):
        tg(chat_id, "У Вас нет доступа к этому боту.")
        return {"ok": True}

    parsed = parse_command(text)

    if not parsed:
        tg(
            chat_id,
            "🟠 Неверный формат сообщения.\n\n"
            "Используйте формат:\n"
            "0001-1"
        )
        return {"ok": True}

    # Register a report destination from every valid command, not only errors.
    register_report_chat(chat_id)
    deal_title = parsed["deal_title"]

    try:
        deal = get_deal_by_title_in_allowed_stages(deal_title)

        if deal is None or deal == "NOT_IN_ALLOWED_STAGES":
            send_not_found(chat_id, deal_title, parsed["item_number"])
            return {"ok": True}

        if deal == "MULTIPLE_IN_ALLOWED_STAGES":
            send_multiple_found(chat_id)
            return {"ok": True}

        if parsed["mode"] == "deal_only":
            send_deal_info(chat_id, deal_title, deal)
            return {"ok": True}

        item_number = parsed["item_number"]
        deal_id = int(deal["ID"])

        total_raw = deal.get(FIELD_TOTAL_ITEMS)

        if total_raw in (None, "", "0", 0):
            tg(
                chat_id,
                "⁉️ В сделке не заполнено количество изделий.\n"
                "Сначала укажите количество изделий в заказе в Битрикс.\n\n"
                "Если потребуется скорректировать сделку в Битрикс, пожалуйста, обратитесь к Руководителю отдела продаж @pretty_jam1"
            )
            return {"ok": True}

        total_items = int(float(str(total_raw).replace(",", ".")))

        if item_number < 1:
            tg(chat_id, "🚫 Номер изделия должен быть больше 0.")
            return {"ok": True}

        if item_number > total_items:
            tg(
                chat_id,
                f"🚫 В заказе указано изделий: {total_items}.\n"
                f"Изделие №{item_number} отметить нельзя."
            )
            return {"ok": True}

        ready_items = extract_ready_items(deal.get(FIELD_READY_LOG))

        if item_number in ready_items:
            tg(
                chat_id,
                f"🟡 По заказу {deal_title} изделие №{item_number} уже было отмечено как готовое."
            )
            return {"ok": True}

        ready_items.append(item_number)
        ready_items = sorted(set(ready_items))

        ready_lines = build_ready_lines(ready_items)

        fields = {
            FIELD_READY_LOG: ready_lines
        }

        all_ready = len(ready_items) == total_items

        if all_ready:
            fields["STAGE_ID"] = BITRIX_STAGE_DONE

        update_deal(deal_id, fields)

        if all_ready:
            message = (
                f"🟢 По заказу {deal_title} изделие №{item_number} отмечено.\n"
                f"Готово {len(ready_items)} из {total_items}.\n\n"
                "Сделка переведена в стадию «ЗАКАЗ ВЫПОЛНЕН»."
            )

            warning = debt_warning(deal.get(FIELD_DEBT))
            if warning:
                message += f"\n\n{warning}"

            tg(chat_id, message)

        else:
            tg(
                chat_id,
                f"🔵 В заказе {deal_title} изделие №{item_number} отмечено.\n"
                f"Готово {len(ready_items)} из {total_items}."
            )

    except Exception as e:
        tg(chat_id, f"❌ Ошибка: {e}")

    return {"ok": True}


@app.post("/webhook/telegram")
async def webhook(req: Request, background_tasks: BackgroundTasks):
    secret = req.headers.get("X-Telegram-Bot-Api-Secret-Token", "")

    if TELEGRAM_SECRET_TOKEN and secret != TELEGRAM_SECRET_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid secret token")

    data = await req.json()
    if data.get("callback_query"):
        callback = data["callback_query"]
        # Telegram requires callback acknowledgements promptly, regardless of
        # Bitrix response time. The slow work happens after the webhook's 200.
        callback_id = callback.get("id")
        if callback_id:
            await asyncio.to_thread(answer_ready_callback, callback_id, "Запрос принят")
        background_tasks.add_task(process_ready_callback, callback)
        return {"ok": True}

    # Bitrix REST uses synchronous requests; run it in a worker so even a
    # 30-second Bitrix timeout cannot block the next Telegram button tap.
    return await asyncio.to_thread(handle_ready_message, data)
