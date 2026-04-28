import os
import re
import requests
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException

app = FastAPI()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_SECRET_TOKEN = os.getenv("TELEGRAM_SECRET_TOKEN", "").strip()
BITRIX_WEBHOOK_BASE = os.getenv("BITRIX_WEBHOOK_BASE", "").rstrip("/")

FIELD_TOTAL_ITEMS = os.getenv("FIELD_TOTAL_ITEMS", "UF_CRM_1603897171608").strip()
FIELD_READY_LOG = os.getenv("FIELD_READY_LOG", "UF_CRM_1737815245").strip()
FIELD_DEBT = os.getenv("FIELD_DEBT", "UF_CRM_1607545265860").strip()
FIELD_DUE_DATE = os.getenv("FIELD_DUE_DATE", "CLOSEDATE").strip()

BITRIX_STAGE_DONE = os.getenv("BITRIX_STAGE_DONE", "EXECUTING").strip()

ALLOWED_STAGES = [
    x.strip() for x in os.getenv(
        "ALLOWED_STAGES",
        "PREPAYMENT_INVOICE,UC_XUA0KH,UC_CMK8UN,UC_V2F091,UC_UL1L0V,NEW"
    ).split(",") if x.strip()
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

    m = re.fullmatch(r"(.+)-(\d+)", text)
    if m:
        return {
            "deal_title": m.group(1).strip(),
            "item_number": int(m.group(2)),
            "mode": "full",
        }

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
        return f"‼️ По заказу имеется доплата в размере {format_money(value)} рублей."

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


def send_not_found(chat_id, deal_title):
    tg(
        chat_id,
        f"🔴 Не удалось определить заказ {deal_title} среди сделок, которые сейчас находятся в работе.\n\n"
        "Проверьте, пожалуйста, правильно ли указан номер заказа.\n"
        "Также возможно, что в работе одновременно находится несколько сделок с одинаковым номером.\n\n"
        "Если потребуется скорректировать сделку в Битрикс, пожалуйста, обратитесь к Руководителю отдела продаж @pretty_jam1"
    )


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
        message += f"\n\n‼️Внимание! По заказу имеется доплата в размере {format_money(debt_value)} рублей."
    else:
        message += "\n\nОстаток по заказу: 0 рублей."

    tg(chat_id, message)


@app.get("/")
def root():
    return {"ok": True, "service": "telegram-bitrix-ready-bot"}


@app.post("/webhook/telegram")
async def webhook(req: Request):
    secret = req.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if TELEGRAM_SECRET_TOKEN and secret != TELEGRAM_SECRET_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid secret token")

    data = await req.json()
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

    deal_title = parsed["deal_title"]

    try:
        deal = get_deal_by_title_in_allowed_stages(deal_title)

        if deal is None or deal == "NOT_IN_ALLOWED_STAGES":
            send_not_found(chat_id, deal_title)
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
