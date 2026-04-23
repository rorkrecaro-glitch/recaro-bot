import os
import re
import requests
from fastapi import FastAPI, Request, HTTPException

app = FastAPI()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_SECRET_TOKEN = os.getenv("TELEGRAM_SECRET_TOKEN", "").strip()
BITRIX_WEBHOOK_BASE = os.getenv("BITRIX_WEBHOOK_BASE", "").rstrip("/")

FIELD_TOTAL_ITEMS = os.getenv("FIELD_TOTAL_ITEMS", "").strip()
FIELD_READY_LOG = os.getenv("FIELD_READY_LOG", "").strip()
BITRIX_STAGE_DONE = os.getenv("BITRIX_STAGE_DONE", "").strip()

ALLOWED_STAGES = [
    x.strip() for x in os.getenv("ALLOWED_STAGES", "").split(",") if x.strip()
]

USE_MANAGER_WHITELIST = os.getenv("USE_MANAGER_WHITELIST", "false").lower() == "true"
MANAGERS_CHAT_IDS = {
    x.strip() for x in os.getenv("MANAGERS_CHAT_IDS", "").split(",") if x.strip()
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
    if not m:
        return None
    deal_title = m.group(1).strip()
    item_number = int(m.group(2))
    return deal_title, item_number


def extract_ready_items(log_text):
    if not log_text:
        return []
    found = re.findall(r"\[(\d+)\]", log_text)
    return sorted({int(x) for x in found})


def build_ready_log(items):
    return "\n".join([f"[{i}] изделие в заказе готово." for i in sorted(items)])


def validate_manager(chat_id):
    if not USE_MANAGER_WHITELIST:
        return True
    return str(chat_id) in MANAGERS_CHAT_IDS


def get_deal_by_title_in_allowed_stages(deal_title):
    result = bx(
        "crm.deal.list",
        {
            "filter[TITLE]": str(deal_title),
            "select[]": [
                "ID",
                "TITLE",
                "STAGE_ID",
                FIELD_TOTAL_ITEMS,
                FIELD_READY_LOG,
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
        raise Exception(
            f"Сделка {deal_title} найдена, но не находится в разрешённых стадиях."
        )

    if len(filtered) > 1:
        stages = ", ".join(sorted({str(x.get('STAGE_ID', '')) for x in filtered}))
        raise Exception(
            f"Найдено несколько сделок с названием {deal_title} в разрешённых стадиях: {stages}"
        )

    return filtered[0]


def update_deal(deal_id, fields):
    payload = {"id": deal_id}
    for k, v in fields.items():
        payload[f"fields[{k}]"] = v
    bx("crm.deal.update", payload)


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
        tg(chat_id, "Формат: НОМЕР_ЗАКАЗА-НОМЕР_ИЗДЕЛИЯ\nПример: 0132-1")
        return {"ok": True}

    deal_title, item_number = parsed

    try:
        deal = get_deal_by_title_in_allowed_stages(deal_title)

        if not deal:
            tg(chat_id, f"Сделка с названием {deal_title} не найдена.")
            return {"ok": True}

        deal_id = int(deal["ID"])

        total_raw = deal.get(FIELD_TOTAL_ITEMS)
        if total_raw in (None, "", "0", 0):
            tg(chat_id, "В сделке не заполнено поле количества изделий.")
            return {"ok": True}

        total_items = int(float(str(total_raw).replace(",", ".")))

        if item_number < 1:
            tg(chat_id, "Номер изделия должен быть больше 0.")
            return {"ok": True}

        if item_number > total_items:
            tg(
                chat_id,
                f"В заказе указано изделий: {total_items}. Изделие №{item_number} отметить нельзя.",
            )
            return {"ok": True}

        ready_log = deal.get(FIELD_READY_LOG) or ""
        ready_items = extract_ready_items(str(ready_log))

        if item_number in ready_items:
            tg(chat_id, f"Изделие №{item_number} уже было отмечено как готовое.")
            return {"ok": True}

        ready_items.append(item_number)
        ready_items = sorted(set(ready_items))

        new_log = build_ready_log(ready_items)

        fields = {
            FIELD_READY_LOG: new_log
        }

        if len(ready_items) == total_items:
            fields["STAGE_ID"] = BITRIX_STAGE_DONE

        update_deal(deal_id, fields)

        if len(ready_items) == total_items:
            tg(
                chat_id,
                f"Изделие №{item_number} отмечено.\n"
                f"Готово {len(ready_items)} из {total_items}.\n"
                f"Все изделия готовы. Сделка переведена в стадию {BITRIX_STAGE_DONE}.",
            )
        else:
            tg(
                chat_id,
                f"Изделие №{item_number} отмечено.\n"
                f"Готово {len(ready_items)} из {total_items}.",
            )

    except Exception as e:
        tg(chat_id, f"Ошибка: {e}")

    return {"ok": True}
