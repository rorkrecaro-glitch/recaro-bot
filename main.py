import os
import re
import requests
from fastapi import FastAPI, Request

app = FastAPI()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BITRIX_WEBHOOK_BASE = os.getenv("BITRIX_WEBHOOK_BASE")

FIELD_TOTAL_ITEMS = os.getenv("FIELD_TOTAL_ITEMS")
FIELD_READY_LOG = os.getenv("FIELD_READY_LOG")
BITRIX_STAGE_DONE = os.getenv("BITRIX_STAGE_DONE")

def tg(chat_id, text):
    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json={
        "chat_id": chat_id,
        "text": text
    })

def bx(method, data):
    r = requests.post(f"{BITRIX_WEBHOOK_BASE}/{method}.json", data=data)
    return r.json()["result"]

def get(deal):
    return bx("crm.deal.get", {"id": deal})

def update(deal, fields):
    d = {"id": deal}
    for k, v in fields.items():
        d[f"fields[{k}]"] = v
    bx("crm.deal.update", d)

def parse(t):
    m = re.match(r"(\\d+)-(\\d+)", t)
    return (int(m.group(1)), int(m.group(2))) if m else None

def extract(log):
    return list(set(map(int, re.findall(r"\\[(\\d+)\\]", log or ""))))

@app.post("/webhook/telegram")
async def webhook(req: Request):
    data = await req.json()
    msg = data.get("message", {})
    chat_id = msg.get("chat", {}).get("id")
    text = msg.get("text", "")

    p = parse(text)
    if not p:
        tg(chat_id, "Формат: 12345-1")
        return

    deal, item = p
    d = get(deal)

    total = int(d.get(FIELD_TOTAL_ITEMS) or 0)
    log = d.get(FIELD_READY_LOG) or ""

    items = extract(log)

    if item in items:
        tg(chat_id, "Уже есть")
        return

    items.append(item)

    new_log = "\\n".join([f"[{i}] изделие готово." for i in sorted(items)])

    fields = {FIELD_READY_LOG: new_log}

    if len(items) == total:
        fields["STAGE_ID"] = BITRIX_STAGE_DONE

    update(deal, fields)

    tg(chat_id, f"{len(items)}/{total}")
