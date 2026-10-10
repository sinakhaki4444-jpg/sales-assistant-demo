import os
import json
import asyncio

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

WS_URL = (
    "wss://gisoo.pro/api/v1/realtime"
    "?model=gpt-realtime-2.1-mini"
)


def get_knowledge_base():
    # روی Render، اطلاعات را در Environment Variable قرار می‌دهیم.
    text = os.getenv("KNOWLEDGE_BASE_JSON", "")

    # اجرای محلی: در صورت وجود فایل، آن را می‌خواند.
    if not text and os.path.exists("knowledge_base.json"):
        with open("knowledge_base.json", "r", encoding="utf-8-sig") as f:
            text = json.dumps(
                json.load(f),
                ensure_ascii=False,
                indent=2
            )

    return text[:10000]


def build_session_update():
    knowledge_text = get_knowledge_base()

    instructions = f"""
تو یک کارشناس فروش حرفه‌ای و فارسی‌زبان هستی.

قواعد مکالمه:
- فقط فارسی طبیعی و محاوره‌ای صحبت کن.
- پاسخ‌ها کوتاه و ترجیحاً یک جمله باشند.
- در هر پاسخ حداکثر یک سؤال بپرس.
- ابتدا نیاز مشتری را کشف کن و سپس محصول مناسب پیشنهاد بده.
- اطلاعات، قیمت و مشخصات محصول را جعل نکن.
- فقط بر اساس اطلاعات موجود در پایگاه دانش پاسخ بده.
- از تکرار، تبلیغات اغراق‌آمیز و توضیحات طولانی پرهیز کن.
- مکالمه را طبیعی و متناسب با پاسخ مشتری ادامه بده.
- در شروع مکالمه، مشتری را کوتاه و طبیعی خوشامد بگو.

پایگاه دانش محصولات:
{knowledge_text}
"""

    return {
        "type": "session.update",
        "session": {
            "audio": {
                "output": {
                    "voice": "marin"
                }
            },
            "instructions": instructions,
            "turn_detection": {
                "type": "server_vad",
                "threshold": 0.4,
                "silence_duration_ms": 350,
                "prefix_padding_ms": 200
            },
            "reasoning": {
                "effort": "none"
            }
        }
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.websocket("/ws")
async def voice_websocket(browser: WebSocket):
    await browser.accept()

    api_key = os.getenv("GISOO_API_KEY")

    if not api_key:
        await browser.close(code=1011, reason="Server API key is missing")
        return

    try:
        async with websockets.connect(
            WS_URL,
            additional_headers={
                "Authorization": f"Bearer {api_key}"
            },
            max_size=None,
            ping_interval=20,
            ping_timeout=20
        ) as gisoo:

            # دریافت رویداد اولیه از Gisoo
            initial_event = await asyncio.wait_for(
                gisoo.recv(),
                timeout=15
            )

            await browser.send_text(initial_event)

            # تنظیم مکالمه و دستورالعمل فروش
            await gisoo.send(
                json.dumps(
                    build_session_update(),
                    ensure_ascii=False
                )
            )

            async def browser_to_gisoo():
                while True:
                    message = await browser.receive_text()
                    await gisoo.send(message)

            async def gisoo_to_browser():
                async for message in gisoo:
                    await browser.send_text(message)

            tasks = [
                asyncio.create_task(browser_to_gisoo()),
                asyncio.create_task(gisoo_to_browser())
            ]

            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED
            )

            for task in pending:
                task.cancel()

            await asyncio.gather(
                *pending,
                return_exceptions=True
            )

    except WebSocketDisconnect:
        pass

    except Exception as e:
        print(f"WebSocket error: {e}")

    finally:
        try:
            await browser.close()
        except Exception:
            pass