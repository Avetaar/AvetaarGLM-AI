# Copyright © 2026 Avetaar / Rival Team

import asyncio
import base64
import json
import os
import time

import httpx

AVETAAR_TARGET = "https://glm-ai.chat"
AVETAAR_UA = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/156.0.0.0 Safari/537.36"
AVETAAR_CHAT_MODELS = ["glm-5.3", "glm-5.3-flash", "glm-5.2", "glm-5.1",
    "glm-4.7", "glm-4.6", "glm-4.5", "glm-4.7-flash", "glm-4.5-flash", "glm-4.7-flashx"]
AVETAAR_DEFAULT_MODEL = "glm-5.3-flash"
AVETAAR_MAX_CHARS = 8000
AVETAAR_MAX_IMG = 4 * 1024 * 1024

def avetaar_data_url(avetaar_path):
    if os.path.getsize(avetaar_path) > AVETAAR_MAX_IMG:
        raise ValueError(f"الصورة > {AVETAAR_MAX_IMG // (1024*1024)}MB")
    ext = avetaar_path.rsplit(".", 1)[-1].lower()
    mime = {"jpg": "image/jpeg", "jpeg": "image/jpeg",
            "png": "image/png", "webp": "image/webp"}
    if ext not in mime:
        raise ValueError(f"الصيغة {ext} غير مدعومة — مدعوم: jpg, png, webp")
    with open(avetaar_path, "rb") as f:
        return f"data:{mime[ext]};base64," + base64.b64encode(f.read()).decode()

async def avetaar_session(avetaar_client):
    r = await avetaar_client.get("/wp-json/glm-chat/v1/session",
        headers={"Accept": "application/json",
                 "X-Requested-With": "com.duckduckgo.mobile.android",
                 "Referer": AVETAAR_TARGET + "/"})
    r.raise_for_status()
    return r.json()["nonce"]

async def avetaar_stream(avetaar_client, avetaar_nonce, avetaar_body):
    if len(avetaar_body.get("messages", [{}])[-1].get("content", "")) > AVETAAR_MAX_CHARS:
        print(f"[Avetaar] النص > {AVETAAR_MAX_CHARS} حرف")
        return "limit"
    async with avetaar_client.stream("POST", "/wp-json/glm-chat/v1/message",
        json=avetaar_body,
        headers={"Accept": "text/event-stream",
                 "Content-Type": "application/json",
                 "Origin": AVETAAR_TARGET,
                 "Referer": AVETAAR_TARGET + "/",
                 "X-GLM-Nonce": avetaar_nonce,
                 "X-Requested-With": "com.duckduckgo.mobile.android"}) as avetaar_s:
        if avetaar_s.status_code == 403:
            return "expired"
        if avetaar_s.status_code != 200:
            print(f"[Avetaar] HTTP {avetaar_s.status_code}: {(await avetaar_s.aread()).decode('utf-8','replace')[:300]}")
            return "error"
        avetaar_buf = ""
        async for avetaar_chunk in avetaar_s.aiter_text():
            avetaar_buf += avetaar_chunk
            while "\n\n" in avetaar_buf:
                avetaar_ev, avetaar_buf = avetaar_buf.split("\n\n", 1)
                for avetaar_line in avetaar_ev.split("\n"):
                    if not avetaar_line.startswith("data:"):
                        continue
                    d = avetaar_line[5:].strip()
                    if not d or d == "[DONE]":
                        continue
                    try:
                        o = json.loads(d)
                    except json.JSONDecodeError:
                        continue
                    t = o.get("type")
                    if t == "delta":
                        print(o.get("text", ""), end="", flush=True)
                    elif t == "meta":
                        if o.get("fallback"):
                            print(f"\n[{o.get('label')} — fallback]", end="")
                    elif t == "done":
                        print(f"\n[Avetaar] الرصيد المتبقي: {o.get('paidLeft')}\n")
                    elif t in ("error", "limit", "notice", "offline"):
                        print(f"\n[Avetaar] {t}: {o.get('message','')}")
                        if o.get("switch"):
                            print(f"[Avetaar] الانتقال إلى {o.get('switch')}")
                        return "error"
        print()
        return "ok"

async def avetaar_text(avetaar_client, avetaar_nonce, avetaar_model, avetaar_prompt):
    return await avetaar_stream(avetaar_client, avetaar_nonce,
        {"model": avetaar_model,
         "messages": [{"role": "user", "content": avetaar_prompt}]})

async def avetaar_save_image(avetaar_client, avetaar_resp):
    ct = avetaar_resp.headers.get("Content-Type", "")
    if "image" in ct:
        out = f"avetaar_generated_{time.strftime('%Y%m%d_%H%M%S')}.img"
        with open(out, "wb") as f:
            f.write(avetaar_resp.content)
        print(f"[Avetaar] تم حفظ الصورة: {out}")
        return "ok"
    payload = avetaar_extract_image(avetaar_resp.json()) if "application/json" in ct \
        else avetaar_extract_image(avetaar_resp.text)
    if not payload:
        print(f"[Avetaar] لم أعثر على صورة في الرد:\n{avetaar_resp.text[:400]}")
        return "error"
    ext = "png"
    if payload.startswith("http"):
        rr = await avetaar_client.get(payload)
        if "jpeg" in rr.headers.get("Content-Type", ""):
            ext = "jpg"
        if "webp" in rr.headers.get("Content-Type", ""):
            ext = "webp"
        raw = rr.content
    else:
        try:
            raw, ext = avetaar_decode_image(payload)
        except Exception:
            print(f"[Avetaar] تعذّر فك الرد:\n{avetaar_resp.text[:400]}")
            return "error"
    out = f"avetaar_generated_{time.strftime('%Y%m%d_%H%M%S')}.{ext}"
    with open(out, "wb") as f:
        f.write(raw)
    print(f"[Avetaar] تم حفظ الصورة: {out} ({len(raw) // 1024}KB)")
    return "ok"


async def avetaar_image(avetaar_client, avetaar_nonce, avetaar_prompt, avetaar_ref_path=None):
    avetaar_body = {"prompt": avetaar_prompt}
    if avetaar_ref_path:
        avetaar_body["image"] = avetaar_data_url(avetaar_ref_path)
    r = await avetaar_client.post("/wp-json/glm-chat/v1/image", json=avetaar_body,
        headers={"Accept": "*/*",
                 "Content-Type": "application/json",
                 "Origin": AVETAAR_TARGET,
                 "Referer": AVETAAR_TARGET + "/",
                 "X-GLM-Nonce": avetaar_nonce,
                 "X-Requested-With": "com.duckduckgo.mobile.android"})
    if r.status_code == 403:
        return "expired"
    if r.status_code != 200:
        print(f"[Avetaar] HTTP {r.status_code}: {r.text[:300]}")
        return "error"
    return await avetaar_save_image(avetaar_client, r)


async def avetaar_vision(avetaar_client, avetaar_nonce, avetaar_model, avetaar_prompt, avetaar_image_path):
    return await avetaar_stream(avetaar_client, avetaar_nonce,
        {"model": avetaar_model,
         "messages": [{"role": "user", "content": avetaar_prompt}],
         "image": avetaar_data_url(avetaar_image_path)})

AVETAAR_VISION_MODEL = "glm-5.3-flash"


def avetaar_extract_image(avetaar_obj):
    if isinstance(avetaar_obj, str):
        s = avetaar_obj.strip()
        if s.startswith("data:image") or s.startswith("http"):
            return s
        if len(s) > 100 and all(c in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=" for c in s):
            return s
        return None
    if isinstance(avetaar_obj, dict):
        for key in ("image", "data", "b64_json", "url", "result", "output"):
            found = avetaar_extract_image(avetaar_obj.get(key))
            if found:
                return found
    if isinstance(avetaar_obj, list):
        for item in avetaar_obj:
            found = avetaar_extract_image(item)
            if found:
                return found
    return None

def avetaar_decode_image(avetaar_payload):
    if avetaar_payload.startswith("data:image"):
        header, b64 = avetaar_payload.split(",", 1)
        ext = header.rsplit("/", 1)[-1].split(";")[0]
        return base64.b64decode(b64), ext
    raw = base64.b64decode(avetaar_payload)
    if raw[:4] == b"\x89PNG":
        return raw, "png"
    if raw[:3] == b"\xff\xd8\xff":
        return raw, "jpeg"
    if raw[:4] == b"GIF8":
        return raw, "gif"
    if raw[4:8] == b"WEBP":
        return raw, "webp"
    return raw, "bin"

def avetaar_menu():
    print("\n[Avetaar] اختر:")
    print("  1) محادثة نصية")
    print("  2) توليد صورة")
    print("  3) سؤال عن صورة")
    print("  0) خروج")
    return input("[Avetaar] > ").strip()

def avetaar_pick_model(avetaar_models, avetaar_default):
    print("[Avetaar] النماذج:")
    for i, m in enumerate(avetaar_models, 1):
        print(f"  {i}) {m}" + (" (الافتراضي)" if m == avetaar_default else ""))
    choice = input(f"[Avetaar] اختر رقم (Enter = {avetaar_default}): ").strip()
    if not choice:
        return avetaar_default
    try:
        n = int(choice)
    except ValueError:
        return avetaar_default
    if 1 <= n <= len(avetaar_models):
        return avetaar_models[n - 1]
    return avetaar_default


async def avetaar_main():
    print("𓂀 Avetaar GLM-AI 𓂀 Dev: @Avetaar")
    async with httpx.AsyncClient(base_url=AVETAAR_TARGET,
                                 headers={"User-Agent": AVETAAR_UA},
                                 timeout=httpx.Timeout(180.0, connect=15.0),
                                 follow_redirects=True) as avetaar_client:
        try:
            avetaar_nonce = await avetaar_session(avetaar_client)
        except Exception as e:
            print(f"[Avetaar] تعذّر فتح الجلسة: {e}")
            return
        print("[Avetaar] الجلسة جاهزة")
        while True:
            mode = avetaar_menu()
            if mode == "0":
                break
            if mode == "1":
                model = avetaar_pick_model(AVETAAR_CHAT_MODELS, AVETAAR_DEFAULT_MODEL)
                prompt = input("[Avetaar] نصك: ").strip()
                if not prompt:
                    continue
                status = await avetaar_text(avetaar_client, avetaar_nonce, model, prompt)
            elif mode == "2":
                prompt = input("[Avetaar] وصف الصورة: ").strip()
                if not prompt:
                    continue
                ref = input("[Avetaar] صورة مرجعية (اختياري، Enter لتخطي): ").strip()
                try:
                    status = await avetaar_image(avetaar_client, avetaar_nonce, prompt, ref or None)
                except ValueError as e:
                    print(f"[Avetaar] {e}")
                    continue
            elif mode == "3":
                img = input("[Avetaar] مسار الصورة: ").strip()
                if not os.path.exists(img):
                    print("[Avetaar] الملف غير موجود")
                    continue
                prompt = input("[Avetaar] سؤالك عن الصورة: ").strip()
                if not prompt:
                    continue
                try:
                    status = await avetaar_vision(avetaar_client, avetaar_nonce,
                        AVETAAR_VISION_MODEL, prompt, img)
                except ValueError as e:
                    print(f"[Avetaar] {e}")
                    continue
            else:
                print("[Avetaar] اختيار غير صالح")
                continue
            if status == "expired":
                print("[Avetaar] الجلسة منتهية — تحديثها")
                try:
                    avetaar_nonce = await avetaar_session(avetaar_client)
                except Exception as e:
                    print(f"[Avetaar] تعذّر فتح الجلسة: {e}")
                    break

if __name__ == "__main__":
    asyncio.run(avetaar_main())
