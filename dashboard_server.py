# -*- coding: utf-8 -*-
"""
FreeFire Level Up Bot - Professional Real-Time Dashboard Backend
Live counters, mode detection (BR / Lone Wolf), match complete tracking, pause control.
Updated with Non-blocking I/O and Advanced Error Logging.
"""

import asyncio
import json
import os
import time
from collections import deque
from typing import Dict, List, Any, Optional
from aiohttp import web

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "index.html")
ACCOUNTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "accounts.json")
CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "token_cache.json")
DEVICES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "devices.json")

# ==================== EXP TABLE ====================
EXP_TABLE: Dict[int, int] = {
    1: 0, 2: 48, 3: 202, 4: 544, 5: 1012, 6: 1844, 7: 2792, 8: 3800,
    9: 4870, 10: 6004, 11: 7192, 12: 8448, 13: 9760, 14: 11140, 15: 12566,
    16: 14060, 17: 15610, 18: 17224, 19: 18902, 20: 20632, 21: 22424, 22: 24278,
    23: 26192, 24: 28166, 25: 30200, 26: 32294, 27: 34448, 28: 37804, 29: 41274,
    30: 44870, 31: 48582, 32: 53394, 33: 58566, 34: 64096, 35: 69994, 36: 76460,
    37: 83506, 38: 91128, 39: 99322, 40: 108092, 41: 120144, 42: 133266, 43: 147472,
    44: 162760, 45: 179126, 46: 196572, 47: 215368, 48: 235516, 49: 257010, 50: 279860,
    51: 304056, 52: 348318, 53: 394982, 54: 444044, 55: 495508, 56: 549364, 57: 633756,
    58: 721744, 59: 813336, 60: 908522, 61: 1041438, 62: 1180352, 63: 1325266,
    64: 1476184, 65: 1634300, 66: 1840946, 67: 2056594, 68: 2281242, 69: 2514880,
    70: 2757530, 71: 3059506, 72: 3372284, 73: 3699456, 74: 4041030, 75: 4397002,
    76: 4829104, 77: 5282204, 78: 5756304, 79: 6251408, 80: 6776502, 81: 7381324,
    82: 8043154, 83: 8752982, 84: 9510808, 85: 10316338, 86: 11277190, 87: 12291748,
    88: 13360304, 89: 14482858, 90: 15659418, 91: 17026708, 92: 18453950, 93: 19941280,
    94: 21488570, 95: 23095858, 96: 24763138, 97: 26490428, 98: 28378704, 99: 30124996,
    100: 32032884
}

# ==================== ASYNC HELPER FUNCTIONS ====================
async def read_json(filepath: str, default_val: Any = None) -> Any:
    if default_val is None:
        default_val = []
    if not os.path.exists(filepath):
        return default_val
    try:
        def _read():
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        return await asyncio.to_thread(_read)
    except Exception:
        return default_val

async def write_json(filepath: str, data: Any) -> bool:
    try:
        def _write():
            tmp_file = filepath + ".tmp"
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_file, filepath)
        await asyncio.to_thread(_write)
        return True
    except Exception as e:
        print(f"Failed to write file {os.path.basename(filepath)}: {e}")
        return False


def calculate_level_progress(level: int, current_exp: int) -> Dict[str, Any]:
    level = max(1, int(level or 1))
    next_level = level + 1
    
    base_exp = EXP_TABLE.get(level, EXP_TABLE.get(100, 32032884) + (level - 100) * 50000)
    target_exp = EXP_TABLE.get(next_level, base_exp + 50000)

    needed_for_level = max(1, target_exp - base_exp)
    earned_in_level = max(0, current_exp - base_exp)
    remaining_exp = max(0, target_exp - current_exp)
    progress_pct = min(100.0, max(0.0, (earned_in_level / needed_for_level) * 100.0))

    return {
        "next_level": next_level,
        "base_exp": base_exp,
        "target_exp": target_exp,
        "needed_for_level": needed_for_level,
        "earned_in_level": earned_in_level,
        "remaining_exp": remaining_exp,
        "progress_pct": round(progress_pct, 1),
    }


def get_game_mode(level: int, match_type: Optional[str] = None) -> Dict[str, str]:
    if match_type:
        mt = str(match_type).upper()
        if mt in ("BR", "BATTLE_ROYALE", "BATTLE ROYALE"):
            return {"mode": "BR", "label": "Battle Royale"}
        if mt in ("LONE_WOLF", "LW", "LONE WOLF"):
            return {"mode": "LONE_WOLF", "label": "Lone Wolf"}

    lvl = int(level or 1)
    if lvl < 3:
        return {"mode": "BR", "label": "Battle Royale (Level < 3)"}
    return {"mode": "LONE_WOLF", "label": "Lone Wolf (Level 3+)"}


# ==================== BOT STATE ====================
class BotState:
    def __init__(self):
        self.accounts: Dict[str, Dict[str, Any]] = {}
        self.logs: List[Dict[str, Any]] = []
        self.max_logs = 300

        self.total_matches_started = 0
        self.total_matches_finished = 0
        self.total_gained_exp = 0

        self.start_time = time.time()
        self.exp_history: deque = deque(maxlen=720)
        self._last_history_push = 0.0

        self.account_workers: Dict[str, asyncio.Task] = {}
        self.account_credentials: Dict[str, Dict[str, Any]] = {}
        self.auth_to_game_id: Dict[str, str] = {}
        self.game_to_auth_id: Dict[str, str] = {}
        self.account_token_map: Dict[str, str] = {}

        self.paused_accounts: set = set()
        self.refresh_callbacks: Dict[str, Any] = {}
        self.active_writers: Dict[str, set] = {}

    def log(self, message: str, level: str = "info", uid: Optional[str] = None):
        entry = {
            "time": time.strftime("%H:%M:%S"),
            "level": level,
            "message": str(message),
            "uid": str(uid) if uid else None,
        }
        self.logs.append(entry)
        if len(self.logs) > self.max_logs:
            self.logs = self.logs[-self.max_logs:]
        
        prefix = f"[{entry['time']}] [{level.upper()}]"
        print(f"{prefix} {message}")

    def register_writer(self, uid: str, writer):
        uid_str = str(uid)
        self.active_writers.setdefault(uid_str, set()).add(writer)

    def unregister_writer(self, uid: str, writer):
        uid_str = str(uid)
        if uid_str in self.active_writers:
            self.active_writers[uid_str].discard(writer)
            if not self.active_writers[uid_str]:
                self.active_writers.pop(uid_str, None)

    def close_writers_for_account(self, uid: str):
        uid_str = str(uid)
        candidates = {uid_str}
        if uid_str in self.auth_to_game_id:
            candidates.add(str(self.auth_to_game_id[uid_str]))
        if uid_str in self.game_to_auth_id:
            candidates.add(str(self.game_to_auth_id[uid_str]))

        for c in list(candidates):
            for w in list(self.active_writers.get(c, [])):
                try:
                    if hasattr(w, "close") and (not hasattr(w, "is_closing") or not w.is_closing()):
                        w.close()
                except Exception:
                    pass
            self.active_writers.pop(c, None)

    def register_account(
        self,
        uid: str,
        nickname: str,
        region: str,
        level: int,
        exp: int,
        likes: int = 0,
        token: Optional[str] = None,
        auth_uid: Optional[str] = None,
    ):
        uid_str = str(uid)
        auth_uid_str = str(auth_uid) if auth_uid else self.game_to_auth_id.get(uid_str, "")

        if auth_uid_str:
            self.auth_to_game_id[auth_uid_str] = uid_str
            self.game_to_auth_id[uid_str] = auth_uid_str
            self.account_token_map[auth_uid_str] = uid_str
            self.account_token_map[uid_str] = auth_uid_str

        if token:
            self.account_token_map[uid_str] = token
            self.account_token_map[token[:16]] = uid_str
            if auth_uid_str:
                self.account_token_map[auth_uid_str] = token

        prog = calculate_level_progress(level or 1, exp)
        lvl_val = int(level or 1)
        mode_info = get_game_mode(lvl_val)

        if uid_str not in self.accounts:
            self.accounts[uid_str] = {
                "uid": uid_str,
                "auth_uid": auth_uid_str or "",
                "nickname": nickname or f"Player_{uid_str[:6]}",
                "region": region or "ID",
                "level": lvl_val,
                "next_level": prog["next_level"],
                "mode": mode_info["mode"],
                "mode_label": mode_info["label"],
                "actual_mode": None,
                "initial_exp": int(exp or 0),
                "current_exp": int(exp or 0),
                "gained_exp": 0,
                "remaining_exp": prog["remaining_exp"],
                "target_exp": prog["target_exp"],
                "needed_for_level": prog["needed_for_level"],
                "earned_in_level": prog["earned_in_level"],
                "progress_pct": prog["progress_pct"],
                "likes": int(likes or 0),
                "status": "PAUSED" if self.is_paused(uid_str) else "ONLINE",
                "is_paused": self.is_paused(uid_str),
                "matches_played": 0,
                "active_matches": 0,
                "last_match_time": None,
                "token": token or "",
                "start_time": time.time(),
                "paused_at": time.time() if self.is_paused(uid_str) else None,
                "total_pause_duration": 0.0,
                "last_updated": time.strftime("%H:%M:%S"),
            }
        else:
            acc = self.accounts[uid_str]
            if auth_uid_str:
                acc["auth_uid"] = auth_uid_str
            if nickname:
                acc["nickname"] = nickname
            if region:
                acc["region"] = region
            if level:
                acc["level"] = lvl_val
            if token:
                acc["token"] = token
            acc["current_exp"] = int(exp or 0)
            acc["gained_exp"] = max(0, int(exp or 0) - acc["initial_exp"])
            acc["next_level"] = prog["next_level"]
            acc["remaining_exp"] = prog["remaining_exp"]
            acc["target_exp"] = prog["target_exp"]
            acc["needed_for_level"] = prog["needed_for_level"]
            acc["earned_in_level"] = prog["earned_in_level"]
            acc["progress_pct"] = prog["progress_pct"]
            acc["likes"] = int(likes or 0)
            if not acc.get("actual_mode"):
                acc["mode"] = mode_info["mode"]
                acc["mode_label"] = mode_info["label"]
            acc["last_updated"] = time.strftime("%H:%M:%S")

        self.recalc_totals()

    def update_exp(self, uid: str, current_exp: int, level: Optional[int] = None):
        uid_str = str(uid)
        if uid_str not in self.accounts:
            return

        acc = self.accounts[uid_str]
        old_exp = acc["current_exp"]
        old_level = int(acc.get("level", 1))

        acc["current_exp"] = int(current_exp)
        if level is not None and level > 0:
            acc["level"] = int(level)

        new_level = int(acc["level"])
        acc["gained_exp"] = max(0, acc["current_exp"] - acc["initial_exp"])

        if not acc.get("actual_mode"):
            mode_info = get_game_mode(new_level)
            acc["mode"] = mode_info["mode"]
            acc["mode_label"] = mode_info["label"]

        prog = calculate_level_progress(new_level, acc["current_exp"])
        acc.update({
            "next_level": prog["next_level"],
            "remaining_exp": prog["remaining_exp"],
            "target_exp": prog["target_exp"],
            "needed_for_level": prog["needed_for_level"],
            "earned_in_level": prog["earned_in_level"],
            "progress_pct": prog["progress_pct"],
            "last_updated": time.strftime("%H:%M:%S"),
        })

        if new_level > old_level:
            self.log(f"🎉 LEVEL UP! {acc['nickname']} ({uid_str}) → Level {new_level}", "success", uid_str)

        diff = acc["current_exp"] - old_exp
        if diff > 0:
            self.log(f"★ {acc['nickname']} ({uid_str}) +{diff:,} EXP", "success", uid_str)

        self.recalc_totals()

    def set_match_mode(self, uid: str, match_type: str):
        uid_str = str(uid)
        if uid_str not in self.accounts:
            return
        mode_info = get_game_mode(self.accounts[uid_str].get("level", 1), match_type=match_type)
        self.accounts[uid_str]["mode"] = mode_info["mode"]
        self.accounts[uid_str]["mode_label"] = mode_info["label"]
        self.accounts[uid_str]["actual_mode"] = mode_info["mode"]

    def clear_match_mode(self, uid: str):
        uid_str = str(uid)
        if uid_str not in self.accounts:
            return
        self.accounts[uid_str]["actual_mode"] = None
        lvl = self.accounts[uid_str].get("level", 1)
        mode_info = get_game_mode(lvl)
        self.accounts[uid_str]["mode"] = mode_info["mode"]
        self.accounts[uid_str]["mode_label"] = mode_info["label"]

    def increment_match_started(self):
        self.total_matches_started += 1

    def increment_match_finished(self, uid: str):
        self.total_matches_finished += 1
        uid_str = str(uid)
        if uid_str in self.accounts:
            self.accounts[uid_str]["matches_played"] += 1
            self.accounts[uid_str]["last_match_time"] = time.strftime("%H:%M:%S")
            self.log(f"✅ Match #{self.accounts[uid_str]['matches_played']} COMPLETED for {uid_str}", "success", uid_str)

    def update_status(self, uid: str, status: str, active_matches: Optional[int] = None):
        uid_str = str(uid)
        if uid_str not in self.accounts:
            return
        if not self.is_paused(uid_str):
            self.accounts[uid_str]["status"] = status
        if active_matches is not None:
            self.accounts[uid_str]["active_matches"] = int(active_matches)

    def is_paused(self, uid: str) -> bool:
        uid_str = str(uid)
        if uid_str in self.paused_accounts:
            return True
        game_id = self.auth_to_game_id.get(uid_str)
        if game_id and game_id in self.paused_accounts:
            return True
        auth_uid = self.game_to_auth_id.get(uid_str)
        if auth_uid and auth_uid in self.paused_accounts:
            return True
        return False

    def toggle_pause(self, uid: str) -> bool:
        uid_str = str(uid)
        candidates = {uid_str}
        if uid_str in self.auth_to_game_id:
            candidates.add(self.auth_to_game_id[uid_str])
        if uid_str in self.game_to_auth_id:
            candidates.add(self.game_to_auth_id[uid_str])

        target_acc = None
        for c in candidates:
            if c in self.accounts:
                target_acc = self.accounts[c]
                break

        is_now_paused = not self.is_paused(uid_str)

        if is_now_paused:
            for c in candidates:
                self.paused_accounts.add(c)
                self.close_writers_for_account(c)
            if target_acc:
                target_acc["is_paused"] = True
                target_acc["status"] = "PAUSED"
        else:
            for c in candidates:
                self.paused_accounts.discard(c)
            if target_acc:
                target_acc["is_paused"] = False
                target_acc["status"] = "ONLINE"

        if "on_pause_toggle" in self.refresh_callbacks:
            try:
                asyncio.create_task(self.refresh_callbacks["on_pause_toggle"](uid_str, is_now_paused))
            except Exception:
                pass

        return is_now_paused

    def toggle_pause_all(self) -> bool:
        any_active = any(not self.is_paused(k) for k in list(self.accounts.keys()))
        for k in list(self.accounts.keys()):
            current_paused = self.is_paused(k)
            if any_active and not current_paused:
                self.toggle_pause(k)
            elif not any_active and current_paused:
                self.toggle_pause(k)
        return any_active

    def get_account_uptime(self, uid_str: str) -> int:
        acc = self.accounts.get(uid_str)
        if not acc:
            return 0
        start_t = acc.get("start_time", time.time())
        return max(0, int(time.time() - start_t))

    def recalc_totals(self):
        self.total_gained_exp = sum(int(a.get("gained_exp", 0)) for a in self.accounts.values())

    def push_exp_history(self):
        now = time.time()
        if now - self._last_history_push < 10:
            return
        self._last_history_push = now
        self.exp_history.append({"t": int(now), "exp": int(self.total_gained_exp)})

    def get_live_stats(self) -> Dict[str, Any]:
        total_active = sum(int(a.get("active_matches", 0)) for a in self.accounts.values())
        uptime_sec = max(1, int(time.time() - self.start_time))
        exp_per_hour = int((self.total_gained_exp / uptime_sec) * 3600)
        matches_per_hour = int((self.total_matches_started / uptime_sec) * 3600)
        return {
            "total_accounts": len(self.accounts),
            "total_matches_started": self.total_matches_started,
            "total_matches_finished": self.total_matches_finished,
            "total_matches_completed": self.total_matches_finished,
            "matches_completed_per_hour": int((self.total_matches_finished / uptime_sec) * 3600),
            "total_active_matches": total_active,
            "total_gained_exp": self.total_gained_exp,
            "exp_per_hour": exp_per_hour,
            "matches_per_hour": matches_per_hour,
            "uptime": uptime_sec,
        }

bot_state = BotState()


# ==================== HTTP HANDLERS ====================
async def handle_index(request: web.Request) -> web.Response:
    content = "<h1>Loading Dashboard...</h1>"
    if os.path.exists(TEMPLATE_PATH):
        try:
            def _read():
                with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
                    return f.read()
            content = await asyncio.to_thread(_read)
        except Exception:
            pass
    return web.Response(text=content, content_type="text/html", charset="utf-8")

async def handle_get_stats(request: web.Request) -> web.Response:
    bot_state.push_exp_history()
    accounts_data = list(bot_state.accounts.values())
    accounts_data.sort(key=lambda x: x.get("gained_exp", 0), reverse=True)

    for acc in accounts_data:
        uid_k = str(acc.get("uid", ""))
        acc["uptime_seconds"] = bot_state.get_account_uptime(uid_k)
        acc["is_paused"] = bot_state.is_paused(uid_k)
        if acc["is_paused"]:
            acc["status"] = "PAUSED"

    return web.json_response({
        **bot_state.get_live_stats(),
        "accounts": accounts_data,
        "logs": bot_state.logs[-100:],
        "exp_history": list(bot_state.exp_history),
    })

async def handle_add_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        existing = await read_json(ACCOUNTS_FILE, [])

        if data.get("uid") and data.get("password"):
            uid = str(data["uid"]).strip()
            pwd = str(data["password"]).strip()
            existing = [a for a in existing if str(a.get("uid", "")) != uid]
            existing.append({"uid": uid, "password": pwd})
        elif data.get("token"):
            token = str(data["token"]).strip()
            existing = [a for a in existing if a.get("token") != token]
            existing.append({"token": token})
        else:
            return web.json_response({"status": "error", "error": "Invalid payload"}, status=400)

        await write_json(ACCOUNTS_FILE, existing)
        
        cb = bot_state.refresh_callbacks.get("on_account_added")
        if cb:
            asyncio.create_task(cb(data))

        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)

async def handle_delete_account(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        req_uid = str(data.get("uid", "")).strip()
        
        existing = await read_json(ACCOUNTS_FILE, [])
        kept = [a for a in existing if str(a.get("uid", "")) != req_uid and str(a.get("token", "")) != req_uid]
        await write_json(ACCOUNTS_FILE, kept)

        bot_state.accounts.pop(req_uid, None)
        bot_state.close_writers_for_account(req_uid)

        cb = bot_state.refresh_callbacks.get("on_account_deleted")
        if cb:
            asyncio.create_task(cb([req_uid]))

        return web.json_response({"status": "ok", "deleted": [req_uid]})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)}, status=500)

async def handle_toggle_pause(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        uid = str(data.get("uid", "")).strip()
        is_paused = bot_state.toggle_pause(uid)
        return web.json_response({"status": "ok", "is_paused": is_paused})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})

async def handle_toggle_pause_all(request: web.Request) -> web.Response:
    try:
        all_paused = bot_state.toggle_pause_all()
        return web.json_response({"status": "ok", "all_paused": all_paused})
    except Exception as e:
        return web.json_response({"status": "error", "error": str(e)})

async def handle_clear_logs(request: web.Request) -> web.Response:
    bot_state.logs.clear()
    return web.json_response({"status": "ok"})

async def handle_stale_accounts(request: web.Request) -> web.Response:
    persisted = await read_json(ACCOUNTS_FILE, [])
    result = []
    for acc in persisted:
        uid = str(acc.get("uid", ""))
        result.append({"uid": uid, "has_live": uid in bot_state.accounts})
    return web.json_response({"status": "ok", "accounts": result})

async def handle_cleanup_stale(request: web.Request) -> web.Response:
    persisted = await read_json(ACCOUNTS_FILE, [])
    live_ids = set(bot_state.accounts.keys())
    kept = [a for a in persisted if str(a.get("uid", "")) in live_ids]
    await write_json(ACCOUNTS_FILE, kept)
    return web.json_response({"status": "ok", "removed": len(persisted) - len(kept)})

async def start_web_dashboard(host: str = "0.0.0.0", port: int = 30058):
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/stats", handle_get_stats)
    app.router.add_post("/api/account/add", handle_add_account)
    app.router.add_post("/api/account/delete", handle_delete_account)
    app.router.add_post("/api/account/pause", handle_toggle_pause)
    app.router.add_post("/api/account/pause_all", handle_toggle_pause_all)
    app.router.add_post("/api/logs/clear", handle_clear_logs)
    app.router.add_get("/api/accounts/persisted", handle_stale_accounts)
    app.router.add_post("/api/accounts/cleanup_stale", handle_cleanup_stale)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return runner