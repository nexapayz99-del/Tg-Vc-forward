import os
import sys
import time
import asyncio
import logging
import subprocess
import psutil
from typing import Dict, Set, List, Optional

# ==================== PYROGRAM ANTI-CRASH PATCH ====================
# This intercepts and mocks missing Pyrogram Voice Chat dependencies
# so py-tgcalls can initialize safely on any library version.
import pyrogram.errors
class DummyGroupcallError(pyrogram.errors.BadRequest):
    pass

for _err in ['GroupcallForbidden','GroupCallForbidden','GroupcallInvalid','GroupCallInvalid','GroupcallNotFound','GroupCallNotFound']:
    if not hasattr(pyrogram.errors, _err):
        setattr(pyrogram.errors, _err, DummyGroupcallError)

try:
    import pyrogram.errors.exceptions.forbidden_403 as _f403
    for _err in ['GroupcallForbidden','GroupCallForbidden']:
        if not hasattr(_f403, _err): setattr(_f403, _err, DummyGroupcallError)
except ImportError: pass

try:
    import pyrogram.errors.exceptions.bad_request_400 as _b400
    for _err in ['GroupcallInvalid','GroupCallInvalid','GroupcallNotFound','GroupCallNotFound']:
        if not hasattr(_b400, _err): setattr(_b400, _err, DummyGroupcallError)
except ImportError: pass

import pyrogram.raw.functions.phone as _phone_ns
_FALLBACK_RAWS = [
    "CreateConferenceCall", "JoinGroupCall", "LeaveGroupCall", "DiscardGroupCall",
    "EditGroupCallTitle", "GetGroupCall", "GetGroupCallParticipants",
    "InviteToGroupCall", "ToggleGroupCallSettings", "SendGroupCallBroadcast",
]
for _name in _FALLBACK_RAWS:
    if not hasattr(_phone_ns, _name):
        class _Dummy:
            ID = 0
            def __init__(self, *a, **k): pass
            async def resolve(self, *a, **k): return b""
            @staticmethod
            def read(b): return None
            def write(self): return b""
        _Dummy.__name__ = _name
        setattr(_phone_ns, _name, _Dummy)
# ===================================================================

import pyromod
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from pyrogram.errors import SessionPasswordNeeded, MessageNotModified
from pytgcalls import PyTgCalls
from pytgcalls.types import MediaStream, AudioQuality, RecordStream
from pytgcalls.types.raw import AudioParameters

import config

# ==================== DEBUG LOGGING SETUP ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot_debug.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("NobiFightBot")

START_TIME = time.time()

# ==================== MONGODB INITIALIZATION ====================
mongo_client = AsyncIOMotorClient(config.MONGO_URI)
db = mongo_client[config.DB_NAME]
users_col = db["users"]
access_col = db["access"]

bot = Client(
    "NobiFightBot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
)


# ==================== HELPER: REAL-TIME PROGRESS BAR ====================
async def show_progress_bar(msg: Message, action_name: str, total_steps: int = 5):
    for step in range(1, total_steps + 1):
        percent = int((step / total_steps) * 100)
        filled = "█" * step
        empty = "░" * (total_steps - step)
        bar = f"[{filled}{empty}] {percent}%"
        try:
            await msg.edit_text(
                f"⚙️ **{action_name} in progress...**\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"{bar}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"Please wait..."
            )
        except MessageNotModified:
            pass
        except Exception:
            pass
        await asyncio.sleep(0.3)


# ==================== PER-USER ISOLATED WORKSPACE ====================
class UserWorkspace:
    def __init__(self, user_id: int, doc: dict):
        self.user_id = user_id
        self.source_chat: Optional[int] = doc.get("source_chat")
        self.recording: bool = False
        self.muted: bool = False
        self.targets: Dict[int, int] = {}

        self.recorder_session: str = doc.get("recorder", "")
        self.broadcaster_sessions: List[str] = list(doc.get("broadcasters", []))

        self.recorder_user: Optional[Client] = None
        self.recorder_call: Optional[PyTgCalls] = None
        self.broadcaster_users: List[Client] = []
        self.broadcaster_calls: List[PyTgCalls] = []

        self.fifo_path = f"/tmp/nobi_fifo_{user_id}.pcm"
        self.sink_name = f"FightSink_{user_id}"
        self.ffmpeg_proc: Optional[asyncio.subprocess.Process] = None

        saved_fx = doc.get("effects", {})
        self.effects = {
            "level": saved_fx.get("level", 1.0),
            "bass": saved_fx.get("bass", 0.0),
            "treble": saved_fx.get("treble", 0.0),
            "gain": saved_fx.get("gain", 0.0),
            "overdrive": saved_fx.get("overdrive", 0.0),
            "echo": tuple(saved_fx["echo"]) if saved_fx.get("echo") else None,
        }

        self._setup_audio_pipe()

    def _setup_audio_pipe(self):
        if os.path.exists(self.fifo_path):
            try: os.remove(self.fifo_path)
            except Exception: pass
        os.mkfifo(self.fifo_path)

        try:
            subprocess.run(
                ["pactl", "load-module", "module-null-sink", f"sink_name={self.sink_name}"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass

    async def save_to_mongo(self):
        doc = {
            "user_id": self.user_id,
            "recorder": self.recorder_session,
            "broadcasters": self.broadcaster_sessions,
            "source_chat": self.source_chat,
            "effects": {
                **self.effects,
                "echo": list(self.effects["echo"]) if self.effects["echo"] else None,
            },
        }
        await users_col.update_one({"user_id": self.user_id}, {"$set": doc}, upsert=True)
        logger.info(f"MongoDB synced successfully for user {self.user_id}")

    def build_ffmpeg_filters(self) -> str:
        fx = self.effects
        af = []
        if fx["level"] != 1.0: af.append(f"volume={fx['level']}")
        if fx["gain"] != 0.0: af.append(f"volume={fx['gain']}dB")
        if fx["bass"] != 0.0: af.append(f"bass=g={fx['bass']}:f=110:w=0.6")
        if fx["treble"] != 0.0: af.append(f"treble=g={fx['treble']}:f=3200:w=0.5")
        if fx["overdrive"] > 0:
            d = min(max(fx["overdrive"], 1.0), 64.0)
            af.append(f"acrusher=level_in={d}:level_out={d*1.5}:bits=8:mode=log:aa=1")
        if fx["echo"]:
            delay, decay = fx["echo"]
            af.append(f"aecho=0.8:0.9:{delay}:{decay}")
        return f"-af {','.join(af)}" if af else ""

    def get_target_stream(self) -> MediaStream:
        af_str = self.build_ffmpeg_filters()
        return MediaStream(
            f"{self.sink_name}.monitor",
            audio_parameters=AudioQuality.STUDIO,
            ffmpeg_parameters=f"-f pulse {af_str}".strip(),
        )

    async def start_bridge(self):
        await self.stop_bridge()
        cmd = [
            "ffmpeg", "-y", "-f", "s16le", "-ac", "2", "-ar", "48000",
            "-i", self.fifo_path, "-f", "pulse", self.sink_name,
        ]
        self.ffmpeg_proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )

    async def stop_bridge(self):
        if self.ffmpeg_proc:
            try:
                self.ffmpeg_proc.terminate()
                await self.ffmpeg_proc.wait()
            except Exception: pass
            self.ffmpeg_proc = None

    async def set_recorder(self, session_str: str, save: bool = True) -> bool:
        if not session_str: return False
        try:
            if self.recorder_user:
                try: await self.recorder_user.stop()
                except Exception: pass
            u_cli = Client(
                f"Rec_{self.user_id}_{int(time.time())}",
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                session_string=session_str,
                in_memory=True,
                no_updates=True,
            )
            await u_cli.start()
            c_cli = PyTgCalls(u_cli)
            try: await c_cli.start()
            except Exception as call_err: logger.warning(f"PyTgCalls start notice (Recorder): {call_err}")

            self.recorder_user = u_cli
            self.recorder_call = c_cli
            self.recorder_session = session_str
            if save: await self.save_to_mongo()
            logger.info(f"Recorder connected successfully for user {self.user_id}")
            return True
        except Exception as e:
            logger.error(f"User {self.user_id} Recorder error: {e}")
            self.recorder_session = ""
            if save: await self.save_to_mongo()
            return False

    async def add_broadcaster(self, session_str: str, save: bool = True) -> bool:
        if not session_str: return False
        try:
            u_cli = Client(
                f"Bcast_{self.user_id}_{len(self.broadcaster_users)+1}_{int(time.time())}",
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                session_string=session_str,
                in_memory=True,
                no_updates=True,
            )
            await u_cli.start()
            c_cli = PyTgCalls(u_cli)
            try: await c_cli.start()
            except Exception as call_err: logger.warning(f"PyTgCalls start notice (Broadcaster): {call_err}")

            self.broadcaster_users.append(u_cli)
            self.broadcaster_calls.append(c_cli)
            if save and session_str not in self.broadcaster_sessions:
                self.broadcaster_sessions.append(session_str)
                await self.save_to_mongo()
            logger.info(f"Broadcaster connected successfully for user {self.user_id}")
            return True
        except Exception as e:
            logger.error(f"User {self.user_id} Broadcaster error: {e}")
            if session_str in self.broadcaster_sessions:
                self.broadcaster_sessions.remove(session_str)
                if save: await self.save_to_mongo()
            return False

    def get_free_broadcaster(self) -> Optional[int]:
        used = set(self.targets.values())
        for i in range(len(self.broadcaster_calls)):
            if i not in used: return i
        return None

    async def reload_streams(self):
        if not self.targets: return
        stream = self.get_target_stream()
        for t_id, b_idx in list(self.targets.items()):
            try:
                await self.broadcaster_calls[b_idx].play(t_id, stream)
                if self.muted: await self.broadcaster_calls[b_idx].mute_stream(t_id)
            except Exception as e: logger.warning(f"Reload failed on target {t_id} for user {self.user_id}: {e}")

    async def leave_all(self) -> int:
        count = len(self.targets)
        for t_id, b_idx in list(self.targets.items()):
            try: await self.broadcaster_calls[b_idx].leave_call(t_id)
            except Exception: pass
        self.targets.clear()

        if self.source_chat and self.recorder_call:
            try: await self.recorder_call.leave_call(self.source_chat)
            except Exception: pass
        await self.stop_bridge()
        self.recording = False
        return count


# ==================== GLOBAL ACCESS & WORKSPACE MANAGER ====================
class GlobalManager:
    def __init__(self):
        self.approved_users: Set[int] = {config.OWNER_ID}
        self.banned_users: Set[int] = set()
        self.notified_pending: Set[int] = set()
        self.workspaces: Dict[int, UserWorkspace] = {}

    async def load_from_mongo(self):
        acc_doc = await access_col.find_one({"_id": "global_access"})
        if acc_doc:
            self.approved_users = set(acc_doc.get("approved_users", [config.OWNER_ID]))
            self.approved_users.add(config.OWNER_ID)
            self.banned_users = set(acc_doc.get("banned_users", []))
        else:
            await self.save_access_to_mongo()

        async for u_doc in users_col.find({}):
            uid = u_doc["user_id"]
            if uid in self.banned_users: continue
            ws = UserWorkspace(uid, u_doc)
            self.workspaces[uid] = ws
            if ws.recorder_session:
                await ws.set_recorder(ws.recorder_session, save=False)
            for b_str in list(ws.broadcaster_sessions):
                await ws.add_broadcaster(b_str, save=False)

    async def save_access_to_mongo(self):
        await access_col.update_one(
            {"_id": "global_access"},
            {"$set": {"approved_users": list(self.approved_users), "banned_users": list(self.banned_users)}},
            upsert=True,
        )

    async def get_ws(self, user_id: int) -> UserWorkspace:
        if user_id not in self.workspaces:
            u_doc = await users_col.find_one({"user_id": user_id}) or {}
            self.workspaces[user_id] = UserWorkspace(user_id, u_doc)
        return self.workspaces[user_id]


MANAGER = GlobalManager()


# ==================== ACCESS GUARD ====================
def is_authorized(_, __, update: Message | CallbackQuery) -> bool:
    user = update.from_user
    if not user: return False
    if user.id in MANAGER.banned_users: return False
    return user.id == config.OWNER_ID or user.id in MANAGER.approved_users

auth_filter = filters.create(is_authorized)


async def handle_unauthorized_user(client: Client, message: Message):
    user = message.from_user
    if not user: return
    if user.id in MANAGER.banned_users:
        return await message.reply_text("🚫 **You are BANNED from using this bot.**")

    user_tag = f"@{user.username}" if user.username else user.mention
    restricted_text = (
        f"⛔  **ACCESS RESTRICTED**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Hello {user_tag},\n\n"
        f"You need approval to use this bot.\n"
        f"📩 Request forwarded to admins.\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"You will be notified once approved."
    )
    await message.reply_text(restricted_text)

    if user.id not in MANAGER.notified_pending:
        MANAGER.notified_pending.add(user.id)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Approve Access", callback_data=f"req_app_{user.id}"),
             InlineKeyboardButton("🚫 Ban User", callback_data=f"req_ban_{user.id}")]
        ])
        try:
            await client.send_message(
                config.OWNER_ID,
                f"📩 **NEW ACCESS REQUEST**\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 **User:** {user.mention} ({user_tag})\n"
                f"🆔 **User ID:** `{user.id}`\n"
                f"━━━━━━━━━━━━━━━━━━━━\n",
                reply_markup=kb,
            )
        except Exception as e: logger.error(f"Could not notify owner: {e}")


# ==================== UI & SUB-MENUS ====================
START_UI = """✨ **Welcome to Audio Forwarder** ✨
━━━━━━━━━━━━━━━━━━━━
👋 **Hello, {mention}!**
━━━━━━━━━━━━━━━━━━━━
🎵 **Real-time Audio Forwarding**
📊 **System:** `██████████` 100% ✅
⚡ **Features:**
• MongoDB Cloud Account Persistence
• 100% Button-Controlled Sub-Menus
• Real-time Forwarding & DSP Effects
• Multi-chat Forwarding
━━━━━━━━━━━━━━━━━━━━
👇 **Select a menu below to control everything:**"""

def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🎵 Audio Controls", callback_data="menu_audio"),
         InlineKeyboardButton("🎛️ Audio Effects", callback_data="menu_fx")],
        [InlineKeyboardButton("📱 My Accounts (DB)", callback_data="menu_acc"),
         InlineKeyboardButton("📊 Utility & Status", callback_data="menu_util")],
        [InlineKeyboardButton("👤 Owner", url=f"tg://user?id={config.OWNER_ID}"),
         InlineKeyboardButton("📖 Help Guide", callback_data="menu_help")]
    ]
    if user_id == config.OWNER_ID:
        rows.append([InlineKeyboardButton("👑 Owner Panel (Approve / Ban / DB)", callback_data="menu_owner")])
    return InlineKeyboardMarkup(rows)

def audio_menu_kb(ws: UserWorkspace) -> InlineKeyboardMarkup:
    rec_lbl = "🟢 Recording (Restart)" if ws.recording else "🎙️ Start Recording (/record)"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(rec_lbl, callback_data="act_record"),
         InlineKeyboardButton("🎯 Set Source Chat", callback_data="act_setsource")],
        [InlineKeyboardButton("➕ Join Target VC (/join)", callback_data="act_join"),
         InlineKeyboardButton("➖ Leave Target VC (/leave)", callback_data="act_leave")],
        [InlineKeyboardButton("🔇 Mute All (/mute)", callback_data="act_mute"),
         InlineKeyboardButton("🔊 Unmute All (/unmute)", callback_data="act_unmute")],
        [InlineKeyboardButton("🔄 Rejoin All (/rejoin)", callback_data="act_rejoin"),
         InlineKeyboardButton("⏹️ Leave Source (/leaverecord)", callback_data="act_leaverec")],
        [InlineKeyboardButton("🛑 Disconnect All (/leaveall)", callback_data="act_leaveall")],
        [InlineKeyboardButton("🏠 Back to Main Menu", callback_data="menu_main")]
    ])

def fx_menu_kb(ws: UserWorkspace) -> InlineKeyboardMarkup:
    fx = ws.effects
    echo_lbl = f"{fx['echo'][0]}ms" if fx["echo"] else "Off"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🔊 Level ({fx['level']})", callback_data="fx_level"),
         InlineKeyboardButton(f"🎸 Bass ({fx['bass']}dB)", callback_data="fx_bass")],
        [InlineKeyboardButton(f"🎻 Treble ({fx['treble']}dB)", callback_data="fx_treble"),
         InlineKeyboardButton(f"⚡ Gain ({fx['gain']}dB)", callback_data="fx_gain")],
        [InlineKeyboardButton(f"🔥 Overdrive ({fx['overdrive']})", callback_data="fx_overdrive"),
         InlineKeyboardButton(f"🗣️ Echo ({echo_lbl})", callback_data="fx_echo")],
        [InlineKeyboardButton("📋 Show Active Effects", callback_data="fx_show"),
         InlineKeyboardButton("🔄 Reset All Effects", callback_data="fx_reset")],
        [InlineKeyboardButton("🏠 Back to Main Menu", callback_data="menu_main")]
    ])

def acc_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📱 Login Account via Phone (OTP & 2FA)", callback_data="acc_otp")],
        [InlineKeyboardButton("🎙️ Set Recorder (String)", callback_data="acc_rec_str"),
         InlineKeyboardButton("➕ Add Broadcaster (String)", callback_data="acc_bcast_str")],
        [InlineKeyboardButton("📋 View My Saved DB Accounts", callback_data="acc_view"),
         InlineKeyboardButton("🗑️ Delete Account from DB", callback_data="acc_del")],
        [InlineKeyboardButton("🏠 Back to Main Menu", callback_data="menu_main")]
    ])

def util_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏓 Ping Latency", callback_data="util_ping"),
         InlineKeyboardButton("📊 System & Bot Stats", callback_data="util_stats")],
        [InlineKeyboardButton("🔗 Active Forward List", callback_data="util_list"),
         InlineKeyboardButton("🆔 My ID & Info", callback_data="util_id")],
        [InlineKeyboardButton("🏠 Back to Main Menu", callback_data="menu_main")]
    ])

def owner_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Approve User ID", callback_data="own_approve"),
         InlineKeyboardButton("❌ Remove Access", callback_data="own_disapprove")],
        [InlineKeyboardButton("🚫 Ban User ID", callback_data="own_ban"),
         InlineKeyboardButton("🟢 Unban User ID", callback_data="own_unban")],
        [InlineKeyboardButton("📋 View All Approved, Banned & DB Users", callback_data="own_list")],
        [InlineKeyboardButton("🔄 Restart Bot Server", callback_data="own_restart"),
         InlineKeyboardButton("🏠 Back to Main Menu", callback_data="menu_main")]
    ])


# ==================== /START HANDLER ====================
@bot.on_message(filters.command("start"))
async def start_cmd(client: Client, message: Message):
    if not is_authorized(None, None, message): return await handle_unauthorized_user(client, message)
    uid = message.from_user.id
    await MANAGER.get_ws(uid)
    await message.reply_text(
        START_UI.format(mention=message.from_user.mention),
        reply_markup=main_menu_kb(uid)
    )

# ==================== OWNER APPROVAL BUTTONS ====================
@bot.on_callback_query(filters.regex(r"^req_(app|ban)_(\d+)$"))
async def handle_access_request_buttons(client: Client, query: CallbackQuery):
    if query.from_user.id != config.OWNER_ID:
        return await query.answer("❌ Only the Bot Owner can do this!", show_alert=True)
    action, target_str = query.data.split("_")[1], query.data.split("_")[2]
    target_uid = int(target_str)

    if action == "app":
        MANAGER.banned_users.discard(target_uid)
        MANAGER.approved_users.add(target_uid)
        MANAGER.notified_pending.discard(target_uid)
        await MANAGER.save_access_to_mongo()
        try: await query.message.edit_text(f"{query.message.text}\n\n✅ **STATUS: APPROVED BY OWNER**")
        except MessageNotModified: pass
        try:
            await client.send_message(target_uid, "🎉 **ACCESS APPROVED!**\nYou now have full access.", reply_markup=main_menu_kb(target_uid))
        except Exception: pass
    else:
        MANAGER.approved_users.discard(target_uid)
        MANAGER.banned_users.add(target_uid)
        MANAGER.notified_pending.discard(target_uid)
        await MANAGER.save_access_to_mongo()
        try: await query.message.edit_text(f"{query.message.text}\n\n🚫 **STATUS: BANNED BY OWNER**")
        except MessageNotModified: pass


# ==================== SUB-MENU ROUTER ====================
@bot.on_callback_query(filters.regex(r"^menu_"))
async def sub_menu_router(client: Client, query: CallbackQuery):
    if not is_authorized(None, None, query): return await query.answer("⛔ Access Restricted!", show_alert=True)
    uid = query.from_user.id
    ws = await MANAGER.get_ws(uid)
    menu = query.data

    try:
        if menu == "menu_main":
            await query.message.edit_text(START_UI.format(mention=query.from_user.mention), reply_markup=main_menu_kb(uid))
        elif menu == "menu_audio":
            status = "🟢 Live" if ws.recording else "🔴 Stopped"
            await query.message.edit_text(
                f"🎵 **Audio Controls Sub-Menu**\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📡 **Source Chat ID:** `{ws.source_chat or 'Not Set'}`\n"
                f"🔗 **Active Targets:** `{len(ws.targets)}/{len(ws.broadcaster_users)}`\n"
                f"🔇 **Muted:** `{ws.muted}` | 📊 **Status:** {status}\n━━━━━━━━━━━━━━━━━━━━",
                reply_markup=audio_menu_kb(ws)
            )
        elif menu == "menu_fx":
            await query.message.edit_text("🎛️ **DSP Audio Effects Sub-Menu**\n━━━━━━━━━━━━━━━━━━━━", reply_markup=fx_menu_kb(ws))
        elif menu == "menu_acc":
            rec_s = "✅ Active" if ws.recorder_user else ("⚠️ Saved in DB (Unloaded)" if ws.recorder_session else "❌ Not Set")
            await query.message.edit_text(
                f"📱 **My Accounts (MongoDB Saved)**\n━━━━━━━━━━━━━━━━━━━━\n"
                f"🎙️ **Recorder Account:** `{rec_s}`\n"
                f"📡 **Broadcaster Accounts:** `{len(ws.broadcaster_users)} active`\n━━━━━━━━━━━━━━━━━━━━",
                reply_markup=acc_menu_kb()
            )
        elif menu == "menu_util":
            await query.message.edit_text("📊 **Utility & System Status Sub-Menu**\n━━━━━━━━━━━━━━━━━━━━", reply_markup=util_menu_kb())
        elif menu == "menu_help":
            await query.message.edit_text("📖 **Complete Command Guide**\n━━━━━━━━━━━━━━━━━━━━\nEvery feature works via Buttons and Commands.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Back Home", callback_data="menu_main")]]))
        elif menu == "menu_owner":
            if uid != config.OWNER_ID: return await query.answer("❌ Owner Only!", show_alert=True)
            await query.message.edit_text(
                f"👑 **Master Owner Control Panel (MongoDB)**\n━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ **Approved Users:** `{len(MANAGER.approved_users)}`\n"
                f"🚫 **Banned Users:** `{len(MANAGER.banned_users)}`",
                reply_markup=owner_menu_kb()
            )
    except MessageNotModified: await query.answer()


# ==================== AUDIO ACTIONS ====================
@bot.on_callback_query(filters.regex(r"^act_"))
async def audio_actions_callbacks(client: Client, query: CallbackQuery):
    if not is_authorized(None, None, query): return await query.answer("⛔ Access Restricted!", show_alert=True)
    uid = query.from_user.id
    chat_id = query.message.chat.id
    ws = await MANAGER.get_ws(uid)
    act = query.data

    if act == "act_setsource":
        await query.answer()
        ans = await client.ask(chat_id, "🎯 **Send Source Chat ID (e.g. `-1004426372558`):**", timeout=60)
        if not ans.text.startswith("/cancel"):
            ws.source_chat = int(ans.text.strip())
            await ws.save_to_mongo()
            await ans.reply_text(f"✅ **Source Chat saved:** `{ws.source_chat}`", reply_markup=audio_menu_kb(ws))

    elif act == "act_record":
        if not ws.recorder_call or not ws.source_chat: return await query.answer("❌ Configure Recorder & Source Chat first!", show_alert=True)
        await query.answer()
        p_msg = await query.message.edit_text("⚙️ Initializing recording stream...")
        await show_progress_bar(p_msg, "Starting Audio Recorder")
        await ws.start_bridge()
        await ws.recorder_call.record(ws.source_chat, RecordStream(ws.fifo_path, AudioParameters(bitrate=48000, channels=2)))
        ws.recording = True
        try: await p_msg.edit_text(f"✅ **Recording active on `{ws.source_chat}`!**", reply_markup=audio_menu_kb(ws))
        except MessageNotModified: pass

    elif act == "act_join":
        await query.answer()
        ans = await client.ask(chat_id, "➕ **Send Target Chat ID:**", timeout=60)
        if not ans.text.startswith("/cancel"):
            t_id = int(ans.text.strip())
            b_idx = ws.targets.get(t_id, ws.get_free_broadcaster())
            if b_idx is None: return await ans.reply_text("❌ **No free Broadcaster accounts!**", reply_markup=acc_menu_kb())
            p_msg = await ans.reply_text("⚙️ Connecting broadcaster to target voice chat...")
            await show_progress_bar(p_msg, "Connecting Audio Stream")
            await ws.broadcaster_calls[b_idx].play(t_id, ws.get_target_stream())
            ws.targets[t_id] = b_idx
            try: await p_msg.edit_text(f"✅ **Connected & streaming to `{t_id}`!**", reply_markup=audio_menu_kb(ws))
            except MessageNotModified: pass

    elif act == "act_leaveall":
        p_msg = await query.message.edit_text("⚙️ Disconnecting all streams...")
        await show_progress_bar(p_msg, "Shutting Down Streams")
        cnt = await ws.leave_all()
        try: await p_msg.edit_text(f"✅ **Disconnected all ({cnt}) streams!**", reply_markup=audio_menu_kb(ws))
        except MessageNotModified: pass

    elif act == "act_mute":
        ws.muted = True
        for tid, idx in ws.targets.items(): await ws.broadcaster_calls[idx].mute_stream(tid)
        await query.answer("🔇 Muted all streams!", show_alert=True)
    elif act == "act_unmute":
        ws.muted = False
        for tid, idx in ws.targets.items(): await ws.broadcaster_calls[idx].unmute_stream(tid)
        await query.answer("🔊 Unmuted all streams!", show_alert=True)
    elif act == "act_rejoin":
        await ws.reload_streams()
        await query.answer("🔄 Reconnected streams!", show_alert=True)


# ==================== DSP EFFECTS ACTIONS ====================
@bot.on_callback_query(filters.regex(r"^fx_"))
async def fx_actions_callbacks(client: Client, query: CallbackQuery):
    if not is_authorized(None, None, query): return await query.answer("⛔ Access Restricted!", show_alert=True)
    uid = query.from_user.id
    ws = await MANAGER.get_ws(uid)
    cmd = query.data.replace("fx_", "")
    try:
        if cmd == "show":
            await query.message.edit_text(f"🎛️ **Active Effects:** `{ws.effects}`", reply_markup=fx_menu_kb(ws))
        elif cmd == "reset":
            ws.effects = {"level": 1.0, "bass": 0.0, "treble": 0.0, "gain": 0.0, "overdrive": 0.0, "echo": None}
            await ws.save_to_mongo()
            await ws.reload_streams()
            await query.answer("🔄 Reset all effects!", show_alert=True)
        elif cmd in ("level", "bass", "treble", "gain", "overdrive"):
            await query.answer()
            ans = await client.ask(query.message.chat.id, f"🎚️ **Send new value for `{cmd.upper()}`:**", timeout=60)
            ws.effects[cmd] = float(ans.text.strip())
            await ws.save_to_mongo()
            await ws.reload_streams()
            await ans.reply_text(f"✅ **{cmd.capitalize()} updated!**", reply_markup=fx_menu_kb(ws))
    except MessageNotModified: await query.answer()


# ==================== MY ACCOUNTS (DB) ACTIONS ====================
@bot.on_callback_query(filters.regex(r"^acc_"))
async def acc_actions_callbacks(client: Client, query: CallbackQuery):
    if not is_authorized(None, None, query): return await query.answer("⛔ Access Restricted!", show_alert=True)
    uid = query.from_user.id
    chat_id = query.message.chat.id
    ws = await MANAGER.get_ws(uid)
    act = query.data

    if act == "acc_otp":
        await query.answer()
        try:
            r_q = await client.ask(chat_id, "📱 Reply `1` for Recorder, `2` for Broadcaster:", timeout=60)
            role = "rec" if r_q.text.strip() == "1" else "bcast"
            p_q = await client.ask(chat_id, "📞 Send Phone Number (e.g. `+919876543210`):", timeout=120)
            
            p_msg = await p_q.reply_text("⚙️ Requesting OTP code from Telegram...")
            tmp = Client(f"otp_{uid}_{int(time.time())}", api_id=config.API_ID, api_hash=config.API_HASH, in_memory=True)
            await tmp.connect()
            sent = await tmp.send_code(p_q.text.strip())

            otp_q = await client.ask(chat_id, "📩 Enter 5-digit Telegram OTP with spaces:", timeout=120)
            otp = otp_q.text.replace(" ", "").strip()
            try: await tmp.sign_in(p_q.text.strip(), sent.phone_code_hash, otp)
            except SessionPasswordNeeded:
                pwd = await client.ask(chat_id, "🔐 Enter 2FA Password:", timeout=120)
                await tmp.check_password(pwd.text.strip())

            s_str = await tmp.export_session_string()
            me = await tmp.get_me()
            await tmp.disconnect()

            if role == "rec":
                ws.recorder_session = s_str
                await ws.set_recorder(s_str, save=True)
            else:
                if s_str not in ws.broadcaster_sessions:
                    ws.broadcaster_sessions.append(s_str)
                await ws.add_broadcaster(s_str, save=True)

            logger.info(f"User {uid} logged in account {me.first_name} via OTP")
            await client.send_message(chat_id, f"✅ **Account Connected!** ({me.first_name})", reply_markup=acc_menu_kb())
        except Exception as e:
            logger.error(f"OTP login failure for {uid}: {e}")
            await client.send_message(chat_id, f"❌ **Error:** `{e}`", reply_markup=acc_menu_kb())

    elif act in ("acc_rec_str", "acc_bcast_str"):
        await query.answer()
        label = "Recorder" if act == "acc_rec_str" else "Broadcaster"
        ans = await client.ask(chat_id, f"🔑 **Send String Session for {label}:**", timeout=120)
        if not ans.text.startswith("/cancel"):
            s_str = ans.text.strip()
            if act == "acc_rec_str": await ws.set_recorder(s_str, save=True)
            else: await ws.add_broadcaster(s_str, save=True)
            await ans.reply_text(f"✅ **{label} Connected!**", reply_markup=acc_menu_kb())

    elif act == "acc_view":
        rec_status = "✅ Active" if ws.recorder_user else ("⚠️ Saved in DB (Unloaded)" if ws.recorder_session else "❌ Not Set")
        b_list = []
        for idx, u in enumerate(ws.broadcaster_users):
            try:
                bme = await u.get_me()
                b_list.append(f"  `{idx+1}.` {bme.first_name} (`{bme.id}`)")
            except Exception: b_list.append(f"  `{idx+1}.` Disconnected")
        b_str = "\n".join(b_list) or f"  📡 {len(ws.broadcaster_sessions)} Saved in DB"

        try:
            await query.message.edit_text(
                f"📋 **Your Saved Accounts (DB & Memory)**\n━━━━━━━━━━━━━━━━━━━━\n"
                f"🎙️ **Recorder Account:** {rec_status}\n\n"
                f"📡 **Broadcasters:**\n{b_str}\n━━━━━━━━━━━━━━━━━━━━",
                reply_markup=acc_menu_kb()
            )
        except MessageNotModified: await query.answer("List is up to date!", show_alert=False)

    elif act == "acc_del":
        await query.answer()
        ans = await client.ask(chat_id, "🗑️ Send `0` to delete Recorder, or `1, 2...` for Broadcaster:", timeout=60)
        try:
            num = int(ans.text.strip())
            if num == 0:
                if ws.recorder_user: await ws.recorder_user.stop()
                ws.recorder_user = None; ws.recorder_call = None; ws.recorder_session = ""
                await ws.save_to_mongo()
                await ans.reply_text("✅ **Recorder deleted!**", reply_markup=acc_menu_kb())
            else:
                idx = num - 1
                u_cli = ws.broadcaster_users.pop(idx)
                ws.broadcaster_calls.pop(idx)
                ws.broadcaster_sessions.pop(idx)
                await ws.save_to_mongo()
                await u_cli.stop()
                await ans.reply_text(f"✅ **Broadcaster #{num} deleted!**", reply_markup=acc_menu_kb())
        except Exception as e: await ans.reply_text(f"❌ **Error:** `{e}`", reply_markup=acc_menu_kb())


# ==================== UTILITY & OWNER ACTIONS ====================
@bot.on_callback_query(filters.regex(r"^util_"))
async def util_actions(client: Client, query: CallbackQuery):
    if not is_authorized(None, None, query): return await query.answer("⛔ Access Restricted!", show_alert=True)
    if query.data == "util_ping":
        t0 = time.perf_counter()
        await query.answer(f"🏓 Pong! {(time.perf_counter()-t0)*1000:.2f}ms", show_alert=True)

@bot.on_callback_query(filters.regex(r"^own_"))
async def owner_actions(client: Client, query: CallbackQuery):
    if query.from_user.id != config.OWNER_ID: return await query.answer("❌ Owner Only!", show_alert=True)
    if query.data == "own_restart":
        try: await query.message.edit_text("🔄 Restarting bot server...")
        except MessageNotModified: pass
        for ws in MANAGER.workspaces.values(): await ws.leave_all()
        os.execv(sys.executable, [sys.executable] + sys.argv)


# ==================== BOOTSTRAP ====================
async def bootstrap():
    logger.info("Starting Nobi Fight Bot with MongoDB & Anti-Crash Patch...")
    await bot.start()
    await MANAGER.load_from_mongo()
    logger.info("Bot & MongoDB Workspaces are LIVE!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(bootstrap())
