cat << 'EOF' > /opt/Tg-Vc-forward/main.py
import os
import sys
import time
import asyncio
import subprocess
import psutil
from typing import Dict, Set, List, Optional

import pyromod
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from pyrogram.errors import SessionPasswordNeeded
from pytgcalls import PyTgCalls
from pytgcalls.types import MediaStream, AudioQuality, RecordStream
from pytgcalls.types.raw import AudioParameters

import config

START_TIME = time.time()

# ==================== MONGODB INITIALIZATION ====================
mongo_client = AsyncIOMotorClient(config.MONGO_URI)
db = mongo_client[config.DB_NAME]
users_col = db["users"]       # Stores per-user accounts, source_chat, effects
access_col = db["access"]     # Stores approved_users, banned_users, pending_requests

bot = Client(
    "NobiFightBot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
)


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
            try:
                os.remove(self.fifo_path)
            except Exception:
                pass
        os.mkfifo(self.fifo_path)

        try:
            subprocess.run(
                [
                    "pactl",
                    "load-module",
                    "module-null-sink",
                    f"sink_name={self.sink_name}",
                ],
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
        await users_col.update_one(
            {"user_id": self.user_id}, {"$set": doc}, upsert=True
        )

    def build_ffmpeg_filters(self) -> str:
        fx = self.effects
        af = []
        if fx["level"] != 1.0:
            af.append(f"volume={fx['level']}")
        if fx["gain"] != 0.0:
            af.append(f"volume={fx['gain']}dB")
        if fx["bass"] != 0.0:
            af.append(f"bass=g={fx['bass']}:f=110:w=0.6")
        if fx["treble"] != 0.0:
            af.append(f"treble=g={fx['treble']}:f=3200:w=0.5")
        if fx["overdrive"] > 0:
            d = min(max(fx["overdrive"], 1.0), 64.0)
            af.append(f"acrusher=level_in={d}:level_out={d*1.5}:bits=8:mode=log:aa=1")
        if fx["echo"]:
            delay, decay = fx["echo"]
            af.append(f"aecho=0.8:0.9:{delay}:{decay}")
        return f"-af {','.join(af)}" if af else ""

    def get_target_stream(self) -> MediaStream:
        af_str = self.build_ffmpeg_filters()
        ffmpeg_params = f"-f pulse {af_str}".strip()
        return MediaStream(
            f"{self.sink_name}.monitor",
            audio_parameters=AudioQuality.STUDIO,
            ffmpeg_parameters=ffmpeg_params,
        )

    async def start_bridge(self):
        await self.stop_bridge()
        cmd = [
            "ffmpeg", "-y", "-f", "s16le", "-ac", "2", "-ar", "48000",
            "-i", self.fifo_path, "-f", "pulse", self.sink_name,
        ]
        self.ffmpeg_proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
        )

    async def stop_bridge(self):
        if self.ffmpeg_proc:
            try:
                self.ffmpeg_proc.terminate()
                await self.ffmpeg_proc.wait()
            except Exception:
                pass
            self.ffmpeg_proc = None

    async def set_recorder(self, session_str: str, save: bool = True) -> bool:
        if not session_str:
            return False
        try:
            if self.recorder_user:
                try:
                    await self.recorder_user.stop()
                except Exception:
                    pass
            u_cli = Client(
                f"Rec_{self.user_id}_{int(time.time())}",
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                session_string=session_str,
                in_memory=True,
                no_updates=True,
            )
            c_cli = PyTgCalls(u_cli)
            await u_cli.start()
            await c_cli.start()
            self.recorder_user = u_cli
            self.recorder_call = c_cli
            self.recorder_session = session_str
            if save:
                await self.save_to_mongo()
            return True
        except Exception as e:
            print(f"[ERROR] User {self.user_id} Recorder error: {e}")
            return False

    async def add_broadcaster(self, session_str: str, save: bool = True) -> bool:
        if not session_str:
            return False
        try:
            idx = len(self.broadcaster_users) + 1
            u_cli = Client(
                f"Bcast_{self.user_id}_{idx}_{int(time.time())}",
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                session_string=session_str,
                in_memory=True,
                no_updates=True,
            )
            c_cli = PyTgCalls(u_cli)
            await u_cli.start()
            await c_cli.start()
            self.broadcaster_users.append(u_cli)
            self.broadcaster_calls.append(c_cli)
            if save and session_str not in self.broadcaster_sessions:
                self.broadcaster_sessions.append(session_str)
                await self.save_to_mongo()
            return True
        except Exception as e:
            print(f"[ERROR] User {self.user_id} Broadcaster error: {e}")
            return False

    def get_free_broadcaster(self) -> Optional[int]:
        used = set(self.targets.values())
        for i in range(len(self.broadcaster_calls)):
            if i not in used:
                return i
        return None

    async def reload_streams(self):
        if not self.targets:
            return
        stream = self.get_target_stream()
        for t_id, b_idx in list(self.targets.items()):
            try:
                await self.broadcaster_calls[b_idx].play(t_id, stream)
                if self.muted:
                    await self.broadcaster_calls[b_idx].mute_stream(t_id)
            except Exception as e:
                print(f"[WARN] Reload failed on {t_id}: {e}")

    async def leave_all(self) -> int:
        count = len(self.targets)
        for t_id, b_idx in list(self.targets.items()):
            try:
                await self.broadcaster_calls[b_idx].leave_call(t_id)
            except Exception:
                pass
        self.targets.clear()

        if self.source_chat and self.recorder_call:
            try:
                await self.recorder_call.leave_call(self.source_chat)
            except Exception:
                pass
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
            if uid in self.banned_users:
                continue
            ws = UserWorkspace(uid, u_doc)
            self.workspaces[uid] = ws
            if ws.recorder_session:
                print(f"🎙️ Restoring MongoDB Recorder for {uid}...")
                await ws.set_recorder(ws.recorder_session, save=False)
            for b_str in list(ws.broadcaster_sessions):
                print(f"📡 Restoring MongoDB Broadcaster for {uid}...")
                await ws.add_broadcaster(b_str, save=False)

    async def save_access_to_mongo(self):
        await access_col.update_one(
            {"_id": "global_access"},
            {
                "$set": {
                    "approved_users": list(self.approved_users),
                    "banned_users": list(self.banned_users),
                }
            },
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
    if not user:
        return False
    if user.id in MANAGER.banned_users:
        return False
    return user.id == config.OWNER_ID or user.id in MANAGER.approved_users

auth_filter = filters.create(is_authorized)


async def handle_unauthorized_user(client: Client, message: Message):
    user = message.from_user
    if not user:
        return
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

    # Forward interactive approval request to OWNER DM
    if user.id not in MANAGER.notified_pending:
        MANAGER.notified_pending.add(user.id)
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Approve Access", callback_data=f"req_app_{user.id}"),
                InlineKeyboardButton("🚫 Ban User", callback_data=f"req_ban_{user.id}"),
            ]
        ])
        try:
            await client.send_message(
                config.OWNER_ID,
                f"📩 **NEW ACCESS REQUEST**\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 **User:** {user.mention} ({user_tag})\n"
                f"🆔 **User ID:** `{user.id}`\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"Click below to grant or deny access:",
                reply_markup=kb,
            )
        except Exception as e:
            print(f"[WARN] Could not notify owner: {e}")


# ==================== UI TEXTS & ALL SUB-MENU KEYBOARDS ====================
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
• Bass/Treble/Gain/Overdrive/Echo
━━━━━━━━━━━━━━━━━━━━
👇 **Select a menu below to control everything:**"""


def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("🎵 Audio Controls", callback_data="menu_audio"),
            InlineKeyboardButton("🎛️ Audio Effects", callback_data="menu_fx"),
        ],
        [
            InlineKeyboardButton("📱 My Accounts (DB)", callback_data="menu_acc"),
            InlineKeyboardButton("📊 Utility & Status", callback_data="menu_util"),
        ],
        [
            InlineKeyboardButton("👤 Owner", url=f"tg://user?id={config.OWNER_ID}"),
            InlineKeyboardButton("📖 Help Guide", callback_data="menu_help"),
        ],
    ]
    if user_id == config.OWNER_ID:
        rows.append(
            [InlineKeyboardButton("👑 Owner Panel (Approve / Ban / DB)", callback_data="menu_owner")]
        )
    return InlineKeyboardMarkup(rows)


def audio_menu_kb(ws: UserWorkspace) -> InlineKeyboardMarkup:
    rec_lbl = "🟢 Recording (Restart)" if ws.recording else "🎙️ Start Recording (/record)"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(rec_lbl, callback_data="act_record"),
            InlineKeyboardButton("🎯 Set Source Chat", callback_data="act_setsource"),
        ],
        [
            InlineKeyboardButton("➕ Join Target VC (/join)", callback_data="act_join"),
            InlineKeyboardButton("➖ Leave Target VC (/leave)", callback_data="act_leave"),
        ],
        [
            InlineKeyboardButton("🔇 Mute All (/mute)", callback_data="act_mute"),
            InlineKeyboardButton("🔊 Unmute All (/unmute)", callback_data="act_unmute"),
        ],
        [
            InlineKeyboardButton("🔄 Rejoin All (/rejoin)", callback_data="act_rejoin"),
            InlineKeyboardButton("⏹️ Leave Source (/leaverecord)", callback_data="act_leaverec"),
        ],
        [
            InlineKeyboardButton("🛑 Disconnect All (/leaveall)", callback_data="act_leaveall"),
        ],
        [
            InlineKeyboardButton("🏠 Back to Main Menu", callback_data="menu_main"),
        ],
    ])


def fx_menu_kb(ws: UserWorkspace) -> InlineKeyboardMarkup:
    fx = ws.effects
    echo_lbl = f"{fx['echo'][0]}ms" if fx["echo"] else "Off"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"🔊 Level ({fx['level']})", callback_data="fx_level"),
            InlineKeyboardButton(f"🎸 Bass ({fx['bass']}dB)", callback_data="fx_bass"),
        ],
        [
            InlineKeyboardButton(f"🎻 Treble ({fx['treble']}dB)", callback_data="fx_treble"),
            InlineKeyboardButton(f"⚡ Gain ({fx['gain']}dB)", callback_data="fx_gain"),
        ],
        [
            InlineKeyboardButton(f"🔥 Overdrive ({fx['overdrive']})", callback_data="fx_overdrive"),
            InlineKeyboardButton(f"🗣️ Echo ({echo_lbl})", callback_data="fx_echo"),
        ],
        [
            InlineKeyboardButton("📋 Show Active Effects", callback_data="fx_show"),
            InlineKeyboardButton("🔄 Reset All Effects", callback_data="fx_reset"),
        ],
        [
            InlineKeyboardButton("🏠 Back to Main Menu", callback_data="menu_main"),
        ],
    ])


def acc_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📱 Login Account via Phone (OTP & 2FA)", callback_data="acc_otp"),
        ],
        [
            InlineKeyboardButton("🎙️ Set Recorder (String)", callback_data="acc_rec_str"),
            InlineKeyboardButton("➕ Add Broadcaster (String)", callback_data="acc_bcast_str"),
        ],
        [
            InlineKeyboardButton("📋 View My Saved DB Accounts", callback_data="acc_view"),
            InlineKeyboardButton("🗑️ Delete Account from DB", callback_data="acc_del"),
        ],
        [
            InlineKeyboardButton("🏠 Back to Main Menu", callback_data="menu_main"),
        ],
    ])


def util_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🏓 Ping Latency", callback_data="util_ping"),
            InlineKeyboardButton("📊 System & Bot Stats", callback_data="util_stats"),
        ],
        [
            InlineKeyboardButton("🔗 Active Forward List", callback_data="util_list"),
            InlineKeyboardButton("🆔 My ID & Info", callback_data="util_id"),
        ],
        [
            InlineKeyboardButton("🏠 Back to Main Menu", callback_data="menu_main"),
        ],
    ])


def owner_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve User ID", callback_data="own_approve"),
            InlineKeyboardButton("❌ Remove Access", callback_data="own_disapprove"),
        ],
        [
            InlineKeyboardButton("🚫 Ban User ID", callback_data="own_ban"),
            InlineKeyboardButton("🟢 Unban User ID", callback_data="own_unban"),
        ],
        [
            InlineKeyboardButton("📋 View All Approved, Banned & DB Users", callback_data="own_list"),
        ],
        [
            InlineKeyboardButton("🔄 Restart Bot Server", callback_data="own_restart"),
            InlineKeyboardButton("🏠 Back to Main Menu", callback_data="menu_main"),
        ],
    ])


# ==================== /START & UNAUTHORIZED INTERCEPTOR ====================
@bot.on_message(filters.command("start"))
async def start_cmd(client: Client, message: Message):
    if not is_authorized(None, None, message):
        return await handle_unauthorized_user(client, message)

    uid = message.from_user.id
    await MANAGER.get_ws(uid)
    await message.reply_text(
        START_UI.format(mention=message.from_user.mention),
        reply_markup=main_menu_kb(uid),
    )


# ==================== INSTANT OWNER APPROVAL / BAN BUTTONS ====================
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

        await query.message.edit_text(
            f"{query.message.text}\n\n✅ **STATUS: APPROVED BY OWNER**"
        )
        try:
            await client.send_message(
                target_uid,
                "🎉 **ACCESS APPROVED!**\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "✅ An admin has approved your request!\n"
                "You now have full access to the bot and all menus below:",
                reply_markup=main_menu_kb(target_uid),
            )
        except Exception:
            pass
    else:
        MANAGER.approved_users.discard(target_uid)
        MANAGER.banned_users.add(target_uid)
        MANAGER.notified_pending.discard(target_uid)
        await MANAGER.save_access_to_mongo()

        await query.message.edit_text(
            f"{query.message.text}\n\n🚫 **STATUS: BANNED BY OWNER**"
        )
        try:
            await client.send_message(
                target_uid,
                "🚫 **Your access request was denied and your ID has been banned.**",
            )
        except Exception:
            pass


# ==================== SUB-MENU NAVIGATION CALLBACKS ====================
@bot.on_callback_query(filters.regex(r"^menu_"))
async def sub_menu_router(client: Client, query: CallbackQuery):
    if not is_authorized(None, None, query):
        return await query.answer("⛔ Access Restricted! Wait for admin approval.", show_alert=True)

    uid = query.from_user.id
    ws = await MANAGER.get_ws(uid)
    menu = query.data

    if menu == "menu_main":
        await query.message.edit_text(
            START_UI.format(mention=query.from_user.mention),
            reply_markup=main_menu_kb(uid),
        )
    elif menu == "menu_audio":
        status = "🟢 Live" if ws.recording else "🔴 Stopped"
        await query.message.edit_text(
            f"🎵 **Audio Controls Sub-Menu**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📡 **Source Chat ID:** `{ws.source_chat or 'Not Set'}`\n"
            f"🔗 **Active Targets:** `{len(ws.targets)}/{len(ws.broadcaster_users)}`\n"
            f"🔇 **Muted:** `{ws.muted}` | 📊 **Status:** {status}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Click any button below to execute:",
            reply_markup=audio_menu_kb(ws),
        )
    elif menu == "menu_fx":
        await query.message.edit_text(
            "🎛️ **DSP Audio Effects Sub-Menu (No Limits!)**\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Click any effect below to adjust its value in real time:",
            reply_markup=fx_menu_kb(ws),
        )
    elif menu == "menu_acc":
        rec_s = "✅ Saved in DB" if ws.recorder_user else "❌ Not Set"
        await query.message.edit_text(
            f"📱 **My Accounts (MongoDB Saved)**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔒 *Your accounts are saved permanently in MongoDB and isolated to your ID.*\n\n"
            f"🎙️ **Recorder Account:** `{rec_s}`\n"
            f"📡 **Broadcaster Accounts:** `{len(ws.broadcaster_users)} active`\n"
            f"━━━━━━━━━━━━━━━━━━━━",
            reply_markup=acc_menu_kb(),
        )
    elif menu == "menu_util":
        await query.message.edit_text(
            "📊 **Utility & System Status Sub-Menu**\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Select a utility command below:",
            reply_markup=util_menu_kb(),
        )
    elif menu == "menu_help":
        await query.message.edit_text(
            "📖 **Complete Command & Button Guide**\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Every feature works both via **Buttons** and **Commands**:\n\n"
            "🎵 **Audio:** `/record`, `/setrecordgroup <id>`, `/join <id>`, `/leave <id>`, `/rejoin`, `/leaverecord`, `/mute`, `/unmute`, `/leaveall`\n\n"
            "🎛️ **Effects:** `/level`, `/bass`, `/treble`, `/gain`, `/overdrive`, `/echo`, `/effects`, `/reset`\n\n"
            "📊 **Utility:** `/ping`, `/stats`, `/status`, `/list`, `/id`, `/panel`\n\n"
            "👑 **Owner:** `/owner`, `/approve <id>`, `/disapprove <id>`, `/ban <id>`, `/unban <id>`, `/userlist`, `/restart`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Back Home", callback_data="menu_main")]]),
        )
    elif menu == "menu_owner":
        if uid != config.OWNER_ID:
            return await query.answer("❌ Owner Only!", show_alert=True)
        await query.message.edit_text(
            f"👑 **Master Owner Control Panel (MongoDB)**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ **Approved Users:** `{len(MANAGER.approved_users)}`\n"
            f"🚫 **Banned Users:** `{len(MANAGER.banned_users)}`\n"
            f"👥 **Loaded User Workspaces:** `{len(MANAGER.workspaces)}`\n"
            f"━━━━━━━━━━━━━━━━━━━━",
            reply_markup=owner_menu_kb(),
        )


# ==================== AUDIO CONTROLS SUB-MENU ACTIONS ====================
@bot.on_callback_query(filters.regex(r"^act_"))
async def audio_actions_callbacks(client: Client, query: CallbackQuery):
    if not is_authorized(None, None, query):
        return await query.answer("⛔ Access Restricted!", show_alert=True)

    uid = query.from_user.id
    chat_id = query.message.chat.id
    ws = await MANAGER.get_ws(uid)
    act = query.data

    if act == "act_setsource":
        await query.answer()
        try:
            ans = await client.ask(
                chat_id,
                "🎯 **Send the Source Group Chat ID (e.g. `-1004426372558`) where your Recorder Account will listen:**\n"
                "*(Or send `/cancel`)*",
                timeout=60,
            )
            if ans.text.startswith("/cancel"):
                return await ans.reply_text("🚫 **Cancelled!**", reply_markup=audio_menu_kb(ws))
            ws.source_chat = int(ans.text.strip())
            await ws.save_to_mongo()
            await ans.reply_text(
                f"✅ **Source Chat saved to MongoDB:** `{ws.source_chat}`",
                reply_markup=audio_menu_kb(ws),
            )
        except Exception as e:
            await client.send_message(chat_id, f"❌ **Invalid Chat ID:** `{e}`", reply_markup=audio_menu_kb(ws))

    elif act == "act_record":
        if not ws.recorder_call:
            return await query.answer("❌ Add a Recorder Account in '📱 My Accounts' first!", show_alert=True)
        if not ws.source_chat:
            return await query.answer("❌ Click '🎯 Set Source Chat' first!", show_alert=True)
        await query.answer("🎙️ Starting recording...")
        try:
            await ws.start_bridge()
            await ws.recorder_call.record(
                ws.source_chat,
                RecordStream(ws.fifo_path, AudioParameters(bitrate=48000, channels=2)),
            )
            ws.recording = True
            await query.message.edit_text(
                f"✅ **Recording started successfully!**\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📡 **Source:** `{ws.source_chat}`\n"
                f"🔗 **Forwarding:** {len(ws.targets)} chats\n"
                f"📊 **Status:** 🟢 Live\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🎵 **Audio is now flowing!**",
                reply_markup=audio_menu_kb(ws),
            )
        except Exception as e:
            await query.message.edit_text(f"❌ **Record Error:** `{e}`", reply_markup=audio_menu_kb(ws))

    elif act == "act_join":
        await query.answer()
        try:
            ans = await client.ask(
                chat_id,
                "➕ **Send the Target Group Chat ID to forward audio into (e.g. `-1003665014605`):**\n"
                "*(Or send `/cancel`)*",
                timeout=60,
            )
            if ans.text.startswith("/cancel"):
                return await ans.reply_text("🚫 **Cancelled!**", reply_markup=audio_menu_kb(ws))
            t_id = int(ans.text.strip())

            b_idx = ws.targets.get(t_id, ws.get_free_broadcaster())
            if b_idx is None:
                return await ans.reply_text(
                    "❌ **No free Broadcaster Account available! Add one in `📱 My Accounts`.**",
                    reply_markup=acc_menu_kb(),
                )
            await ws.broadcaster_calls[b_idx].play(t_id, ws.get_target_stream())
            ws.targets[t_id] = b_idx
            await ans.reply_text(
                f"✅ **Forwarding started successfully!**\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 **Target:** `{t_id}`\n"
                f"🔗 **Total connections:** {len(ws.targets)}\n"
                f"📊 **Status:** 🟢 Connected\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🎵 **Audio is now flowing to this chat!**",
                reply_markup=audio_menu_kb(ws),
            )
        except Exception as e:
            await client.send_message(chat_id, f"❌ **Join Failed:** `{e}`", reply_markup=audio_menu_kb(ws))

    elif act == "act_leave":
        if not ws.targets:
            return await query.answer("📭 No active target chats to leave!", show_alert=True)
        await query.answer()
        t_list = "\n".join([f"• `{tid}`" for tid in ws.targets])
        ans = await client.ask(
            chat_id,
            f"➖ **Send Target Chat ID to disconnect:**\n{t_list}\n*(Or `/cancel`)*",
            timeout=60,
        )
        if not ans.text.startswith("/cancel"):
            try:
                tid = int(ans.text.strip())
                if tid in ws.targets:
                    b_idx = ws.targets.pop(tid)
                    await ws.broadcaster_calls[b_idx].leave_call(tid)
                    await ans.reply_text(f"✅ **Left `{tid}`!**", reply_markup=audio_menu_kb(ws))
            except Exception as e:
                await ans.reply_text(f"❌ **Error:** `{e}`", reply_markup=audio_menu_kb(ws))

    elif act == "act_mute":
        ws.muted = True
        for tid, idx in ws.targets.items():
            try:
                await ws.broadcaster_calls[idx].mute_stream(tid)
            except Exception:
                pass
        await query.answer("🔇 All your streams are now Muted!", show_alert=True)
        await sub_menu_router(client, query)

    elif act == "act_unmute":
        ws.muted = False
        for tid, idx in ws.targets.items():
            try:
                await ws.broadcaster_calls[idx].unmute_stream(tid)
            except Exception:
                pass
        await query.answer("🔊 All your streams are now Unmuted!", show_alert=True)

    elif act == "act_rejoin":
        if ws.source_chat and ws.recording and ws.recorder_call:
            try:
                await ws.start_bridge()
                await ws.recorder_call.record(
                    ws.source_chat,
                    RecordStream(ws.fifo_path, AudioParameters(bitrate=48000, channels=2)),
                )
            except Exception:
                pass
        await ws.reload_streams()
        await query.answer("🔄 Reconnected all active streams!", show_alert=True)

    elif act == "act_leaverec":
        if ws.source_chat and ws.recorder_call:
            try:
                await ws.recorder_call.leave_call(ws.source_chat)
            except Exception:
                pass
        await ws.stop_bridge()
        ws.recording = False
        await query.answer("⏹️ Left Source Recording VC!", show_alert=True)
        await query.message.edit_text("⏹️ **Recorder left the Source VC.**", reply_markup=audio_menu_kb(ws))

    elif act == "act_leaveall":
        left_cnt = await ws.leave_all()
        await query.message.edit_text(
            f"✅ **All chats disconnected successfully!**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📡 **Source:** ✅ Left\n"
            f"🔗 **Forwarding:** {left_cnt}/{left_cnt} left\n"
            f"📊 **Status:** 🔴 All disconnected\n"
            f"━━━━━━━━━━━━━━━━━━━━",
            reply_markup=audio_menu_kb(ws),
        )


# ==================== DSP EFFECTS SUB-MENU ACTIONS ====================
@bot.on_callback_query(filters.regex(r"^fx_"))
async def fx_actions_callbacks(client: Client, query: CallbackQuery):
    if not is_authorized(None, None, query):
        return await query.answer("⛔ Access Restricted!", show_alert=True)

    uid = query.from_user.id
    chat_id = query.message.chat.id
    ws = await MANAGER.get_ws(uid)
    cmd = query.data.replace("fx_", "")

    if cmd == "show":
        fx = ws.effects
        echo_str = f"{fx['echo'][0]}ms / {fx['echo'][1]}" if fx["echo"] else "Off"
        return await query.message.edit_text(
            f"🎛️ **Your Active Audio Effects**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔊 **Level:** `{fx['level']}`\n"
            f"🎸 **Bass:** `{fx['bass']} dB`\n"
            f"🎻 **Treble:** `{fx['treble']} dB`\n"
            f"⚡ **Gain:** `{fx['gain']} dB`\n"
            f"🔥 **Overdrive:** `{fx['overdrive']}`\n"
            f"🗣️ **Echo:** `{echo_str}`",
            reply_markup=fx_menu_kb(ws),
        )

    elif cmd == "reset":
        ws.effects = {
            "level": 1.0, "bass": 0.0, "treble": 0.0,
            "gain": 0.0, "overdrive": 0.0, "echo": None,
        }
        await ws.save_to_mongo()
        await ws.reload_streams()
        await query.answer("🔄 All effects reset to default!", show_alert=True)
        return await query.message.edit_text(
            "✅ **All audio effects have been reset!**", reply_markup=fx_menu_kb(ws)
        )

    elif cmd == "echo":
        await query.answer()
        ans = await client.ask(
            chat_id,
            "🗣️ **Send Echo value as `<delay_ms>/<decay>` (e.g. `500/0.6`) or `off`:**",
            timeout=60,
        )
        val_str = ans.text.strip().lower()
        if val_str == "off":
            ws.effects["echo"] = None
        else:
            try:
                p = val_str.split("/")
                ws.effects["echo"] = (int(p[0]), min(max(float(p[1]), 0.1), 0.99))
            except Exception:
                return await ans.reply_text("❌ **Invalid format!**", reply_markup=fx_menu_kb(ws))
        await ws.save_to_mongo()
        await ws.reload_streams()
        return await ans.reply_text("✅ **Echo Effect Updated!**", reply_markup=fx_menu_kb(ws))

    elif cmd in ("level", "bass", "treble", "gain", "overdrive"):
        await query.answer()
        ans = await client.ask(
            chat_id,
            f"🎚️ **Send new numeric value for `{cmd.upper()}` (0 - ∞):**",
            timeout=60,
        )
        try:
            val = float(ans.text.strip())
            ws.effects[cmd] = val
            await ws.save_to_mongo()
            await ws.reload_streams()
            await ans.reply_text(
                f"✅ **{cmd.capitalize()} updated to `{val}` & saved to MongoDB!**",
                reply_markup=fx_menu_kb(ws),
            )
        except ValueError:
            await ans.reply_text("❌ **Invalid number!**", reply_markup=fx_menu_kb(ws))


# ==================== MY ACCOUNTS (MONGODB) SUB-MENU ACTIONS ====================
@bot.on_callback_query(filters.regex(r"^acc_"))
async def acc_actions_callbacks(client: Client, query: CallbackQuery):
    if not is_authorized(None, None, query):
        return await query.answer("⛔ Access Restricted!", show_alert=True)

    uid = query.from_user.id
    chat_id = query.message.chat.id
    ws = await MANAGER.get_ws(uid)
    act = query.data

    if act == "acc_otp":
        await query.answer()
        try:
            role_q = await client.ask(
                chat_id,
                "📱 **Add Account to MongoDB via Phone (OTP/2FA)**\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "Reply `1` for **Recorder Account** (Source Listener)\n"
                "Reply `2` for **Broadcaster Account** (Target Speaker)\n"
                "*(Or `/cancel`)*",
                timeout=60,
            )
            if role_q.text.startswith("/cancel"):
                return await role_q.reply_text("🚫 **Cancelled!**", reply_markup=acc_menu_kb())
            role = "rec" if role_q.text.strip() == "1" else "bcast"

            phone_q = await client.ask(
                chat_id, "📞 **Send Phone Number with country code (e.g. `+919876543210`):**", timeout=120
            )
            phone = phone_q.text.strip()

            tmp = Client(
                f"otp_{uid}_{int(time.time())}",
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                in_memory=True,
            )
            await tmp.connect()
            sent = await tmp.send_code(phone)

            otp_q = await client.ask(
                chat_id, "📩 **Enter the 5-digit Telegram OTP with spaces (e.g. `1 2 3 4 5`):**", timeout=120
            )
            otp = otp_q.text.replace(" ", "").strip()

            try:
                await tmp.sign_in(phone, sent.phone_code_hash, otp)
            except SessionPasswordNeeded:
                pwd_q = await client.ask(chat_id, "🔐 **Send your 2FA Password:**", timeout=120)
                await tmp.check_password(pwd_q.text.strip())

            s_str = await tmp.export_session_string()
            me = await tmp.get_me()
            await tmp.disconnect()

            if role == "rec":
                await ws.set_recorder(s_str, save=True)
                await client.send_message(
                    chat_id,
                    f"✅ **Recorder Account Saved to MongoDB!**\n👤 **{me.first_name}** (`{me.id}`)",
                    reply_markup=acc_menu_kb(),
                )
            else:
                await ws.add_broadcaster(s_str, save=True)
                await client.send_message(
                    chat_id,
                    f"✅ **Broadcaster #{len(ws.broadcaster_users)} Saved to MongoDB!**\n👤 **{me.first_name}** (`{me.id}`)",
                    reply_markup=acc_menu_kb(),
                )
        except Exception as e:
            await client.send_message(chat_id, f"❌ **OTP Login Failed:** `{e}`", reply_markup=acc_menu_kb())

    elif act in ("acc_rec_str", "acc_bcast_str"):
        await query.answer()
        label = "Recorder" if act == "acc_rec_str" else "Broadcaster"
        ans = await client.ask(
            chat_id, f"🔑 **Send Pyrogram/Pyrofork `STRING_SESSION` for your {label} Account:**", timeout=120
        )
        if ans.text.startswith("/cancel"):
            return await ans.reply_text("🚫 **Cancelled!**", reply_markup=acc_menu_kb())
        wait = await ans.reply_text("⏳ **Connecting & saving to MongoDB...**")
        ok = (
            await ws.set_recorder(ans.text.strip(), save=True)
            if act == "acc_rec_str"
            else await ws.add_broadcaster(ans.text.strip(), save=True)
        )
        if ok:
            await wait.edit_text(f"✅ **{label} Account Connected & Saved in MongoDB!**", reply_markup=acc_menu_kb())
        else:
            await wait.edit_text("❌ **Invalid Session String!**", reply_markup=acc_menu_kb())

    elif act == "acc_view":
        rec_info = "❌ None"
        if ws.recorder_user:
            try:
                rme = await ws.recorder_user.get_me()
                rec_info = f"✅ {rme.first_name} (`{rme.id}`)"
            except Exception:
                rec_info = "⚠️ Error"
        b_list = []
        for idx, u in enumerate(ws.broadcaster_users):
            try:
                bme = await u.get_me()
                b_list.append(f"  `{idx+1}.` {bme.first_name} (`{bme.id}`)")
            except Exception:
                b_list.append(f"  `{idx+1}.` Disconnected")
        b_str = "\n".join(b_list) or "  ❌ None"
        await query.message.edit_text(
            f"📋 **Your MongoDB Saved Accounts**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🎙️ **Recorder:** {rec_info}\n\n"
            f"📡 **Broadcasters ({len(ws.broadcaster_users)}):**\n{b_str}",
            reply_markup=acc_menu_kb(),
        )

    elif act == "acc_del":
        await query.answer()
        ans = await client.ask(
            chat_id,
            "🗑️ **Send `0` to delete your Recorder Account, or `1, 2...` to delete a Broadcaster Account:**",
            timeout=60,
        )
        try:
            num = int(ans.text.strip())
            if num == 0:
                if ws.recorder_user:
                    await ws.recorder_user.stop()
                ws.recorder_user = None
                ws.recorder_call = None
                ws.recorder_session = ""
                await ws.save_to_mongo()
                await ans.reply_text("✅ **Recorder removed from MongoDB!**", reply_markup=acc_menu_kb())
            else:
                idx = num - 1
                u_cli = ws.broadcaster_users.pop(idx)
                ws.broadcaster_calls.pop(idx)
                ws.broadcaster_sessions.pop(idx)
                await ws.save_to_mongo()
                await u_cli.stop()
                await ans.reply_text(f"✅ **Broadcaster #{num} removed from MongoDB!**", reply_markup=acc_menu_kb())
        except Exception as e:
            await ans.reply_text(f"❌ **Error:** `{e}`", reply_markup=acc_menu_kb())


# ==================== UTILITY SUB-MENU ACTIONS ====================
@bot.on_callback_query(filters.regex(r"^util_"))
async def util_actions_callbacks(client: Client, query: CallbackQuery):
    if not is_authorized(None, None, query):
        return await query.answer("⛔ Access Restricted!", show_alert=True)

    uid = query.from_user.id
    ws = await MANAGER.get_ws(uid)
    u_cmd = query.data

    if u_cmd == "util_ping":
        t0 = time.perf_counter()
        await query.answer("🏓 Pinging...")
        ms = (time.perf_counter() - t0) * 1000
        await query.message.edit_text(f"🏓 **Pong!** `{ms:.2f} ms`", reply_markup=util_menu_kb())

    elif u_cmd == "util_stats":
        cpu = psutil.cpu_percent()
        ram = psutil.virtual_memory().percent
        up = int(time.time() - START_TIME)
        h, r = divmod(up, 3600)
        m, s = divmod(r, 60)
        await query.message.edit_text(
            f"📊 **System & Workspace Status**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📡 **Your Source:** `{ws.source_chat or 'None'}`\n"
            f"🎙️ **Recording:** `{'🟢 Active' if ws.recording else '🔴 Stopped'}`\n"
            f"🔗 **Your Targets:** `{len(ws.targets)}/{len(ws.broadcaster_users)}`\n"
            f"💻 **CPU:** `{cpu}%` | 🧠 **RAM:** `{ram}%`\n"
            f"⏱️ **Uptime:** `{h}h {m}m {s}s`",
            reply_markup=util_menu_kb(),
        )

    elif u_cmd == "util_list":
        if not ws.targets:
            return await query.message.edit_text("📭 **No active target streams!**", reply_markup=util_menu_kb())
        lines = [f"• `{tid}` (Broadcaster #{idx+1})" for tid, idx in ws.targets.items()]
        await query.message.edit_text(
            "🔗 **Your Active Forward List:**\n" + "\n".join(lines), reply_markup=util_menu_kb()
        )

    elif u_cmd == "util_id":
        await query.message.edit_text(
            f"🆔 **ID Information**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 **Your User ID:** `{uid}`\n"
            f"💬 **Current Chat ID:** `{query.message.chat.id}`",
            reply_markup=util_menu_kb(),
        )


# ==================== OWNER ADMIN PANEL ACTIONS ====================
@bot.on_callback_query(filters.regex(r"^own_"))
async def owner_actions_callbacks(client: Client, query: CallbackQuery):
    if query.from_user.id != config.OWNER_ID:
        return await query.answer("❌ Owner Only!", show_alert=True)

    chat_id = query.message.chat.id
    act = query.data

    if act in ("own_approve", "own_disapprove", "own_ban", "own_unban"):
        await query.answer()
        prompts = {
            "own_approve": "✅ **Send User ID to APPROVE (saves to MongoDB & notifies user):**",
            "own_disapprove": "❌ **Send User ID to REMOVE ACCESS:**",
            "own_ban": "🚫 **Send User ID to BAN:**",
            "own_unban": "🟢 **Send User ID to UNBAN:**",
        }
        ans = await client.ask(chat_id, prompts[act] + "\n*(Or `/cancel`)*", timeout=60)
        if ans.text.startswith("/cancel"):
            return await ans.reply_text("🚫 **Cancelled!**", reply_markup=owner_menu_kb())
        try:
            t_uid = int(ans.text.strip())
            if t_uid == config.OWNER_ID:
                return await ans.reply_text("❌ **Cannot modify Owner ID!**", reply_markup=owner_menu_kb())

            if act == "own_approve":
                MANAGER.banned_users.discard(t_uid)
                MANAGER.approved_users.add(t_uid)
                await MANAGER.save_access_to_mongo()
                await ans.reply_text(f"✅ **User `{t_uid}` approved in MongoDB!**", reply_markup=owner_menu_kb())
                try:
                    await client.send_message(
                        t_uid,
                        "🎉 **ACCESS APPROVED!**\nYou have been granted access to the bot. Send `/start`!",
                        reply_markup=main_menu_kb(t_uid),
                    )
                except Exception:
                    pass

            elif act == "own_disapprove":
                MANAGER.approved_users.discard(t_uid)
                if t_uid in MANAGER.workspaces:
                    await MANAGER.workspaces[t_uid].leave_all()
                await MANAGER.save_access_to_mongo()
                await ans.reply_text(f"❌ **Access removed from `{t_uid}`!**", reply_markup=owner_menu_kb())

            elif act == "own_ban":
                MANAGER.approved_users.discard(t_uid)
                MANAGER.banned_users.add(t_uid)
                if t_uid in MANAGER.workspaces:
                    await MANAGER.workspaces[t_uid].leave_all()
                await MANAGER.save_access_to_mongo()
                await ans.reply_text(f"🚫 **User `{t_uid}` BANNED in MongoDB!**", reply_markup=owner_menu_kb())

            elif act == "own_unban":
                MANAGER.banned_users.discard(t_uid)
                await MANAGER.save_access_to_mongo()
                await ans.reply_text(f"🟢 **User `{t_uid}` UNBANNED!**", reply_markup=owner_menu_kb())
        except Exception as e:
            await ans.reply_text(f"❌ **Error:** `{e}`", reply_markup=owner_menu_kb())

    elif act == "own_list":
        app_s = ", ".join([f"`{u}`" for u in MANAGER.approved_users]) or "None"
        ban_s = ", ".join([f"`{u}`" for u in MANAGER.banned_users]) or "None"
        db_lines = [
            f"• `{uid}`: Rec={'✅' if ws.recorder_user else '❌'} | Bcast={len(ws.broadcaster_users)}"
            for uid, ws in MANAGER.workspaces.items()
        ]
        await query.message.edit_text(
            f"📋 **MongoDB Database Overview**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ **Approved IDs:** {app_s}\n"
            f"🚫 **Banned IDs:** {ban_s}\n\n"
            f"👥 **User Accounts in DB:**\n" + ("\n".join(db_lines) or "None"),
            reply_markup=owner_menu_kb(),
        )

    elif act == "own_restart":
        await query.answer("🔄 Restarting bot...", show_alert=True)
        for ws in MANAGER.workspaces.values():
            await ws.leave_all()
        os.execv(sys.executable, [sys.executable] + sys.argv)


# ==================== TEXT COMMAND HANDLERS (ALL COMMANDS SUPPORTED) ====================
@bot.on_message(
    filters.command([
        "help", "panel", "record", "setrecordgroup", "join", "leave", "leaveall",
        "leaverecord", "rejoin", "mute", "unmute", "level", "bass", "treble",
        "gain", "overdrive", "echo", "effects", "reset", "ping", "stats",
        "status", "list", "id",
    ])
)
async def all_text_commands_router(client: Client, message: Message):
    if not is_authorized(None, None, message):
        return await handle_unauthorized_user(client, message)

    uid = message.from_user.id
    ws = await MANAGER.get_ws(uid)
    cmd = message.command[0].lower()

    if cmd in ("help", "panel"):
        return await message.reply_text(
            START_UI.format(mention=message.from_user.mention),
            reply_markup=main_menu_kb(uid),
        )

    elif cmd == "id":
        return await message.reply_text(
            f"🆔 **Chat ID:** `{message.chat.id}`\n👤 **Your ID:** `{uid}`",
            reply_markup=main_menu_kb(uid),
        )

    elif cmd == "ping":
        t0 = time.perf_counter()
        m = await message.reply_text("🏓 **Pinging...**")
        ms = (time.perf_counter() - t0) * 1000
        return await m.edit_text(f"🏓 **Pong!** `{ms:.2f} ms`", reply_markup=util_menu_kb())

    elif cmd in ("stats", "status"):
        cpu = psutil.cpu_percent()
        ram = psutil.virtual_memory().percent
        return await message.reply_text(
            f"📊 **Status**\n📡 **Source:** `{ws.source_chat}`\n🔗 **Targets:** `{len(ws.targets)}`\n💻 **CPU:** `{cpu}%` | 🧠 **RAM:** `{ram}%`",
            reply_markup=util_menu_kb(),
        )

    elif cmd == "list":
        lines = [f"• `{t}`" for t in ws.targets] or ["📭 None"]
        return await message.reply_text("🔗 **Forward List:**\n" + "\n".join(lines), reply_markup=util_menu_kb())

    elif cmd == "setrecordgroup":
        if len(message.command) < 2:
            return await message.reply_text("❌ **Usage:** `/setrecordgroup <chat_id>`", reply_markup=audio_menu_kb(ws))
        ws.source_chat = int(message.command[1])
        await ws.save_to_mongo()
        return await message.reply_text(f"✅ **Source saved:** `{ws.source_chat}`", reply_markup=audio_menu_kb(ws))

    elif cmd == "record":
        if len(message.command) > 1:
            ws.source_chat = int(message.command[1])
            await ws.save_to_mongo()
        elif not ws.source_chat and message.chat.type.name != "PRIVATE":
            ws.source_chat = message.chat.id
            await ws.save_to_mongo()
        if not ws.recorder_call or not ws.source_chat:
            return await message.reply_text(
                "❌ **Configure your Recorder Account & Source Chat first!**",
                reply_markup=audio_menu_kb(ws),
            )
        await ws.start_bridge()
        await ws.recorder_call.record(
            ws.source_chat, RecordStream(ws.fifo_path, AudioParameters(bitrate=48000, channels=2))
        )
        ws.recording = True
        return await message.reply_text(
            f"✅ **Recording started on `{ws.source_chat}`!**", reply_markup=audio_menu_kb(ws)
        )

    elif cmd == "join":
        if len(message.command) < 2:
            return await message.reply_text("❌ **Usage:** `/join <target_id>`", reply_markup=audio_menu_kb(ws))
        t_id = int(message.command[1])
        b_idx = ws.targets.get(t_id, ws.get_free_broadcaster())
        if b_idx is None:
            return await message.reply_text("❌ **No free Broadcaster Account!**", reply_markup=acc_menu_kb())
        await ws.broadcaster_calls[b_idx].play(t_id, ws.get_target_stream())
        ws.targets[t_id] = b_idx
        return await message.reply_text(f"✅ **Forwarding to `{t_id}`!**", reply_markup=audio_menu_kb(ws))

    elif cmd == "leave":
        if len(message.command) < 2:
            return await message.reply_text("❌ **Usage:** `/leave <target_id>`")
        tid = int(message.command[1])
        if tid in ws.targets:
            b_idx = ws.targets.pop(tid)
            await ws.broadcaster_calls[b_idx].leave_call(tid)
        return await message.reply_text(f"✅ **Left `{tid}`!**", reply_markup=audio_menu_kb(ws))

    elif cmd == "leaveall":
        cnt = await ws.leave_all()
        return await message.reply_text(f"✅ **Disconnected all ({cnt}) chats!**", reply_markup=audio_menu_kb(ws))

    elif cmd == "leaverecord":
        if ws.source_chat and ws.recorder_call:
            await ws.recorder_call.leave_call(ws.source_chat)
        await ws.stop_bridge()
        ws.recording = False
        return await message.reply_text("✅ **Left Source Chat!**", reply_markup=audio_menu_kb(ws))

    elif cmd == "rejoin":
        await ws.reload_streams()
        return await message.reply_text("🔄 **Reconnected all streams!**", reply_markup=audio_menu_kb(ws))

    elif cmd in ("mute", "unmute"):
        ws.muted = cmd == "mute"
        for tid, idx in ws.targets.items():
            if ws.muted:
                await ws.broadcaster_calls[idx].mute_stream(tid)
            else:
                await ws.broadcaster_calls[idx].unmute_stream(tid)
        return await message.reply_text(f"✅ **Streams {cmd}d!**", reply_markup=audio_menu_kb(ws))

    elif cmd in ("level", "bass", "treble", "gain", "overdrive"):
        if len(message.command) < 2:
            return await message.reply_text(f"❌ **Usage:** `/{cmd} <val>`", reply_markup=fx_menu_kb(ws))
        ws.effects[cmd] = float(message.command[1])
        await ws.save_to_mongo()
        await ws.reload_streams()
        return await message.reply_text(f"🎛️ **{cmd.capitalize()} set to `{ws.effects[cmd]}`!**", reply_markup=fx_menu_kb(ws))

    elif cmd == "echo":
        if len(message.command) < 2 or message.command[1].lower() == "off":
            ws.effects["echo"] = None
        else:
            p = message.command[1].split("/")
            ws.effects["echo"] = (int(p[0]), float(p[1]) if len(p) > 1 else 0.5)
        await ws.save_to_mongo()
        await ws.reload_streams()
        return await message.reply_text("✅ **Echo updated!**", reply_markup=fx_menu_kb(ws))

    elif cmd == "effects":
        return await message.reply_text(f"🎛️ **Effects:** `{ws.effects}`", reply_markup=fx_menu_kb(ws))

    elif cmd == "reset":
        ws.effects = {"level": 1.0, "bass": 0.0, "treble": 0.0, "gain": 0.0, "overdrive": 0.0, "echo": None}
        await ws.save_to_mongo()
        await ws.reload_streams()
        return await message.reply_text("🔄 **Effects reset!**", reply_markup=fx_menu_kb(ws))


# ==================== OWNER DIRECT COMMANDS ====================
@bot.on_message(
    filters.command(["owner", "approve", "disapprove", "ban", "unban", "userlist", "restart"])
    & filters.user(config.OWNER_ID)
)
async def owner_direct_cmds(client: Client, message: Message):
    cmd = message.command[0].lower()
    if cmd == "owner":
        return await message.reply_text("👑 **Owner Panel:**", reply_markup=owner_menu_kb())
    elif cmd == "userlist":
        return await message.reply_text(
            f"✅ **Approved:** `{list(MANAGER.approved_users)}`\n🚫 **Banned:** `{list(MANAGER.banned_users)}`",
            reply_markup=owner_menu_kb(),
        )
    elif cmd == "restart":
        await message.reply_text("🔄 **Restarting...**")
        os.execv(sys.executable, [sys.executable] + sys.argv)

    uid = (
        message.reply_to_message.from_user.id
        if message.reply_to_message and message.reply_to_message.from_user
        else (int(message.command[1]) if len(message.command) > 1 else None)
    )
    if not uid:
        return await message.reply_text(f"❌ **Usage:** `/{cmd} <user_id>`", reply_markup=owner_menu_kb())

    if cmd == "approve":
        MANAGER.banned_users.discard(uid)
        MANAGER.approved_users.add(uid)
        await MANAGER.save_access_to_mongo()
        await message.reply_text(f"✅ **Approved `{uid}` in MongoDB!**", reply_markup=owner_menu_kb())
        try:
            await client.send_message(
                uid, "🎉 **You have been approved! Send `/start` to use the bot.**", reply_markup=main_menu_kb(uid)
            )
        except Exception:
            pass
    elif cmd == "disapprove":
        MANAGER.approved_users.discard(uid)
        if uid in MANAGER.workspaces:
            await MANAGER.workspaces[uid].leave_all()
        await MANAGER.save_access_to_mongo()
        await message.reply_text(f"❌ **Removed access for `{uid}`!**", reply_markup=owner_menu_kb())
    elif cmd == "ban":
        MANAGER.approved_users.discard(uid)
        MANAGER.banned_users.add(uid)
        if uid in MANAGER.workspaces:
            await MANAGER.workspaces[uid].leave_all()
        await MANAGER.save_access_to_mongo()
        await message.reply_text(f"🚫 **Banned `{uid}`!**", reply_markup=owner_menu_kb())
    elif cmd == "unban":
        MANAGER.banned_users.discard(uid)
        await MANAGER.save_access_to_mongo()
        await message.reply_text(f"🟢 **Unbanned `{uid}`!**", reply_markup=owner_menu_kb())


# ==================== BOOTSTRAP ====================
async def bootstrap():
    print("🚀 Starting Nobi Fight Bot with MongoDB...")
    await bot.start()
    await MANAGER.load_from_mongo()
    print("✅ Bot & MongoDB Workspaces are LIVE!")
    await asyncio.Event().wait()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(bootstrap())
EOF
