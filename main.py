import os
import sys
import time
import asyncio
import subprocess
import psutil
from typing import Dict, Set, List, Optional

import pyromod
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
DB = config.load_db()

bot = Client(
    "NobiFightBot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
)


# ==================== PER-USER ISOLATED WORKSPACE ====================
class UserWorkspace:
    """Isolated state, accounts, audio pipe, and DSP effects for a single Telegram user."""

    def __init__(self, user_id: int, saved_data: dict):
        self.user_id = user_id
        self.source_chat: Optional[int] = saved_data.get("source_chat")
        self.recording: bool = False
        self.muted: bool = False

        # Target Chat ID -> Broadcaster Index in this user's personal pool
        self.targets: Dict[int, int] = {}

        # User's personal accounts (No other user can access these)
        self.recorder_session: str = saved_data.get("recorder", "")
        self.broadcaster_sessions: List[str] = list(saved_data.get("broadcasters", []))

        self.recorder_user: Optional[Client] = None
        self.recorder_call: Optional[PyTgCalls] = None
        self.broadcaster_users: List[Client] = []
        self.broadcaster_calls: List[PyTgCalls] = []

        # Dedicated Virtual Audio Pipe & PulseAudio Sink for this user
        self.fifo_path = f"/tmp/nobi_fifo_{user_id}.pcm"
        self.sink_name = f"FightSink_{user_id}"
        self.sink_module_id: Optional[str] = None
        self.ffmpeg_proc: Optional[asyncio.subprocess.Process] = None

        self.effects = {
            "level": 1.0,
            "bass": 0.0,
            "treble": 0.0,
            "gain": 0.0,
            "overdrive": 0.0,
            "echo": None,
        }

        self._setup_user_audio_pipe()

    def _setup_user_audio_pipe(self):
        if os.path.exists(self.fifo_path):
            try:
                os.remove(self.fifo_path)
            except Exception:
                pass
        os.mkfifo(self.fifo_path)

        # Create isolated PulseAudio null-sink for this user
        try:
            res = subprocess.check_output(
                [
                    "pactl",
                    "load-module",
                    "module-null-sink",
                    f"sink_name={self.sink_name}",
                ],
                text=True,
            ).strip()
            self.sink_module_id = res
        except Exception as e:
            print(f"[WARN] PulseAudio sink creation for {self.user_id}: {e}")

    def to_dict(self) -> dict:
        return {
            "recorder": self.recorder_session,
            "broadcasters": self.broadcaster_sessions,
            "source_chat": self.source_chat,
        }

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
                MANAGER.sync_db()
            return True
        except Exception as e:
            print(f"[ERROR] User {self.user_id} Recorder failed: {e}")
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
                MANAGER.sync_db()
            return True
        except Exception as e:
            print(f"[ERROR] User {self.user_id} Broadcaster failed: {e}")
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
                print(f"[WARN] User {self.user_id} reload error on {t_id}: {e}")

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


# ==================== GLOBAL MULTI-TENANT MANAGER ====================
class GlobalManager:
    def __init__(self):
        self.approved_users: Set[int] = set(DB.get("approved_users", [config.OWNER_ID]))
        self.approved_users.add(config.OWNER_ID)
        self.banned_users: Set[int] = set(DB.get("banned_users", []))
        self.open_for_all: bool = DB.get("open_for_all", config.OPEN_FOR_ALL)
        self.workspaces: Dict[int, UserWorkspace] = {}

    def get_ws(self, user_id: int) -> UserWorkspace:
        if user_id not in self.workspaces:
            saved = DB.get("users", {}).get(str(user_id), {})
            self.workspaces[user_id] = UserWorkspace(user_id, saved)
        return self.workspaces[user_id]

    def sync_db(self):
        DB["approved_users"] = list(self.approved_users)
        DB["banned_users"] = list(self.banned_users)
        DB["open_for_all"] = self.open_for_all
        DB["users"] = {
            str(uid): ws.to_dict() for uid, ws in self.workspaces.items()
        }
        config.save_db(DB)


MANAGER = GlobalManager()


# ==================== ACCESS FILTERS ====================
def check_access(_, __, update: Message | CallbackQuery) -> bool:
    user = update.from_user
    if not user:
        return False
    if user.id in MANAGER.banned_users:
        return False
    if user.id == config.OWNER_ID or MANAGER.open_for_all:
        return True
    return user.id in MANAGER.approved_users

access_filter = filters.create(check_access)


# ==================== UI TEMPLATES & KEYBOARDS ====================
START_UI = """✨ **Welcome to Audio Forwarder** ✨
━━━━━━━━━━━━━━━━━━━━
👋 **Hello, {mention}!**
━━━━━━━━━━━━━━━━━━━━
🎵 **Real-time Audio Forwarding**
📊 **System:** `██████████` 100% ✅
⚡ **Features:**
• Private Per-User Account Login
• Control 100% inside Bot DM (No Group Needed!)
• Real-time forwarding & DSP Effects
• Multi-chat forwarding
• Bass/Treble/Gain/Overdrive/Echo
━━━━━━━━━━━━━━━━━━━━
📱 **Click `My Accounts` below to login your account!**"""

HELP_UI = """📖 **Help Menu (Works in Bot DM!)**
━━━━━━━━━━━━━━━━━━━━
📱 **1. Account Setup**
Use the `📱 My Accounts` button to add your **Recorder** (listener) and **Broadcaster** (speaker) accounts via OTP or Session String. Only YOU can use your accounts!
━━━━━━━━━━━━━━━━━━━━
🎵 **2. Audio Controls (Inside Bot DM)**
/setrecordgroup <id> - Set your Source VC Chat ID
/record [id] - Start recording from Source VC
/join <id> - Forward audio to Target VC Chat ID
/rejoin - Reconnect all your streams
/leave <id> - Stop forwarding to chat
/leaveall - Stop all your streams
/leaverecord - Leave source VC
/mute - Mute your streams
/unmute - Unmute your streams
━━━━━━━━━━━━━━━━━━━━
🎛️ **3. Effects (no limits!)**
/level <0-∞> - Volume
/bass <0-∞> - Bass
/treble <0-∞> - Treble
/gain <0-∞> - Gain
/overdrive <0-∞> - Distortion/Gain
/echo <delay/decay/off> - Echo
/effects - Show your active effects
/reset - Reset your effects
━━━━━━━━━━━━━━━━━━━━
📊 **4. Utility**
/panel - Interactive Control Panel
/list - Your active forwarding chats
/status - Your stream & account status
/id - Get chat ID
/ping - Check bot latency"""


def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("📱 My Accounts (Login)", callback_data="usr_accounts"),
            InlineKeyboardButton("🎛️ Control Panel", callback_data="usr_panel"),
        ],
        [
            InlineKeyboardButton("👤 Owner", user_id=config.OWNER_ID),
            InlineKeyboardButton("📖 Help", callback_data="nav_help"),
        ],
    ]
    if user_id == config.OWNER_ID:
        rows.append(
            [InlineKeyboardButton("👑 Owner Admin Panel (Bans & Access)", callback_data="own_main")]
        )
    return InlineKeyboardMarkup(rows)


def user_accounts_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📱 Login via Phone (OTP/2FA)", callback_data="acc_login_otp"),
        ],
        [
            InlineKeyboardButton("🎙️ Set Recorder (String)", callback_data="acc_set_rec"),
            InlineKeyboardButton("➕ Add Broadcaster (String)", callback_data="acc_add_bcast"),
        ],
        [
            InlineKeyboardButton("📋 My Connected Accounts", callback_data="acc_list"),
            InlineKeyboardButton("🗑️ Remove My Account", callback_data="acc_delete"),
        ],
        [
            InlineKeyboardButton("🏠 Back Home", callback_data="nav_home"),
        ],
    ])


def owner_panel_kb() -> InlineKeyboardMarkup:
    mode_str = "🟢 Open for All" if MANAGER.open_for_all else "🔒 Approved Only"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"Mode: {mode_str} (Toggle)", callback_data="own_toggle_mode"),
        ],
        [
            InlineKeyboardButton("✅ Approve User", callback_data="own_approve"),
            InlineKeyboardButton("❌ Remove Access", callback_data="own_disapprove"),
        ],
        [
            InlineKeyboardButton("🚫 Ban User", callback_data="own_ban"),
            InlineKeyboardButton("🟢 Unban User", callback_data="own_unban"),
        ],
        [
            InlineKeyboardButton("📋 All Users & Banned List", callback_data="own_list_users"),
        ],
        [
            InlineKeyboardButton("🏠 Back Home", callback_data="nav_home"),
        ],
    ])


# ==================== START, HELP & MENU HANDLERS ====================
@bot.on_message(filters.command("start"))
async def start_cmd(client: Client, message: Message):
    uid = message.from_user.id
    if uid in MANAGER.banned_users:
        return await message.reply_text("🚫 **You are BANNED from using this bot!**")
    if not MANAGER.open_for_all and uid not in MANAGER.approved_users:
        return await message.reply_text(
            "🔒 **Bot is currently in Private Mode.**\nContact the Owner for access!",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("👤 Contact Owner", user_id=config.OWNER_ID)]]
            ),
        )
    await message.reply_text(
        START_UI.format(mention=message.from_user.mention),
        reply_markup=main_menu_kb(uid),
    )


@bot.on_message(filters.command("help") & access_filter)
async def help_cmd(client: Client, message: Message):
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Home", callback_data="nav_home")]])
    await message.reply_text(HELP_UI, reply_markup=kb)


# ==================== USER SELF-SERVICE ACCOUNT MANAGER ====================
@bot.on_callback_query(filters.regex(r"^(usr_accounts|acc_)") & access_filter)
async def user_account_callbacks(client: Client, query: CallbackQuery):
    uid = query.from_user.id
    chat_id = query.message.chat.id
    ws = MANAGER.get_ws(uid)
    data = query.data

    if data == "usr_accounts":
        rec_state = "✅ Connected" if ws.recorder_user else "❌ Not Set"
        text = (
            f"📱 **Your Private Account Manager**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔒 *Only YOU can control the accounts you add here.*\n\n"
            f"🎙️ **Your Recorder Account:** `{rec_state}`\n"
            f"📡 **Your Broadcaster Accounts:** `{len(ws.broadcaster_users)} active`\n"
            f"🎯 **Your Source Chat ID:** `{ws.source_chat or 'Not Set'}`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Choose how you want to add or manage your accounts:"
        )
        return await query.message.edit_text(text, reply_markup=user_accounts_kb())

    # 1. LOGIN VIA PHONE + OTP + 2FA (SELF-SERVICE)
    elif data == "acc_login_otp":
        await query.answer()
        try:
            role_msg = await client.ask(
                chat_id,
                "📱 **Add Account via Phone & OTP**\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "Reply with:\n"
                "• `1` — Set as your **Recorder Account** (Joins Source Chat to listen)\n"
                "• `2` — Add as a **Broadcaster Account** (Joins Target Chat to blast audio)\n\n"
                "*(Send `/cancel` to abort)*",
                timeout=60,
            )
            if role_msg.text.startswith("/cancel"):
                return await role_msg.reply_text("🚫 **Login Cancelled!**")
            role = "recorder" if role_msg.text.strip() == "1" else "broadcaster"

            phone_msg = await client.ask(
                chat_id,
                "📞 **Send your Phone Number with country code (e.g. `+919876543210`):**",
                timeout=120,
            )
            if phone_msg.text.startswith("/cancel"):
                return await phone_msg.reply_text("🚫 **Cancelled!**")
            phone = phone_msg.text.strip()

            temp_cli = Client(
                f"tmp_otp_{uid}_{int(time.time())}",
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                in_memory=True,
            )
            await temp_cli.connect()
            code_info = await temp_cli.send_code(phone)

            otp_msg = await client.ask(
                chat_id,
                "📩 **Send the 5-digit OTP received on Telegram with spaces (e.g., `1 2 3 4 5`):**",
                timeout=120,
            )
            otp_code = otp_msg.text.replace(" ", "").strip()

            try:
                await temp_cli.sign_in(phone, code_info.phone_code_hash, otp_code)
            except SessionPasswordNeeded:
                pwd_msg = await client.ask(
                    chat_id,
                    "🔐 **2FA (Two-Step Verification) is enabled on this account.**\n"
                    "Send your 2FA Password:",
                    timeout=120,
                )
                await temp_cli.check_password(pwd_msg.text.strip())

            session_str = await temp_cli.export_session_string()
            me = await temp_cli.get_me()
            await temp_cli.disconnect()

            if role == "recorder":
                await ws.set_recorder(session_str, save=True)
                await client.send_message(
                    chat_id,
                    f"✅ **Successfully added as your Recorder Account!**\n"
                    f"👤 **Account:** {me.first_name} (`{me.id}`)",
                    reply_markup=user_accounts_kb(),
                )
            else:
                await ws.add_broadcaster(session_str, save=True)
                await client.send_message(
                    chat_id,
                    f"✅ **Successfully added to your Broadcasters (#{len(ws.broadcaster_users)})!**\n"
                    f"👤 **Account:** {me.first_name} (`{me.id}`)",
                    reply_markup=user_accounts_kb(),
                )
        except Exception as e:
            await client.send_message(chat_id, f"❌ **Login Failed:** `{e}`")

    # 2. SET RECORDER VIA STRING SESSION
    elif data == "acc_set_rec":
        await query.answer()
        try:
            ask_s = await client.ask(
                chat_id,
                "🎙️ **Send your Pyrogram/Pyrofork `STRING_SESSION` for your Recorder Account:**\n"
                "*(Send `/cancel` to abort)*",
                timeout=120,
            )
            if ask_s.text.startswith("/cancel"):
                return await ask_s.reply_text("🚫 **Cancelled!**")
            wait = await ask_s.reply_text("⏳ **Verifying & starting your Recorder Account...**")
            ok = await ws.set_recorder(ask_s.text.strip(), save=True)
            if ok:
                me = await ws.recorder_user.get_me()
                await wait.edit_text(
                    f"✅ **Your Recorder Account is Ready!**\n👤 **{me.first_name}** (`{me.id}`)",
                    reply_markup=user_accounts_kb(),
                )
            else:
                await wait.edit_text("❌ **Invalid String Session!**")
        except asyncio.TimeoutError:
            await client.send_message(chat_id, "⏰ **Timed out.**")

    # 3. ADD BROADCASTER VIA STRING SESSION
    elif data == "acc_add_bcast":
        await query.answer()
        try:
            ask_s = await client.ask(
                chat_id,
                "📡 **Send your Pyrogram/Pyrofork `STRING_SESSION` for your Broadcaster Account:**\n"
                "*(Send `/cancel` to abort)*",
                timeout=120,
            )
            if ask_s.text.startswith("/cancel"):
                return await ask_s.reply_text("🚫 **Cancelled!**")
            wait = await ask_s.reply_text("⏳ **Verifying & starting your Broadcaster Account...**")
            ok = await ws.add_broadcaster(ask_s.text.strip(), save=True)
            if ok:
                me = await ws.broadcaster_users[-1].get_me()
                await wait.edit_text(
                    f"✅ **Broadcaster Account #{len(ws.broadcaster_users)} Added!**\n"
                    f"👤 **{me.first_name}** (`{me.id}`)",
                    reply_markup=user_accounts_kb(),
                )
            else:
                await wait.edit_text("❌ **Invalid String Session!**")
        except asyncio.TimeoutError:
            await client.send_message(chat_id, "⏰ **Timed out.**")

    # 4. LIST USER'S PERSONAL ACCOUNTS
    elif data == "acc_list":
        await query.answer()
        rec_txt = "❌ Not Set"
        if ws.recorder_user:
            try:
                rme = await ws.recorder_user.get_me()
                rec_txt = f"✅ {rme.first_name} (`{rme.id}`)"
            except Exception:
                rec_txt = "⚠️ Disconnected"

        b_lines = []
        for i, u in enumerate(ws.broadcaster_users):
            try:
                bme = await u.get_me()
                b_lines.append(f"  `{i+1}.` {bme.first_name} (`{bme.id}`)")
            except Exception:
                b_lines.append(f"  `{i+1}.` Disconnected")
        b_txt = "\n".join(b_lines) if b_lines else "  ❌ None Added"

        await query.message.edit_text(
            f"📋 **Your Personal Connected Accounts**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🎙️ **Recorder:** {rec_txt}\n\n"
            f"📡 **Broadcasters ({len(ws.broadcaster_users)}):**\n{b_txt}\n"
            f"━━━━━━━━━━━━━━━━━━━━",
            reply_markup=user_accounts_kb(),
        )

    # 5. DELETE USER'S OWN ACCOUNT
    elif data == "acc_delete":
        await query.answer()
        ask_del = await client.ask(
            chat_id,
            "🗑️ **Remove Your Account**\n"
            "• Send `0` to remove your **Recorder Account**\n"
            "• Or send the **Broadcaster Number** (`1`, `2`, ...) to remove that Broadcaster\n"
            "*(Send `/cancel` to abort)*",
            timeout=60,
        )
        if ask_del.text.startswith("/cancel"):
            return await ask_del.reply_text("🚫 **Cancelled!**")
        try:
            choice = int(ask_del.text.strip())
            if choice == 0:
                if ws.recorder_user:
                    await ws.recorder_user.stop()
                ws.recorder_user = None
                ws.recorder_call = None
                ws.recorder_session = ""
                MANAGER.sync_db()
                await ask_del.reply_text("✅ **Your Recorder Account was removed!**")
            else:
                idx = choice - 1
                u_cli = ws.broadcaster_users.pop(idx)
                ws.broadcaster_calls.pop(idx)
                ws.broadcaster_sessions.pop(idx)
                MANAGER.sync_db()
                await u_cli.stop()
                await ask_del.reply_text(f"✅ **Removed your Broadcaster Account #{choice}!**")
        except Exception as e:
            await ask_del.reply_text(f"❌ **Invalid selection:** `{e}`")


# ==================== OWNER ADMIN PANEL (BAN / UNBAN / ACCESS) ====================
@bot.on_message(filters.command("owner") & filters.user(config.OWNER_ID))
async def owner_cmd(client: Client, message: Message):
    await message.reply_text(
        "👑 **Master Owner Panel**\nManage user access, bans, and global mode:",
        reply_markup=owner_panel_kb(),
    )


@bot.on_callback_query(filters.regex(r"^own_"))
async def owner_callbacks(client: Client, query: CallbackQuery):
    if query.from_user.id != config.OWNER_ID:
        return await query.answer("❌ Only the Bot Owner can use this button!", show_alert=True)

    data = query.data
    chat_id = query.message.chat.id

    if data == "own_main":
        return await query.message.edit_text(
            f"👑 **Master Owner Panel**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🌐 **Public Mode:** `{'ON' if MANAGER.open_for_all else 'OFF (Approved Only)'}`\n"
            f"✅ **Approved Users:** `{len(MANAGER.approved_users)}`\n"
            f"🚫 **Banned Users:** `{len(MANAGER.banned_users)}`\n"
            f"👥 **Active Workspaces:** `{len(MANAGER.workspaces)}`",
            reply_markup=owner_panel_kb(),
        )

    elif data == "own_toggle_mode":
        MANAGER.open_for_all = not MANAGER.open_for_all
        MANAGER.sync_db()
        await query.answer("Updated Bot Access Mode!")
        return await owner_callbacks(client, query)

    elif data in ("own_approve", "own_disapprove", "own_ban", "own_unban"):
        await query.answer()
        titles = {
            "own_approve": "✅ **Send User ID to APPROVE:**",
            "own_disapprove": "❌ **Send User ID to REMOVE ACCESS (and stop their streams):**",
            "own_ban": "🚫 **Send User ID to BAN (stops streams & blocks bot):**",
            "own_unban": "🟢 **Send User ID to UNBAN:**",
        }
        try:
            msg = await client.ask(chat_id, titles[data] + "\n*(Or `/cancel`)*", timeout=60)
            if msg.text.startswith("/cancel"):
                return await msg.reply_text("🚫 **Cancelled!**")
            t_uid = int(msg.text.strip())
            if t_uid == config.OWNER_ID:
                return await msg.reply_text("❌ **Cannot modify Owner ID!**")

            if data == "own_approve":
                MANAGER.banned_users.discard(t_uid)
                MANAGER.approved_users.add(t_uid)
                res = f"✅ **Approved `{t_uid}`!**"
            elif data == "own_disapprove":
                MANAGER.approved_users.discard(t_uid)
                if t_uid in MANAGER.workspaces:
                    await MANAGER.workspaces[t_uid].leave_all()
                res = f"❌ **Removed access for `{t_uid}`!**"
            elif data == "own_ban":
                MANAGER.approved_users.discard(t_uid)
                MANAGER.banned_users.add(t_uid)
                if t_uid in MANAGER.workspaces:
                    await MANAGER.workspaces[t_uid].leave_all()
                res = f"🚫 **Banned `{t_uid}` and disconnected all their streams!**"
            else:
                MANAGER.banned_users.discard(t_uid)
                res = f"🟢 **Unbanned `{t_uid}`!**"

            MANAGER.sync_db()
            await msg.reply_text(res, reply_markup=owner_panel_kb())
        except Exception as e:
            await client.send_message(chat_id, f"❌ **Error:** `{e}`")

    elif data == "own_list_users":
        await query.answer()
        app_str = ", ".join([f"`{u}`" for u in MANAGER.approved_users]) or "None"
        ban_str = ", ".join([f"`{u}`" for u in MANAGER.banned_users]) or "None"
        ws_lines = [
            f"• `{uid}`: Rec={'✅' if ws.recorder_user else '❌'} | Bcast={len(ws.broadcaster_users)}"
            for uid, ws in MANAGER.workspaces.items()
        ]
        ws_str = "\n".join(ws_lines) or "None"
        await query.message.edit_text(
            f"📋 **Global Users & Workspaces**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 **Users with Accounts:**\n{ws_str}\n\n"
            f"✅ **Approved:** {app_str}\n"
            f"🚫 **Banned:** {ban_str}",
            reply_markup=owner_panel_kb(),
        )


# ==================== NAVIGATION & USER CONTROL PANEL ====================
@bot.on_callback_query(filters.regex(r"^(nav_|usr_panel|ctl_)") & access_filter)
async def nav_callbacks(client: Client, query: CallbackQuery):
    uid = query.from_user.id
    ws = MANAGER.get_ws(uid)
    data = query.data

    if data == "nav_home":
        return await query.message.edit_text(
            START_UI.format(mention=query.from_user.mention),
            reply_markup=main_menu_kb(uid),
        )
    elif data == "nav_help":
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Home", callback_data="nav_home")]])
        return await query.message.edit_text(HELP_UI, reply_markup=kb)

    elif data == "usr_panel":
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔇 Mute", callback_data="ctl_mute"),
                InlineKeyboardButton("🔊 Unmute", callback_data="ctl_unmute"),
            ],
            [
                InlineKeyboardButton("🔄 Reset FX", callback_data="ctl_reset"),
                InlineKeyboardButton("🛑 Leave All", callback_data="ctl_leaveall"),
            ],
            [
                InlineKeyboardButton("📱 My Accounts", callback_data="usr_accounts"),
                InlineKeyboardButton("🏠 Home", callback_data="nav_home"),
            ],
        ])
        status = "🟢 Live" if ws.recording else "🔴 Stopped"
        return await query.message.edit_text(
            f"🎛️ **Your Personal Control Panel**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📡 **Your Source Chat:** `{ws.source_chat or 'Not Set (/setrecordgroup)'}`\n"
            f"🔗 **Your Active Targets:** `{len(ws.targets)}/{len(ws.broadcaster_users)}`\n"
            f"📊 **Status:** {status}",
            reply_markup=kb,
        )

    elif data == "ctl_mute":
        ws.muted = True
        for t_id, b_idx in ws.targets.items():
            await ws.broadcaster_calls[b_idx].mute_stream(t_id)
        await query.answer("🔇 Your streams are now Muted!")
    elif data == "ctl_unmute":
        ws.muted = False
        for t_id, b_idx in ws.targets.items():
            await ws.broadcaster_calls[b_idx].unmute_stream(t_id)
        await query.answer("🔊 Your streams are now Unmuted!")
    elif data == "ctl_reset":
        for k in ws.effects:
            ws.effects[k] = 1.0 if k == "level" else (None if k == "echo" else 0.0)
        await ws.reload_streams()
        await query.answer("🔄 Your effects have been reset!")
    elif data == "ctl_leaveall":
        await ws.leave_all()
        await query.message.edit_text(
            "🛑 **All your chats disconnected successfully!**",
            reply_markup=main_menu_kb(uid),
        )


# ==================== DM-READY AUDIO COMMANDS (PER-USER) ====================
@bot.on_message(filters.command("setrecordgroup") & access_filter)
async def setrecordgroup_cmd(client: Client, message: Message):
    ws = MANAGER.get_ws(message.from_user.id)
    if len(message.command) < 2:
        return await message.reply_text(
            "❌ **Usage:** `/setrecordgroup <source_chat_id>`\n"
            "Example: `/setrecordgroup -1004426372558`"
        )
    try:
        ws.source_chat = int(message.command[1])
        MANAGER.sync_db()
        await message.reply_text(
            f"✅ **Your Source Recording Chat is set to:** `{ws.source_chat}`\n"
            f"Now you can simply type `/record` right here in DM!"
        )
    except ValueError:
        await message.reply_text("❌ **Invalid Chat ID!**")


@bot.on_message(filters.command("record") & access_filter)
async def record_cmd(client: Client, message: Message):
    ws = MANAGER.get_ws(message.from_user.id)
    if not ws.recorder_call:
        return await message.reply_text(
            "❌ **You haven't added your Recorder Account yet!**\n"
            "Click `/start` -> `📱 My Accounts` to login your account.",
            reply_markup=user_accounts_kb(),
        )

    # Allow /record <chat_id> directly in DM, or fallback to saved source_chat
    if len(message.command) > 1:
        try:
            ws.source_chat = int(message.command[1])
            MANAGER.sync_db()
        except ValueError:
            return await message.reply_text("❌ **Invalid Source Chat ID!**")
    elif not ws.source_chat:
        if message.chat.type.name != "PRIVATE":
            ws.source_chat = message.chat.id
            MANAGER.sync_db()
        else:
            return await message.reply_text(
                "❌ **Specify a Source Chat ID to record from in DM!**\n"
                "**Usage:** `/record -1004426372558` or `/setrecordgroup -1004426372558`"
            )

    try:
        await ws.start_bridge()
        await ws.recorder_call.record(
            ws.source_chat,
            RecordStream(
                ws.fifo_path,
                AudioParameters(bitrate=48000, channels=2),
            ),
        )
        ws.recording = True
    except Exception as e:
        return await message.reply_text(
            f"❌ **Could not record from `{ws.source_chat}`:**\n`{e}`\n\n"
            f"💡 *Make sure your Recorder Account is inside that group and the Voice Chat is active!*"
        )

    text = (
        f"✅ **Recording started successfully!**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 **Source:** `{ws.source_chat}`\n"
        f"🔗 **Forwarding:** {len(ws.targets)} chats\n"
        f"📊 **Status:** 🟢 Live\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🎵 **Audio is now flowing!**"
    )
    await message.reply_text(text)


@bot.on_message(filters.command("join") & access_filter)
async def join_cmd(client: Client, message: Message):
    ws = MANAGER.get_ws(message.from_user.id)
    if len(message.command) < 2:
        return await message.reply_text("❌ **Usage:** `/join <target_chat_id>`")

    try:
        target_id = int(message.command[1])
    except ValueError:
        return await message.reply_text("❌ **Invalid Chat ID!**")

    if target_id in ws.targets:
        b_idx = ws.targets[target_id]
    else:
        b_idx = ws.get_free_broadcaster()
        if b_idx is None:
            return await message.reply_text(
                "❌ **You have no free Broadcaster Accounts left!**\n"
                "Open `/start` -> `📱 My Accounts` to login another Broadcaster account.",
                reply_markup=user_accounts_kb(),
            )

    try:
        await ws.broadcaster_calls[b_idx].play(target_id, ws.get_target_stream())
        ws.targets[target_id] = b_idx
    except Exception as e:
        return await message.reply_text(
            f"❌ **Failed to join target VC `{target_id}`:**\n`{e}`\n\n"
            f"💡 *Make sure your Broadcaster Account #{b_idx+1} is in that group and VC is open!*"
        )

    text = (
        f"✅ **Forwarding started successfully!**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 **Target:** `{target_id}`\n"
        f"🔗 **Total connections:** {len(ws.targets)}\n"
        f"📊 **Status:** 🟢 Connected\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🎵 **Audio is now flowing to this chat!**"
    )
    await message.reply_text(text)


@bot.on_message(filters.command("leaveall") & access_filter)
async def leaveall_cmd(client: Client, message: Message):
    ws = MANAGER.get_ws(message.from_user.id)
    total_left = await ws.leave_all()
    text = (
        f"✅ **All chats disconnected successfully!**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 **Source:** ✅ Left\n"
        f"🔗 **Forwarding:** {total_left}/{total_left} left\n"
        f"📊 **Status:** 🔴 All disconnected\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )
    await message.reply_text(text)


@bot.on_message(filters.command("leave") & access_filter)
async def leave_cmd(client: Client, message: Message):
    ws = MANAGER.get_ws(message.from_user.id)
    if len(message.command) < 2:
        return await message.reply_text("❌ **Usage:** `/leave <target_chat_id>`")
    try:
        target_id = int(message.command[1])
    except ValueError:
        return await message.reply_text("❌ **Invalid Chat ID!**")

    if target_id not in ws.targets:
        return await message.reply_text("❌ **You are not forwarding to that chat!**")

    b_idx = ws.targets.pop(target_id)
    try:
        await ws.broadcaster_calls[b_idx].leave_call(target_id)
    except Exception:
        pass
    await message.reply_text(f"✅ **Stopped forwarding to `{target_id}`!**")


@bot.on_message(filters.command("leaverecord") & access_filter)
async def leaverecord_cmd(client: Client, message: Message):
    ws = MANAGER.get_ws(message.from_user.id)
    if ws.source_chat and ws.recorder_call:
        try:
            await ws.recorder_call.leave_call(ws.source_chat)
        except Exception:
            pass
    await ws.stop_bridge()
    ws.recording = False
    await message.reply_text("✅ **Your Recorder Account left the Source VC!**")


@bot.on_message(filters.command("rejoin") & access_filter)
async def rejoin_cmd(client: Client, message: Message):
    ws = MANAGER.get_ws(message.from_user.id)
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
    await message.reply_text("🔄 **Reconnected all your active streams!**")


@bot.on_message(filters.command("mute") & access_filter)
async def mute_cmd(client: Client, message: Message):
    ws = MANAGER.get_ws(message.from_user.id)
    ws.muted = True
    for t_id, b_idx in ws.targets.items():
        try:
            await ws.broadcaster_calls[b_idx].mute_stream(t_id)
        except Exception:
            pass
    await message.reply_text("🔇 **Your target streams are now Muted!**")


@bot.on_message(filters.command("unmute") & access_filter)
async def unmute_cmd(client: Client, message: Message):
    ws = MANAGER.get_ws(message.from_user.id)
    ws.muted = False
    for t_id, b_idx in ws.targets.items():
        try:
            await ws.broadcaster_calls[b_idx].unmute_stream(t_id)
        except Exception:
            pass
    await message.reply_text("🔊 **Your target streams are now Unmuted!**")


# ==================== PER-USER DSP EFFECTS ====================
@bot.on_message(
    filters.command(["level", "bass", "treble", "gain", "overdrive"]) & access_filter
)
async def fx_cmd(client: Client, message: Message):
    ws = MANAGER.get_ws(message.from_user.id)
    cmd = message.command[0].lower()
    if len(message.command) < 2:
        return await message.reply_text(f"❌ **Usage:** `/{cmd} <0-∞>`")
    try:
        val = float(message.command[1])
    except ValueError:
        return await message.reply_text("❌ **Invalid number!**")

    ws.effects[cmd] = val
    await ws.reload_streams()
    await message.reply_text(
        f"🎛️ **Your {cmd.capitalize()} set to `{val}`!**\n"
        f"🔗 Applied to `{len(ws.targets)}` active target chats."
    )


@bot.on_message(filters.command("echo") & access_filter)
async def echo_cmd(client: Client, message: Message):
    ws = MANAGER.get_ws(message.from_user.id)
    if len(message.command) < 2:
        return await message.reply_text("❌ **Usage:** `/echo <delay/decay>` or `/echo off`")
    arg = message.command[1].lower()
    if arg == "off":
        ws.effects["echo"] = None
        await ws.reload_streams()
        return await message.reply_text("✅ **Your Echo effect is now OFF!**")
    try:
        parts = arg.split("/")
        delay = int(parts[0])
        decay = min(max(float(parts[1]) if len(parts) > 1 else 0.5, 0.1), 0.99)
        ws.effects["echo"] = (delay, decay)
        await ws.reload_streams()
        await message.reply_text(f"✅ **Your Echo set to `{delay}ms / {decay}`!**")
    except Exception:
        await message.reply_text("❌ **Invalid format! Example:** `/echo 500/0.6`")


@bot.on_message(filters.command("effects") & access_filter)
async def effects_cmd(client: Client, message: Message):
    ws = MANAGER.get_ws(message.from_user.id)
    fx = ws.effects
    echo_str = f"{fx['echo'][0]}ms/{fx['echo'][1]}" if fx["echo"] else "Off"
    await message.reply_text(
        f"🎛️ **Your Active Effects**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"• **Level:** `{fx['level']}`\n"
        f"• **Bass:** `{fx['bass']} dB`\n"
        f"• **Treble:** `{fx['treble']} dB`\n"
        f"• **Gain:** `{fx['gain']} dB`\n"
        f"• **Overdrive:** `{fx['overdrive']}`\n"
        f"• **Echo:** `{echo_str}`"
    )


@bot.on_message(filters.command("reset") & access_filter)
async def reset_cmd(client: Client, message: Message):
    ws = MANAGER.get_ws(message.from_user.id)
    for k in ws.effects:
        ws.effects[k] = 1.0 if k == "level" else (None if k == "echo" else 0.0)
    await ws.reload_streams()
    await message.reply_text("🔄 **All your audio effects have been reset!**")


# ==================== UTILITIES & DIRECT OWNER COMMANDS ====================
@bot.on_message(filters.command("panel") & access_filter)
async def panel_cmd(client: Client, message: Message):
    uid = message.from_user.id
    ws = MANAGER.get_ws(uid)
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔇 Mute", callback_data="ctl_mute"),
            InlineKeyboardButton("🔊 Unmute", callback_data="ctl_unmute"),
        ],
        [
            InlineKeyboardButton("🔄 Reset FX", callback_data="ctl_reset"),
            InlineKeyboardButton("🛑 Leave All", callback_data="ctl_leaveall"),
        ],
        [
            InlineKeyboardButton("📱 My Accounts", callback_data="usr_accounts"),
        ],
    ])
    status = "🟢 Live" if ws.recording else "🔴 Idle"
    await message.reply_text(
        f"🎛️ **Your Personal Control Panel**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 **Source:** `{ws.source_chat or 'Not Set'}`\n"
        f"🔗 **Active Targets:** `{len(ws.targets)}/{len(ws.broadcaster_users)}`\n"
        f"📊 **Status:** {status}",
        reply_markup=kb,
    )


@bot.on_message(filters.command("list") & access_filter)
async def list_cmd(client: Client, message: Message):
    ws = MANAGER.get_ws(message.from_user.id)
    if not ws.targets:
        return await message.reply_text("📭 **You have no active target chats!**")
    lines = [f"• `{t_id}` (Your Broadcaster #{b_idx+1})" for t_id, b_idx in ws.targets.items()]
    await message.reply_text("🔗 **Your Forwarding List:**\n" + "\n".join(lines))


@bot.on_message(filters.command(["status", "stats"]) & access_filter)
async def status_cmd(client: Client, message: Message):
    ws = MANAGER.get_ws(message.from_user.id)
    cpu = psutil.cpu_percent()
    ram = psutil.virtual_memory().percent
    await message.reply_text(
        f"📊 **Your Workspace & System Status**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎙️ **Your Recorder:** `{'✅ Ready' if ws.recorder_user else '❌ Not Set'}`\n"
        f"📡 **Your Broadcasters:** `{len(ws.broadcaster_users)}`\n"
        f"🎯 **Source Chat:** `{ws.source_chat or 'Not Set'}`\n"
        f"🔗 **Forwarding:** `{len(ws.targets)} chats`\n"
        f"💻 **Server CPU:** `{cpu}%` | 🧠 **RAM:** `{ram}%`"
    )


@bot.on_message(filters.command("id") & access_filter)
async def id_cmd(client: Client, message: Message):
    chat = message.chat
    chat_type = str(chat.type).split(".")[-1].capitalize()
    await message.reply_text(
        f"🆔 **Chat Information**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 **Chat ID:** `{chat.id}`\n"
        f"👤 **Your User ID:** `{message.from_user.id}`\n"
        f"📊 **Type:** {chat_type}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )


@bot.on_message(filters.command("ping") & access_filter)
async def ping_cmd(client: Client, message: Message):
    t0 = time.perf_counter()
    m = await message.reply_text("🏓 **Pinging...**")
    ms = (time.perf_counter() - t0) * 1000
    await m.edit_text(f"🏓 **Pong!** `{ms:.2f} ms`")


@bot.on_message(filters.command(["ban", "unban", "approve", "disapprove"]) & filters.user(config.OWNER_ID))
async def owner_quick_cmds(client: Client, message: Message):
    cmd = message.command[0].lower()
    uid = (
        message.reply_to_message.from_user.id
        if message.reply_to_message and message.reply_to_message.from_user
        else (int(message.command[1]) if len(message.command) > 1 else None)
    )
    if not uid or uid == config.OWNER_ID:
        return await message.reply_text(f"❌ **Usage:** `/{cmd} <user_id>`")

    if cmd == "approve":
        MANAGER.banned_users.discard(uid)
        MANAGER.approved_users.add(uid)
        await message.reply_text(f"✅ **Approved `{uid}`!**")
    elif cmd == "disapprove":
        MANAGER.approved_users.discard(uid)
        if uid in MANAGER.workspaces:
            await MANAGER.workspaces[uid].leave_all()
        await message.reply_text(f"❌ **Removed access from `{uid}`!**")
    elif cmd == "ban":
        MANAGER.approved_users.discard(uid)
        MANAGER.banned_users.add(uid)
        if uid in MANAGER.workspaces:
            await MANAGER.workspaces[uid].leave_all()
        await message.reply_text(f"🚫 **Banned `{uid}`!**")
    elif cmd == "unban":
        MANAGER.banned_users.discard(uid)
        await message.reply_text(f"🟢 **Unbanned `{uid}`!**")
    MANAGER.sync_db()


@bot.on_message(filters.command("restart") & filters.user(config.OWNER_ID))
async def restart_cmd(client: Client, message: Message):
    await message.reply_text("🔄 **Restarting Multi-Tenant Bot...**")
    for ws in MANAGER.workspaces.values():
        await ws.leave_all()
    os.execv(sys.executable, [sys.executable] + sys.argv)


# ==================== BOOTSTRAP SAVED USER WORKSPACES ====================
async def bootstrap():
    print("🚀 Starting Nobi Multi-Tenant Fight Bot...")
    await bot.start()

    users_data = DB.get("users", {})
    for uid_str, u_data in users_data.items():
        uid = int(uid_str)
        if uid in MANAGER.banned_users:
            continue
        ws = MANAGER.get_ws(uid)
        if ws.recorder_session:
            print(f"🎙️ Restoring Recorder for User {uid}...")
            await ws.set_recorder(ws.recorder_session, save=False)
        for b_sess in list(ws.broadcaster_sessions):
            print(f"📡 Restoring Broadcaster for User {uid}...")
            await ws.add_broadcaster(b_sess, save=False)

    print("✅ Multi-Tenant Bot is LIVE! Every user can now login & stream from Bot DM.")
    await asyncio.Event().wait()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(bootstrap())
