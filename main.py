import asyncio
import psutil
from pyrogram import Client, filters
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import SessionPasswordNeeded
import pyromod

import config
from database.mongo import users_col, accounts_col, forwarding_configs
from keyboards.menus import main_menu_kb, owner_menu_kb, effects_menu_kb
from services.encryption import encrypt_session, decrypt_session
from media.audio_bridge import ActiveRelay, active_relays

bot = Client("NexaVCForwarder", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.BOT_TOKEN)

# --- AUTHORIZATION GUARD ---
async def check_auth(user_id: int) -> bool:
    if user_id == config.OWNER_ID: return True
    user = await users_col.find_one({"user_id": user_id})
    return bool(user and user.get("approved") and not user.get("banned"))

@bot.on_message(filters.command("start"))
async def start_handler(client: Client, message: Message):
    uid = message.from_user.id
    if await check_auth(uid):
        await message.reply("NEXA LIVE VC FORWARDER 🟢", reply_markup=main_menu_kb(uid == config.OWNER_ID))
    else:
        await users_col.update_one({"user_id": uid}, {"$setOnInsert": {"user_id": uid, "approved": False, "banned": False}}, upsert=True)
        txt = (f"⛔ ACCESS RESTRICTED\n━━━━━━━━━━━━━━━━━━━━\n"
               f"Hello {message.from_user.mention},\n\nYou need approval to use this bot.\n"
               f"📩 Request forwarded to admins.\n\nYou will be notified once approved.")
        await message.reply(txt)
        await client.send_message(config.OWNER_ID, f"📩 Access Request: {message.from_user.mention} (`{uid}`)", 
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Approve", callback_data=f"apprv_{uid}"), InlineKeyboardButton("🚫 Ban", callback_data=f"ban_{uid}")]]))

# --- OWNER CONTROLS ---
@bot.on_callback_query(filters.regex(r"^(apprv|ban)_"))
async def owner_approve_ban(client: Client, query: CallbackQuery):
    if query.from_user.id != config.OWNER_ID: return await query.answer("Access Denied", show_alert=True)
    action, uid = query.data.split("_")
    uid = int(uid)
    if action == "apprv":
        await users_col.update_one({"user_id": uid}, {"$set": {"approved": True, "banned": False}})
        await query.message.edit_text(f"{query.message.text}\n\n✅ APPROVED")
        await client.send_message(uid, "🎉 ACCESS APPROVED! Type /start to manage your VCs.")
    elif action == "ban":
        await users_col.update_one({"user_id": uid}, {"$set": {"approved": False, "banned": True}})
        await query.message.edit_text(f"{query.message.text}\n\n🚫 BANNED")

# --- ACCOUNT MANAGEMENT ---
@bot.on_callback_query(filters.regex(r"^add_(record|broadcast)$"))
async def add_account(client: Client, query: CallbackQuery):
    if not await check_auth(query.from_user.id): return await query.answer("Unauthorized", show_alert=True)
    role = query.data.split("_")[1]
    acc_count = await accounts_col.count_documents({"user_id": query.from_user.id})
    if acc_count >= 5: return await query.answer("Account limit reached (5).", show_alert=True)
    
    method = await client.ask(query.message.chat.id, "Select Method:\n1. OTP Login\n2. String Session")
    if method.text.strip() == "1":
        phone = await client.ask(query.message.chat.id, "Send phone number (e.g. +123456789):")
        temp = Client(f"temp_{query.from_user.id}", api_id=config.API_ID, api_hash=config.API_HASH, in_memory=True)
        await temp.connect()
        sc = await temp.send_code(phone.text.strip())
        otp = await client.ask(query.message.chat.id, "Send OTP (with spaces):")
        try:
            await temp.sign_in(phone.text.strip(), sc.phone_code_hash, otp.text.replace(" ", ""))
        except SessionPasswordNeeded:
            pwd = await client.ask(query.message.chat.id, "Send 2FA password:")
            await temp.check_password(pwd.text)
        session_str = await temp.export_session_string()
        await temp.disconnect()
    else:
        ss_msg = await client.ask(query.message.chat.id, "Send String Session:")
        session_str = ss_msg.text.strip()
    
    await accounts_col.update_one(
        {"user_id": query.from_user.id, "role": role},
        {"$set": {"session": encrypt_session(session_str), "role": role}},
        upsert=True
    )
    await query.message.reply(f"✅ {role.capitalize()} account saved securely.")

# --- FORWARDING CONTROLS ---
@bot.on_callback_query(filters.regex("^start_fwd$"))
async def start_forwarding(client: Client, query: CallbackQuery):
    uid = query.from_user.id
    if not await check_auth(uid): return
    
    rec_acc = await accounts_col.find_one({"user_id": uid, "role": "record"})
    bc_acc = await accounts_col.find_one({"user_id": uid, "role": "broadcast"})
    if not rec_acc or not bc_acc: return await query.answer("You need both a Record and Broadcast account set up!", show_alert=True)
    
    src = await client.ask(query.message.chat.id, "Send Source Group Chat ID (e.g., -100xxx):")
    dst = await client.ask(query.message.chat.id, "Send Destination Group Chat ID (e.g., -100xxx):")
    
    fx = {"volume": 100, "bass": 0, "treble": 0, "gain": 0, "overdrive": 0, "echo": None}
    await forwarding_configs.update_one({"user_id": uid}, {"$set": {"source": int(src.text), "dest": int(dst.text), "fx": fx}}, upsert=True)
    
    relay = ActiveRelay(uid)
    await relay.initialize(decrypt_session(rec_acc["session"]), decrypt_session(bc_acc["session"]))
    await relay.start_relay(int(src.text), int(dst.text), fx)
    active_relays[uid] = relay
    
    await query.message.reply("✅ Live VC Forwarding Started!")

@bot.on_callback_query(filters.regex("^menu_effects$"))
async def effects_menu(client: Client, query: CallbackQuery):
    uid = query.from_user.id
    conf = await forwarding_configs.find_one({"user_id": uid})
    fx = conf.get("fx", {}) if conf else {}
    await query.message.edit_text("🎛️ Configure Real-Time Audio Effects", reply_markup=effects_menu_kb(fx))

@bot.on_callback_query(filters.regex(r"^fx_(volume|bass|treble|gain|overdrive|echo|mute|unmute|reset)$"))
async def apply_effect(client: Client, query: CallbackQuery):
    uid = query.from_user.id
    effect = query.data.split("_")[1]
    conf = await forwarding_configs.find_one({"user_id": uid})
    fx = conf.get("fx", {}) if conf else {}
    
    if effect in ["mute", "unmute"]:
        if uid in active_relays:
            if effect == "mute": await active_relays[uid].broadcaster_call.mute_stream(conf["dest"])
            else: await active_relays[uid].broadcaster_call.unmute_stream(conf["dest"])
        return await query.answer(f"Stream {effect}d")
    elif effect == "reset":
        fx = {"volume": 100, "bass": 0, "treble": 0, "gain": 0, "overdrive": 0, "echo": None}
    else:
        val = await client.ask(query.message.chat.id, f"Send new value for {effect}:")
        fx[effect] = val.text if effect == "echo" else float(val.text)
        
    await forwarding_configs.update_one({"user_id": uid}, {"$set": {"fx": fx}}, upsert=True)
    if uid in active_relays: await active_relays[uid].reload_effects(conf["dest"], fx)
    await query.message.edit_reply_markup(effects_menu_kb(fx))

@bot.on_callback_query(filters.regex("^stop_fwd$"))
async def stop_forwarding(client: Client, query: CallbackQuery):
    uid = query.from_user.id
    if uid in active_relays:
        await active_relays[uid].stop()
        del active_relays[uid]
        await query.answer("Forwarding Stopped", show_alert=True)
    else:
        await query.answer("No active relay", show_alert=True)

if __name__ == "__main__":
    print("Nexa Live VC Forwarder starting...")
    bot.run()
