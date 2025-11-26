import json
import os
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

DB_FILE = "database.json"
_db = {}

def get_default_db() -> Dict[str, Any]:
    return {
        "participants": {},
        "bonus_roles": {},
        "hashtag": "",
        "tag": {"enabled": False, "text": "", "quantity": 1},
        "inscricao_channel": None,
        "button_message_id": None,
        "inscricoes_closed": False,
        "blacklist": {},
        "chat_lock": {"enabled": False, "channel_id": None},
        "moderators": [],
    }

def load_db():
    global _db
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r', encoding='utf-8') as f:
                _db = json.load(f)
                logger.info("📥 Dados carregados do arquivo JSON")
                return _db
        except Exception as e:
            logger.error(f"Erro ao carregar JSON: {e}")
    
    _db = get_default_db()
    save_db(_db)
    return _db

def save_db(data=None):
    global _db
    
    if data:
        _db = data
    else:
        data = _db
    
    try:
        with open(DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.debug("💾 Dados salvos em JSON")
    except Exception as e:
        logger.error(f"Erro ao salvar JSON: {e}")

def get_all_data():
    return _db

def add_participant(user_id, first_name, last_name, tickets, message_id=None):
    _db["participants"][str(user_id)] = {
        "first_name": first_name,
        "last_name": last_name,
        "tickets": tickets,
        "message_id": message_id
    }
    save_db()

def get_participant(user_id):
    return _db["participants"].get(str(user_id))

def get_all_participants():
    return _db["participants"]

def is_registered(user_id):
    return str(user_id) in _db["participants"]

def remove_participant(user_id):
    if str(user_id) in _db["participants"]:
        del _db["participants"][str(user_id)]
        save_db()
        return True
    return False

def update_tickets(user_id, tickets):
    if str(user_id) in _db["participants"]:
        _db["participants"][str(user_id)]["tickets"] = tickets
        save_db()

def is_name_taken(first_name, last_name):
    full_name_lower = f"{first_name} {last_name}".lower()
    for participant in _db["participants"].values():
        stored_name = f"{participant['first_name']} {participant['last_name']}".lower()
        if stored_name == full_name_lower:
            return True
    return False

def clear_participants():
    _db["participants"] = {}
    save_db()

def add_bonus_role(role_id, quantity, abbreviation):
    _db["bonus_roles"][str(role_id)] = {
        "quantity": quantity,
        "abbreviation": abbreviation
    }
    save_db()

def get_bonus_roles():
    return _db["bonus_roles"]

def remove_bonus_role(role_id):
    if str(role_id) in _db["bonus_roles"]:
        del _db["bonus_roles"][str(role_id)]
        save_db()
        return True
    return False

def set_hashtag(hashtag):
    _db["hashtag"] = hashtag
    save_db()

def get_hashtag():
    return _db["hashtag"]

def is_hashtag_locked():
    return False

def set_tag(enabled, text=None, quantity=1):
    _db["tag"] = {
        "enabled": enabled,
        "text": text if text else "",
        "quantity": quantity
    }
    save_db()

def get_tag():
    return _db["tag"]

def set_inscricao_channel(channel_id):
    _db["inscricao_channel"] = channel_id
    save_db()

def get_inscricao_channel():
    return _db["inscricao_channel"]

def set_button_message_id(message_id):
    _db["button_message_id"] = message_id
    save_db()

def add_button_message_id(message_id):
    current = _db.get("button_message_id")
    if isinstance(current, list):
        if message_id not in current:
            current.append(message_id)
    elif current:
        _db["button_message_id"] = [current, message_id]
    else:
        _db["button_message_id"] = message_id
    save_db()

def get_button_message_id():
    return _db["button_message_id"]

def set_inscricoes_closed(closed):
    _db["inscricoes_closed"] = closed
    save_db()

def get_inscricoes_closed():
    return _db["inscricoes_closed"]

def add_to_blacklist(user_id, reason, banned_by=None):
    _db["blacklist"][str(user_id)] = {
        "reason": reason,
        "banned_by": banned_by
    }
    save_db()

def remove_from_blacklist(user_id):
    if str(user_id) in _db["blacklist"]:
        del _db["blacklist"][str(user_id)]
        save_db()
        return True
    return False

def get_blacklist():
    return _db["blacklist"]

def is_blacklisted(user_id):
    return str(user_id) in _db["blacklist"]

def set_chat_lock(enabled, channel_id=None):
    _db["chat_lock"] = {
        "enabled": enabled,
        "channel_id": channel_id
    }
    save_db()

def get_chat_lock():
    return _db["chat_lock"]

def add_moderator(user_id):
    if str(user_id) not in _db["moderators"]:
        _db["moderators"].append(str(user_id))
        save_db()

def remove_moderator(user_id):
    if str(user_id) in _db["moderators"]:
        _db["moderators"].remove(str(user_id))
        save_db()
        return True
    return False

def get_moderators():
    return _db["moderators"]

def is_moderator(user_id):
    return str(user_id) in _db["moderators"]

def get_statistics():
    participants = _db["participants"]
    bonus_roles = _db["bonus_roles"]
    
    total_participants = len(participants)
    total_tickets = 0
    participants_with_tag = 0
    tickets_by_role = {}
    
    for participant in participants.values():
        tickets = participant.get("tickets", {})
        
        total_tickets += sum(int(t) if isinstance(t, (int, str)) else t.get("quantity", 1) 
                           for t in tickets.values() if t)
        
        if tickets.get("tag") or tickets.get("manual_tag"):
            participants_with_tag += 1
        
        roles = tickets.get("roles", {})
        for role_id, role_info in roles.items():
            if role_id not in tickets_by_role:
                tickets_by_role[role_id] = {
                    "count": 0,
                    "total_tickets": 0,
                    "abbreviation": bonus_roles.get(role_id, {}).get("abbreviation", "?")
                }
            tickets_by_role[role_id]["count"] += 1
            tickets_by_role[role_id]["total_tickets"] += int(role_info.get("quantity", 1))
    
    return {
        "total_participants": total_participants,
        "total_tickets": total_tickets,
        "participants_with_tag": participants_with_tag,
        "tickets_by_role": tickets_by_role,
        "blacklist_count": len(_db["blacklist"])
    }

def add_manual_tag(user_id, quantity=1):
    if str(user_id) in _db["participants"]:
        participant = _db["participants"][str(user_id)]
        if "tickets" not in participant:
            participant["tickets"] = {}
        
        current_manual_tag = participant["tickets"].get("manual_tag", 0)
        participant["tickets"]["manual_tag"] = int(current_manual_tag) + int(quantity)
        save_db()

def clear_all():
    global _db
    _db = get_default_db()
    save_db()

_db = load_db()
