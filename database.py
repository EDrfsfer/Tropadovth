from pymongo import MongoClient
import json
import os
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)

# Tenta usar MongoDB primeiro, fallback para JSON local
MONGODB_URI = os.getenv("MONGODB_URI")
USE_MONGO = bool(MONGODB_URI)

# Variável global para dados em memória
_db_cache = {}

if USE_MONGO:
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        # Testa a conexão
        client.server_info()
        db_mongo = client["discord_bot"]
        collection = db_mongo["bot_data"]
        logger.info("✅ MongoDB conectado com sucesso!")
    except Exception as e:
        logger.warning(f"⚠️ MongoDB não disponível, usando JSON local: {e}")
        USE_MONGO = False
        db_mongo = None

DB_FILE = "database.json"

def get_default_db():
    """Retorna estrutura padrão do banco de dados"""
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
    """Carrega dados do MongoDB ou arquivo JSON local"""
    global _db_cache
    
    if USE_MONGO and db_mongo:
        try:
            data = collection.find_one({"_id": "main"})
            if data:
                data.pop("_id", None)
                _db_cache = data
                logger.info("📥 Dados carregados do MongoDB")
                return data
        except Exception as e:
            logger.warning(f"Erro ao carregar MongoDB: {e}")
    
    # Fallback para arquivo JSON
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r', encoding='utf-8') as f:
                _db_cache = json.load(f)
                logger.info("📥 Dados carregados do arquivo JSON")
                return _db_cache
        except Exception as e:
            logger.error(f"Erro ao carregar JSON: {e}")
    
    # Cria novo banco padrão
    _db_cache = get_default_db()
    save_db(_db_cache)
    return _db_cache

def save_db(data=None):
    """Salva dados no MongoDB E no arquivo JSON (backup local)"""
    global _db_cache
    
    if data:
        _db_cache = data
    else:
        data = _db_cache
    
    # Salva em JSON sempre (backup local)
    try:
        with open(DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.debug("💾 Dados salvos em JSON local")
    except Exception as e:
        logger.error(f"Erro ao salvar JSON: {e}")
    
    # Tenta salvar no MongoDB também
    if USE_MONGO and db_mongo:
        try:
            data_to_save = data.copy()
            collection.replace_one(
                {"_id": "main"},
                {"_id": "main", **data_to_save},
                upsert=True
            )
            logger.debug("☁️ Dados salvos no MongoDB")
        except Exception as e:
            logger.warning(f"Aviso ao salvar MongoDB: {e}")

# ========== HASHTAG ==========

def set_hashtag(hashtag: str):
    global _db
    _db["hashtag"]["value"] = hashtag
    _save()

def get_hashtag() -> Optional[str]:
    global _db
    return _db.get("hashtag", {}).get("value")

def is_hashtag_locked() -> bool:
    global _db
    return _db.get("hashtag", {}).get("locked", False)

def lock_hashtag():
    global _db
    _db["hashtag"]["locked"] = True
    _save()

def unlock_hashtag():
    global _db
    _db["hashtag"]["locked"] = False
    _save()

# ========== TAG ==========

def set_tag(enabled: bool, text: Optional[str] = None, quantity: int = 1):
    global _db
    _db["tag"]["enabled"] = enabled
    if text:
        _db["tag"]["text"] = text
    if quantity > 0:
        _db["tag"]["quantity"] = quantity
    _save()

def get_tag() -> Dict[str, Any]:
    global _db
    return _db.get("tag", {"enabled": False, "text": None, "quantity": 1})

# ========== BONUS ROLES ==========

def add_bonus_role(role_id: int, quantity: int, abbreviation: str):
    global _db
    _db["bonus_roles"][str(role_id)] = {
        "quantity": quantity,
        "abbreviation": abbreviation
    }
    _save()

def get_bonus_roles() -> Dict[int, Dict[str, Any]]:
    global _db
    roles = {}
    for role_id, data in _db.get("bonus_roles", {}).items():
        try:
            roles[int(role_id)] = data
        except ValueError:
            continue
    return roles

def remove_bonus_role(role_id: int) -> bool:
    global _db
    role_id_str = str(role_id)
    if role_id_str in _db.get("bonus_roles", {}):
        del _db["bonus_roles"][role_id_str]
        _save()
        return True
    return False

# ========== PARTICIPANTS ==========

def add_participant(user_id: int, first_name: str, last_name: str, tickets: Dict[str, Any], message_id: int):
    global _db
    _db["participants"][str(user_id)] = {
        "first_name": first_name,
        "last_name": last_name,
        "tickets": tickets,
        "message_id": message_id,
        "timestamp": datetime.utcnow().isoformat()
    }
    _save()

def get_participant(user_id: int) -> Optional[Dict[str, Any]]:
    global _db
    user_id_str = str(user_id)
    if user_id_str in _db.get("participants", {}):
        return _db["participants"][user_id_str]
    return None

def get_all_participants() -> Dict[int, Dict[str, Any]]:
    global _db
    return _db.get("participants", {})

def is_registered(user_id: int) -> bool:
    global _db
    return str(user_id) in _db.get("participants", {})

def is_name_taken(first_name: str, last_name: str) -> bool:
    global _db
    search_name = f"{first_name.lower()} {last_name.lower()}"
    for data in _db.get("participants", {}).values():
        participant_name = f"{data['first_name'].lower()} {data['last_name'].lower()}"
        if participant_name == search_name:
            return True
    return False

def update_tickets(user_id: int, tickets: Dict[str, Any]):
    global _db
    user_id_str = str(user_id)
    if user_id_str in _db.get("participants", {}):
        _db["participants"][user_id_str]["tickets"] = tickets
        _save()

def remove_participant(user_id: int) -> bool:
    global _db
    user_id_str = str(user_id)
    if user_id_str in _db.get("participants", {}):
        del _db["participants"][user_id_str]
        _save()
        return True
    return False

def add_manual_tag(user_id: int, quantity: int):
    global _db
    user_id_str = str(user_id)
    if user_id_str in _db.get("participants", {}):
        if "manual_tag" not in _db["participants"][user_id_str]["tickets"]:
            _db["participants"][user_id_str]["tickets"]["manual_tag"] = 0
        _db["participants"][user_id_str]["tickets"]["manual_tag"] += quantity
        _save()

def clear_participants():
    global _db
    _db["participants"] = {}
    _save()

def clear_all():
    global _db
    _db = get_default_db()
    _save()

# ========== BLACKLIST ==========

def add_to_blacklist(user_id: int, reason: str, banned_by: int):
    global _db
    _db["blacklist"][str(user_id)] = {
        "reason": reason,
        "banned_by": banned_by,
        "timestamp": datetime.utcnow().isoformat()
    }
    _save()

def remove_from_blacklist(user_id: int) -> bool:
    global _db
    user_id_str = str(user_id)
    if user_id_str in _db.get("blacklist", {}):
        del _db["blacklist"][user_id_str]
        _save()
        return True
    return False

def get_blacklist() -> Dict[int, Dict[str, Any]]:
    global _db
    return _db.get("blacklist", {})

def is_blacklisted(user_id: int) -> bool:
    global _db
    return str(user_id) in _db.get("blacklist", {})

# ========== INSCRICAO CHANNEL ==========

def set_inscricao_channel(channel_id: int):
    global _db
    _db["inscricao_channel"] = channel_id
    _save()

def get_inscricao_channel() -> Optional[int]:
    global _db
    return _db.get("inscricao_channel")

# ========== BUTTON MESSAGE ==========

def set_button_message_id(message_id: int):
    global _db
    _db["button_message_id"] = message_id
    _save()

def add_button_message_id(message_id: int):
    """Adiciona um message_id à lista (permite múltiplas mensagens)"""
    global _db
    if not isinstance(_db.get("button_message_id"), list):
        if _db.get("button_message_id"):
            _db["button_message_id"] = [_db["button_message_id"]]
        else:
            _db["button_message_id"] = []
    if message_id not in _db["button_message_id"]:
        _db["button_message_id"].append(message_id)
    _save()

def get_button_message_id():
    global _db
    return _db.get("button_message_id")

# ========== CHAT LOCK ==========

def set_chat_lock(enabled: bool, channel_id: Optional[int] = None):
    global _db
    _db["chat_lock"]["enabled"] = enabled
    if channel_id:
        _db["chat_lock"]["channel_id"] = channel_id
    _save()

def get_chat_lock() -> Dict[str, Any]:
    global _db
    return _db.get("chat_lock", {"enabled": False, "channel_id": None})

# ========== MODERATORS ==========

def add_moderator(user_id: int):
    global _db
    if user_id not in _db.get("moderators", []):
        _db["moderators"].append(user_id)
    _save()

def remove_moderator(user_id: int) -> bool:
    global _db
    if user_id in _db.get("moderators", []):
        _db["moderators"].remove(user_id)
        _save()
        return True
    return False

def is_moderator(user_id: int) -> bool:
    global _db
    return user_id in _db.get("moderators", [])

def get_moderators() -> list:
    global _db
    return _db.get("moderators", [])

# ========== INSCRICOES CLOSED ==========

def set_inscricoes_closed(closed: bool):
    global _db
    _db["inscricoes_closed"] = closed
    _save()

def get_inscricoes_closed() -> bool:
    global _db
    return _db.get("inscricoes_closed", False)

# ========== STATISTICS ==========

def get_statistics() -> Dict[str, Any]:
    global _db
    participants = _db.get("participants", {})
    
    total_participants = len(participants)
    total_tickets = 0
    participants_with_tag = 0
    tickets_by_role = {}
    
    for data in participants.values():
        tickets = data.get("tickets", {})
        
        # Total de fichas
        base = tickets.get("base", 1)
        total_tickets += base
        
        # Fichas de TAG
        tag_qty = tickets.get("tag", 0) + tickets.get("manual_tag", 0)
        total_tickets += tag_qty
        if tag_qty > 0:
            participants_with_tag += 1
        
        # Fichas por cargo
        roles = tickets.get("roles", {})
        for role_id, role_info in roles.items():
            if role_id not in tickets_by_role:
                tickets_by_role[role_id] = {
                    "count": 0,
                    "total_tickets": 0,
                    "abbreviation": role_info.get("abbreviation", "")
                }
            role_qty = role_info.get("quantity", 1)
            tickets_by_role[role_id]["count"] += 1
            tickets_by_role[role_id]["total_tickets"] += role_qty
            total_tickets += role_qty
    
    return {
        "total_participants": total_participants,
        "total_tickets": total_tickets,
        "participants_with_tag": participants_with_tag,
        "tickets_by_role": tickets_by_role,
        "blacklist_count": len(_db.get("blacklist", {}))
    }
