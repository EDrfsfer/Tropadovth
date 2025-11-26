import json
import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from pymongo import MongoClient
import subprocess
import psycopg2
from psycopg2.extras import Json

logger = logging.getLogger(__name__)

# Conexão com MongoDB
MONGODB_URI = os.getenv("MONGODB_URI")
client = MongoClient(MONGODB_URI)
db_mongo = client["discord_bot"]

# PostgreSQL
DATABASE_URL = os.getenv("DATABASE_URL")

DB_FILE = "db.json"  # Caminho do arquivo JSON

# ========== DATABASE ABSTRACTION ==========

def get_connection():
    """Cria conexão com PostgreSQL"""
    return psycopg2.connect(DATABASE_URL)

def init_db():
    """Cria tabela se não existir"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_data (
                id INTEGER PRIMARY KEY DEFAULT 1,
                data JSONB NOT NULL
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Erro ao inicializar DB: {e}")

def load_db() -> Dict[str, Any]:
    """Carrega dados do PostgreSQL"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT data FROM bot_data WHERE id = 1")
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        if result:
            return json.loads(result[0])
    except Exception as e:
        logger.error(f"Erro ao carregar PostgreSQL: {e}")
    
    return get_default_db()

def save_db(data: Dict[str, Any]):
    """Salva dados no PostgreSQL"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO bot_data (id, data) VALUES (1, %s) ON CONFLICT (id) DO UPDATE SET data = %s",
            (Json(data), Json(data))
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Erro ao salvar PostgreSQL: {e}")

# Inicializa banco no startup
init_db()

# ========== LOAD/SAVE ==========

def load_db_mongo() -> Dict[str, Any]:
    """Carrega dados do MongoDB"""
    try:
        data = db_mongo.config.find_one({"_id": "main"})
        if data:
            data.pop("_id", None)  # Remove o _id do MongoDB
            return data
    except Exception as e:
        logger.error(f"Erro ao carregar MongoDB: {e}")
    
    return get_default_db()

def save_db_mongo(data: Dict[str, Any]):
    """Salva dados no arquivo JSON e faz push para GitHub"""
    try:
        with open(DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        # Tenta fazer commit automático (opcional)
        try:
            subprocess.run(
                ["git", "add", DB_FILE],
                capture_output=True,
                timeout=5
            )
            subprocess.run(
                ["git", "commit", "-m", f"Auto-save database - {datetime.utcnow().isoformat()}"],
                capture_output=True,
                timeout=5
            )
            subprocess.run(
                ["git", "push"],
                capture_output=True,
                timeout=10
            )
            logger.info("Database sincronizado com GitHub")
        except Exception as e:
            logger.warning(f"Não foi possível fazer push automático: {e}")
            
    except Exception as e:
        logger.error(f"Erro ao salvar database: {e}")

def get_default_db() -> Dict[str, Any]:
    """Estrutura padrão do banco"""
    return {
        "participants": {},
        "bonus_roles": {},
        "hashtag": {"value": None, "locked": False},
        "tag": {"enabled": False, "text": None, "quantity": 1},
        "inscricao_channel": None,
        "button_message_id": None,
        "blacklist": {},
        "chat_lock": {"enabled": False, "channel_id": None},
        "moderators": [],
        "inscricoes_closed": False
    }

# Carregar DB na inicialização
_db = load_db()

def _save():
    """Helper para salvar DB"""
    global _db
    save_db(_db)

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
