import json
import os
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)

_db_cache = {}
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
    """Carrega dados do arquivo JSON local"""
    global _db_cache
    
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r', encoding='utf-8') as f:
                _db_cache = json.load(f)
                logger.info("📥 Dados carregados do arquivo JSON")
                return _db_cache
        except Exception as e:
            logger.error(f"Erro ao carregar JSON: {e}")
    
    _db_cache = get_default_db()
    save_db(_db_cache)
    return _db_cache

def save_db(data=None):
    """Salva dados no arquivo JSON"""
    global _db_cache
    
    if data:
        _db_cache = data
    else:
        data = _db_cache
    
    try:
        with open(DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.debug("💾 Dados salvos em JSON")
    except Exception as e:
        logger.error(f"Erro ao salvar JSON: {e}")

def get_all_data():
    """Retorna todos os dados em memória"""
    return _db_cache

def add_participant(user_id, first_name, last_name, tickets, message_id=None):
    """Adiciona um participante"""
    _db_cache["participants"][str(user_id)] = {
        "first_name": first_name,
        "last_name": last_name,
        "tickets": tickets,
        "message_id": message_id
    }
    save_db()

def get_participant(user_id):
    """Retorna dados de um participante"""
    return _db_cache["participants"].get(str(user_id))

def get_all_participants():
    """Retorna todos os participantes"""
    return _db_cache["participants"]

def is_registered(user_id):
    """Verifica se o usuário está inscrito"""
    return str(user_id) in _db_cache["participants"]

def remove_participant(user_id):
    """Remove um participante"""
    if str(user_id) in _db_cache["participants"]:
        del _db_cache["participants"][str(user_id)]
        save_db()
        return True
    return False

def update_tickets(user_id, tickets):
    """Atualiza as fichas de um participante"""
    if str(user_id) in _db_cache["participants"]:
        _db_cache["participants"][str(user_id)]["tickets"] = tickets
        save_db()

def is_name_taken(first_name, last_name):
    """Verifica se o nome já foi registrado"""
    full_name_lower = f"{first_name} {last_name}".lower()
    for participant in _db_cache["participants"].values():
        stored_name = f"{participant['first_name']} {participant['last_name']}".lower()
        if stored_name == full_name_lower:
            return True
    return False

def clear_participants():
    """Limpa todos os participantes"""
    _db_cache["participants"] = {}
    save_db()

def add_bonus_role(role_id, quantity, abbreviation):
    """Adiciona um cargo bônus"""
    _db_cache["bonus_roles"][str(role_id)] = {
        "quantity": quantity,
        "abbreviation": abbreviation
    }
    save_db()

def get_bonus_roles():
    """Retorna todos os cargos bônus"""
    return _db_cache["bonus_roles"]

def remove_bonus_role(role_id):
    """Remove um cargo bônus"""
    if str(role_id) in _db_cache["bonus_roles"]:
        del _db_cache["bonus_roles"][str(role_id)]
        save_db()
        return True
    return False

def set_hashtag(hashtag):
    """Define a hashtag obrigatória"""
    _db_cache["hashtag"] = hashtag
    save_db()

def get_hashtag():
    """Retorna a hashtag configurada"""
    return _db_cache["hashtag"]

def is_hashtag_locked():
    """Verifica se a hashtag está bloqueada"""
    return False

def set_tag(enabled, text=None, quantity=1):
    """Configura a TAG do servidor"""
    _db_cache["tag"] = {
        "enabled": enabled,
        "text": text if text else "",
        "quantity": quantity
    }
    save_db()

def get_tag():
    """Retorna configuração da TAG"""
    return _db_cache["tag"]

def set_inscricao_channel(channel_id):
    """Define o canal de inscrições"""
    _db_cache["inscricao_channel"] = channel_id
    save_db()

def get_inscricao_channel():
    """Retorna o canal de inscrições"""
    return _db_cache["inscricao_channel"]

def set_button_message_id(message_id):
    """Define o ID da mensagem do botão"""
    _db_cache["button_message_id"] = message_id
    save_db()

def add_button_message_id(message_id):
    """Adiciona um ID de mensagem do botão (suporta múltiplos)"""
    current = _db_cache.get("button_message_id")
    if isinstance(current, list):
        if message_id not in current:
            current.append(message_id)
    elif current:
        _db_cache["button_message_id"] = [current, message_id]
    else:
        _db_cache["button_message_id"] = message_id
    save_db()

def get_button_message_id():
    """Retorna o ID da mensagem do botão"""
    return _db_cache["button_message_id"]

def set_inscricoes_closed(closed):
    """Define se as inscrições estão fechadas"""
    _db_cache["inscricoes_closed"] = closed
    save_db()

def get_inscricoes_closed():
    """Verifica se as inscrições estão fechadas"""
    return _db_cache["inscricoes_closed"]

def add_to_blacklist(user_id, reason, banned_by=None):
    """Adiciona usuário à blacklist"""
    _db_cache["blacklist"][str(user_id)] = {
        "reason": reason,
        "banned_by": banned_by
    }
    save_db()

def remove_from_blacklist(user_id):
    """Remove usuário da blacklist"""
    if str(user_id) in _db_cache["blacklist"]:
        del _db_cache["blacklist"][str(user_id)]
        save_db()
        return True
    return False

def get_blacklist():
    """Retorna a blacklist"""
    return _db_cache["blacklist"]

def is_blacklisted(user_id):
    """Verifica se o usuário está na blacklist"""
    return str(user_id) in _db_cache["blacklist"]

def set_chat_lock(enabled, channel_id=None):
    """Define o chat lock"""
    _db_cache["chat_lock"] = {
        "enabled": enabled,
        "channel_id": channel_id
    }
    save_db()

def get_chat_lock():
    """Retorna configuração do chat lock"""
    return _db_cache["chat_lock"]

def add_moderator(user_id):
    """Adiciona um moderador"""
    if str(user_id) not in _db_cache["moderators"]:
        _db_cache["moderators"].append(str(user_id))
        save_db()

def remove_moderator(user_id):
    """Remove um moderador"""
    if str(user_id) in _db_cache["moderators"]:
        _db_cache["moderators"].remove(str(user_id))
        save_db()
        return True
    return False

def get_moderators():
    """Retorna lista de moderadores"""
    return _db_cache["moderators"]

def is_moderator(user_id):
    """Verifica se o usuário é moderador"""
    return str(user_id) in _db_cache["moderators"]

def get_statistics():
    """Retorna estatísticas do sorteio"""
    participants = _db_cache["participants"]
    bonus_roles = _db_cache["bonus_roles"]
    
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
        "blacklist_count": len(_db_cache["blacklist"])
    }

def add_manual_tag(user_id, quantity=1):
    """Adiciona TAG manual para um usuário"""
    if str(user_id) in _db_cache["participants"]:
        participant = _db_cache["participants"][str(user_id)]
        if "tickets" not in participant:
            participant["tickets"] = {}
        
        current_manual_tag = participant["tickets"].get("manual_tag", 0)
        participant["tickets"]["manual_tag"] = int(current_manual_tag) + int(quantity)
        save_db()

def clear_all():
    """Limpa todos os dados"""
    global _db_cache
    _db_cache = get_default_db()
    save_db()

_db_cache = load_db()
