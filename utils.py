import logging
import re
import discord
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

def _clean_text(s: Optional[str]) -> str:
    if not s:
        return ""
    # remove emojis/caracteres especiais mantendo letras/números/espacos
    return re.sub(r'[^\w\s]', '', s).strip().casefold()

def validate_full_name(first_name: str, last_name: str) -> tuple[bool, str]:
    """Valida se o nome está correto"""
    if not first_name or not last_name:
        return False, "❌ Primeiro nome e sobrenome são obrigatórios!"
    
    if len(first_name) < 2:
        return False, "❌ Primeiro nome deve ter no mínimo 2 caracteres!"
    
    if len(last_name) < 2:
        return False, "❌ Sobrenome deve ter no mínimo 2 caracteres!"
    
    return True, ""

def calculate_tickets(
    member, 
    bonus_roles: dict, 
    tag_enabled: bool, 
    tag_text: str, 
    tag_quantity: int
) -> dict:
    """
    Calcula as fichas de um membro baseado em seus cargos e TAG
    
    Retorna um dicionário com a estrutura:
    {
        "roles": {
            "role_id": {"quantity": 1, "abbreviation": "S.B"},
            ...
        },
        "tag": 0,  # fichas de TAG automática
        "tag_text": "membro",
        "manual_tag": 0  # fichas de TAG manual
    }
    """
    tickets = {
        "roles": {},
        "tag": 0,
        "tag_text": tag_text or "",
        "manual_tag": 0
    }
    
    if not member:
        return tickets
    
    # Processa cargos do membro
    if bonus_roles:
        for role_id, role_info in bonus_roles.items():
            # Verifica se o membro tem esse cargo
            member_role_ids = [r.id for r in member.roles]
            
            if int(role_id) in member_role_ids:
                qty = int(role_info.get("quantity", 1) or 1)
                abbr = (role_info.get("abbreviation") or role_info.get("abreviation") or "").strip()
                
                tickets["roles"][str(role_id)] = {
                    "quantity": qty,
                    "abbreviation": abbr
                }
                
                logger.info(f"DEBUG CALC: {member.name} tem cargo {role_id} ({abbr}) = {qty} fichas")
    
    # Processa TAG automática
    if tag_enabled and tag_text:
        tag_search = tag_text.strip().lower()
        
        # Checa em várias posições do nome do usuário
        checks = [
            member.display_name or "",
            member.nick or "",
            member.global_name or "",
            member.name or ""
        ]
        
        for field_value in checks:
            if field_value and tag_search in field_value.strip().lower():
                tickets["tag"] = tag_quantity
                logger.info(f"DEBUG CALC: {member.name} tem TAG automática ({tag_text}) = {tag_quantity} fichas")
                break
    
    return tickets

def get_total_tickets(tickets: dict) -> int:
    """Calcula o total de fichas"""
    total = 0
    
    # Fichas de cargos
    if tickets.get("roles"):
        for role_info in tickets["roles"].values():
            qty = int(role_info.get("quantity", 1) or 1)
            total += qty
    
    # Fichas de TAG automática
    total += int(tickets.get("tag", 0) or 0)
    
    # Fichas de TAG manual
    total += int(tickets.get("manual_tag", 0) or 0)
    
    return total

def format_tickets_list(tickets: dict, guild) -> list[str]:
    """Formata a lista de fichas para exibição"""
    lines = []
    
    # Cargos
    if tickets.get("roles"):
        for role_id, role_info in tickets["roles"].items():
            qty = int(role_info.get("quantity", 1) or 1)
            abbr = (role_info.get("abbreviation") or "").strip() or "Cargo"
            lines.append(f"• **{abbr}**: {qty} ficha(s)")
    
    # TAG automática
    if tickets.get("tag", 0):
        tag_text = (tickets.get("tag_text") or "TAG").strip()
        tag_qty = int(tickets.get("tag", 0) or 0)
        lines.append(f"• **{tag_text}**: {tag_qty} ficha(s)")
    
    # TAG manual
    if tickets.get("manual_tag", 0):
        tag_text = (tickets.get("tag_text") or "TAG").strip()
        manual_qty = int(tickets.get("manual_tag", 0) or 0)
        lines.append(f"• **{tag_text} (Manual)**: {manual_qty} ficha(s)")
    
    return lines if lines else ["Sem fichas bônus"]

def format_detailed_entry(first_name: str, last_name: str, tickets: dict) -> list[str]:
    """Formata uma entrada detalhada para /lista com_fichas"""
    lines = []
    full_name = f"{first_name} {last_name}".strip()
    
    # Nome base
    lines.append(full_name)
    
    # Cargos
    if tickets.get("roles"):
        for role_id, role_info in tickets["roles"].items():
            qty = int(role_info.get("quantity", 1) or 1)
            abbr = (role_info.get("abbreviation") or "").strip() or "Cargo"
            
            for _ in range(qty):
                lines.append(f"{full_name} {abbr}".strip())
    
    # TAG automática
    if tickets.get("tag", 0):
        tag_text = (tickets.get("tag_text") or "TAG").strip()
        tag_qty = int(tickets.get("tag", 0) or 0)
        
        for _ in range(tag_qty):
            lines.append(f"{full_name} {tag_text}".strip())
    
    # TAG manual
    if tickets.get("manual_tag", 0):
        tag_text = (tickets.get("tag_text") or "TAG").strip()
        manual_qty = int(tickets.get("manual_tag", 0) or 0)
        
        for _ in range(manual_qty):
            lines.append(f"{full_name} {tag_text}".strip())
    
    return lines

def parse_color(color_str: Optional[str]) -> discord.Color:
    """
    Converte string de cor para discord.Color
    Aceita: nomes (blue, red, green) ou HEX (#FF5733)
    """
    if not color_str:
        return discord.Color.blue()
    
    color_str = color_str.strip().lower()
    
    # Cores nomeadas
    color_map = {
        "blue": discord.Color.blue(),
        "red": discord.Color.red(),
        "green": discord.Color.green(),
        "yellow": discord.Color.yellow(),
        "purple": discord.Color.purple(),
        "pink": discord.Color.pink(),
        "orange": discord.Color.orange(),
        "gold": discord.Color.gold(),
        "teal": discord.Color.teal(),
        "cyan": discord.Color.cyan(),
        "magenta": discord.Color.magenta(),
        "dark_blue": discord.Color.dark_blue(),
        "dark_green": discord.Color.dark_green(),
        "dark_red": discord.Color.dark_red(),
    }
    
    if color_str in color_map:
        return color_map[color_str]
    
    # HEX color
    if color_str.startswith("#"):
        try:
            hex_value = color_str.lstrip("#")
            return discord.Color(int(hex_value, 16))
        except ValueError:
            return discord.Color.blue()
    
    return discord.Color.blue()
