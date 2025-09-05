import re
import os
from datetime import datetime
import logging

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Regex aprimorada para o formato "DD/MM/YYYY HH:MM - Autor: Mensagem"
WHATSAPP_MESSAGE_REGEX = re.compile(
    r"^(?P<date>\d{1,2}/\d{1,2}/\d{2,4})\s(?P<time>\d{1,2}:\d{2})\s-\s(?P<author>[^:]+):\s(?P<message>.+)",
    re.DOTALL
)

# Regex para mensagens de sistema, que não possuem um "Autor:"
SYSTEM_MESSAGE_REGEX = re.compile(
    r"^(?P<date>\d{1,2}/\d{1,2}/\d{2,4})\s(?P<time>\d{1,2}:\d{2})\s-\s(?P<message>(?!.+:).+)"
)

# Regex para extrair o nome do grupo do nome do arquivo
GROUP_NAME_REGEX = re.compile(r"Conversa do WhatsApp com (.+?)\.txt")

MEDIA_PLACEHOLDERS = [
    "(arquivo anexado)",
    "<Arquivo de mídia oculto>",
    "<Mídia oculta>"
]

def _parse_timestamp(date_str: str, time_str: str) -> datetime or None:
    """Tenta fazer o parse de strings de data e hora com o formato do WhatsApp."""
    try:
        return datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M")
    except ValueError:
        logging.warning(f"Não foi possível parsear a data/hora: {date_str} {time_str}")
        return None

def parse_whatsapp_chat(file_path: str) -> (str or None, list):
    """
    Realiza o parsing de um arquivo de texto de exportação do WhatsApp de forma robusta.
    """
    logging.info(f"Iniciando parsing do arquivo: {file_path}")
    
    file_name = os.path.basename(file_path)
    group_name_match = GROUP_NAME_REGEX.search(file_name)
    group_name = group_name_match.group(1).strip() if group_name_match else "Nome de Grupo Desconhecido"
    
    messages = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            current_message_data = None
            
            for line in f:
                line = line.strip()
                if not line:
                    continue

                match = WHATSAPP_MESSAGE_REGEX.match(line)
                
                if match:
                    if current_message_data:
                        messages.append(current_message_data)

                    data = match.groupdict()
                    timestamp_dt = _parse_timestamp(data['date'], data['time'])
                    
                    if not timestamp_dt:
                        if current_message_data:
                            current_message_data["message_text"] += "\n" + line
                        continue

                    text = data['message']
                    current_message_data = {
                        "timestamp_utc": timestamp_dt,
                        "author": data['author'].strip(),
                        "message_text": text.strip(),
                        "is_system_message": False,
                        "has_media": any(placeholder in text for placeholder in MEDIA_PLACEHOLDERS),
                        "media_filename": extract_media_filename(text)
                    }
                elif current_message_data:
                    # Linha de continuação de uma mensagem anterior
                    current_message_data["message_text"] += "\n" + line
                    if not current_message_data["has_media"]:
                        current_message_data["has_media"] = any(placeholder in line for placeholder in MEDIA_PLACEHOLDERS)
                    if not current_message_data["media_filename"]:
                        current_message_data["media_filename"] = extract_media_filename(line)
                else:
                    # Tenta identificar como mensagem de sistema
                    system_match = SYSTEM_MESSAGE_REGEX.match(line)
                    if system_match:
                        if current_message_data:
                            messages.append(current_message_data)
                            current_message_data = None
                        
                        data = system_match.groupdict()
                        timestamp_dt = _parse_timestamp(data['date'], data['time'])

                        if timestamp_dt:
                            messages.append({
                                "timestamp_utc": timestamp_dt,
                                "author": "System",
                                "message_text": data['message'].strip(),
                                "is_system_message": True,
                                "has_media": False,
                                "media_filename": None
                            })

            if current_message_data:
                messages.append(current_message_data)

    except Exception as e:
        logging.error(f"Erro ao ler ou parsear o arquivo {file_path}: {e}", exc_info=True)
        return group_name, []

    logging.info(f"Parsing concluído. {len(messages)} mensagens extraídas para o grupo '{group_name}'.")
    return group_name, messages

def extract_media_filename(text: str) -> str or None:
    """
    Extrai um nome de arquivo de mídia do texto da mensagem, se existir.
    """
    # Regex para cobrir IMG, VID, PTT, DOC, STK e outros formatos de mídia
    media_filename_regex = re.compile(
        r"((?:IMG|VID|PTT|DOC|STK)-\d{8}-WA\d{4,}\.\w+)",
        re.IGNORECASE
    )
    match = media_filename_regex.search(text)
    return match.group(0) if match else None
