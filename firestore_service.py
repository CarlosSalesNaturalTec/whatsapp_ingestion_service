import os
from google.cloud import firestore
from google.api_core.exceptions import GoogleAPICallError
import hashlib
from datetime import datetime
import logging

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Inicializa o cliente do Firestore
try:
    db = firestore.Client()
except Exception as e:
    logging.error(f"Não foi possível inicializar o cliente do Firestore: {e}")
    db = None

def get_group_id(group_name: str) -> str:
    """Gera um ID determinístico para o grupo a partir do seu nome."""
    return hashlib.sha1(group_name.encode('utf-8')).hexdigest()

def get_message_id(timestamp: datetime, author: str, text_preview: str) -> str:
    """
    Gera um ID determinístico para a mensagem para garantir a idempotência.
    Usa timestamp, autor e um preview do texto.
    """
    # Usar os primeiros 50 caracteres do texto para o hash
    preview = text_preview[:50]
    
    # Converter timestamp para string no formato ISO para consistência
    ts_str = timestamp.isoformat()
    
    unique_string = f"{ts_str}-{author}-{preview}"
    return hashlib.sha256(unique_string.encode('utf-8')).hexdigest()

def log_system_event(task_id: str, source: str, details: str, status: str):
    """
    Registra um evento no log do sistema no Firestore.

    Args:
        task_id (str): Um ID único para a tarefa de processamento.
        source (str): A origem do log (ex: 'whatsapp_ingestion').
        details (str): Detalhes sobre o evento.
        status (str): O status do evento (ex: 'running', 'completed', 'error').
    """
    if not db:
        logging.error("Cliente Firestore não inicializado. Não é possível registrar o log do sistema.")
        return

    try:
        log_ref = db.collection('system_logs').document(task_id)
        log_data = {
            'timestamp': firestore.SERVER_TIMESTAMP,
            'source': source,
            'details': details,
            'status': status
        }
        log_ref.set(log_data, merge=True)
    except GoogleAPICallError as e:
        logging.error(f"Erro ao registrar log no Firestore: {e}")

def process_and_save_messages(group_name: str, messages: list, media_metadata_map: dict):
    """
    Processa uma lista de mensagens, enriquece com metadados de mídia e salva
    no Firestore de forma idempotente.

    Args:
        group_name (str): O nome do grupo do WhatsApp.
        messages (list): A lista de mensagens parseadas.
        media_metadata_map (dict): Um dicionário mapeando nomes de arquivo de mídia
                                   para seus metadados no GCS.
    """
    if not db:
        raise ConnectionError("Conexão com o Firestore não está disponível.")

    group_id = get_group_id(group_name)
    group_ref = db.collection('whatsapp_groups').document(group_id)
    messages_ref = group_ref.collection('messages')
    
    # Atualiza metadados do grupo
    try:
        group_ref.set({
            'group_name': group_name,
            'last_ingestion_date': firestore.SERVER_TIMESTAMP
        }, merge=True)
    except GoogleAPICallError as e:
        logging.error(f"Erro ao atualizar metadados do grupo '{group_name}': {e}")
        # Prosseguir mesmo se isso falhar, para tentar salvar as mensagens

    # Usar um batch para escrever as mensagens de forma mais eficiente
    batch = db.batch()
    saved_count = 0
    
    # Obter IDs de mensagens existentes para evitar reescritas desnecessárias
    existing_message_ids = {doc.id for doc in messages_ref.stream()}
    logging.info(f"Encontradas {len(existing_message_ids)} mensagens existentes para o grupo '{group_name}'.")

    for msg in messages:
        message_id = get_message_id(
            msg['timestamp_utc'],
            msg['author'],
            msg['message_text']
        )

        # **Lógica de Idempotência**: Pular se a mensagem já existe
        if message_id in existing_message_ids:
            continue

        doc_ref = messages_ref.document(message_id)
        
        message_data = {
            "timestamp_utc": msg['timestamp_utc'],
            "author": msg['author'],
            "message_text": msg['message_text'],
            "is_system_message": msg['is_system_message'],
            "has_media": msg['has_media'],
            "nlp_status": "pending",
            "media_analysis_status": "pending" if msg['has_media'] else "not_applicable"
        }

        # Adicionar metadados de mídia se existirem
        if msg['has_media'] and msg['media_filename'] in media_metadata_map:
            media_info = media_metadata_map[msg['media_filename']]
            message_data['media'] = {
                "original_filename": msg['media_filename'],
                "gcs_uri": media_info['gcs_uri'],
                "hash_sha256": media_info['hash_sha256'],
                "media_type": media_info['media_type']
            }
        
        batch.set(doc_ref, message_data)
        saved_count += 1
        
        # O Firestore recomenda que um batch não exceda 500 operações.
        if saved_count % 499 == 0:
            logging.info(f"Commitando batch com {saved_count} mensagens...")
            batch.commit()
            batch = db.batch() # Inicia um novo batch

    # Commit final para as mensagens restantes
    if saved_count > 0:
        try:
            batch.commit()
            logging.info(f"Commit final. {saved_count} novas mensagens salvas para o grupo '{group_name}'.")
        except GoogleAPICallError as e:
            logging.error(f"Erro no commit final do batch para o grupo '{group_name}': {e}")
    else:
        logging.info(f"Nenhuma mensagem nova para salvar para o grupo '{group_name}'.")
