import os
from google.cloud import firestore
from google.api_core.exceptions import GoogleAPICallError
import hashlib
from datetime import datetime
import logging

# Importa a função de upload do GCS
from gcs_service import upload_media_to_gcs

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
    preview = text_preview[:50]
    ts_str = timestamp.isoformat()
    unique_string = f"{ts_str}-{author}-{preview}"
    return hashlib.sha256(unique_string.encode('utf-8')).hexdigest()

def log_system_event(task_id: str, source: str, details: str, status: str):
    """
    Registra um evento no log do sistema no Firestore.
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

def process_and_save_messages(group_name: str, messages: list, media_files_map: dict, gcs_bucket_name: str):
    """
    Processa mensagens, faz upload de mídias associadas para o GCS com o caminho
    correto e salva os metadados no Firestore de forma idempotente.

    Args:
        group_name (str): O nome do grupo do WhatsApp.
        messages (list): A lista de mensagens parseadas.
        media_files_map (dict): Mapeia `media_filename` para o seu caminho local (`media_path`).
        gcs_bucket_name (str): O nome do bucket do GCS para upload.
    """
    if not db:
        raise ConnectionError("Conexão com o Firestore não está disponível.")

    group_id = get_group_id(group_name)
    group_ref = db.collection('whatsapp_groups').document(group_id)
    messages_ref = group_ref.collection('messages')
    
    try:
        group_ref.set({
            'group_name': group_name,
            'last_ingestion_date': firestore.SERVER_TIMESTAMP
        }, merge=True)
    except GoogleAPICallError as e:
        logging.error(f"Erro ao atualizar metadados do grupo '{group_name}': {e}")

    batch = db.batch()
    saved_count = 0
    
    existing_message_ids = {doc.id for doc in messages_ref.stream()}
    logging.info(f"Encontradas {len(existing_message_ids)} mensagens existentes para o grupo '{group_name}'.")

    for msg in messages:
        message_id = get_message_id(
            msg['timestamp_utc'],
            msg['author'],
            msg['message_text']
        )

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

        # Se a mensagem tiver mídia, faz o upload para o GCS
        if msg['has_media'] and msg['media_filename'] in media_files_map:
            media_filename = msg['media_filename']
            media_local_path = media_files_map[media_filename]
            
            # Constrói o caminho de destino no GCS
            destination_blob = f"whatsapp/groups/{group_id}/messages/{message_id}/{media_filename}"
            
            gcs_uri, file_hash, media_type = upload_media_to_gcs(
                file_path=media_local_path,
                bucket_name=gcs_bucket_name,
                destination_blob_name=destination_blob
            )
            
            if gcs_uri:
                message_data['media'] = {
                    "original_filename": media_filename,
                    "gcs_uri": gcs_uri,
                    "hash_sha256": file_hash,
                    "media_type": media_type
                }
            else:
                # Se o upload falhar, marca como pendente para uma nova tentativa
                message_data['media_analysis_status'] = 'upload_failed'
                logging.warning(f"Falha no upload da mídia '{media_filename}' para a mensagem '{message_id}'.")

        batch.set(doc_ref, message_data)
        saved_count += 1
        
        if saved_count % 499 == 0:
            logging.info(f"Commitando batch com {saved_count} mensagens...")
            batch.commit()
            batch = db.batch()

    if saved_count > 0:
        try:
            batch.commit()
            logging.info(f"Commit final. {saved_count} novas mensagens salvas para o grupo '{group_name}'.")
        except GoogleAPICallError as e:
            logging.error(f"Erro no commit final do batch para o grupo '{group_name}': {e}")
    else:
        logging.info(f"Nenhuma mensagem nova para salvar para o grupo '{group_name}'.")

