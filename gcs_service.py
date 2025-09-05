import os
import hashlib
import mimetypes
from google.cloud import storage
from google.api_core.exceptions import GoogleAPICallError, NotFound
import logging

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Inicializa o cliente do GCS
try:
    storage_client = storage.Client()
except Exception as e:
    logging.error(f"Não foi possível inicializar o cliente do Google Cloud Storage: {e}")
    storage_client = None

def calculate_file_hash(file_path: str) -> str:
    """Calcula o hash SHA-256 de um arquivo."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        # Lê o arquivo em chunks para não sobrecarregar a memória
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def upload_media_to_gcs(file_path: str, bucket_name: str) -> (str, str, str):
    """
    Faz o upload de um arquivo de mídia para o Google Cloud Storage de forma idempotente.

    A idempotência é garantida usando o hash do arquivo como nome do blob.
    Se um blob com o mesmo nome (hash) já existe, o upload é pulado.

    Args:
        file_path (str): O caminho local para o arquivo de mídia.
        bucket_name (str): O nome do bucket no GCS.

    Returns:
        tuple: (gcs_uri, file_hash, media_type)
               - gcs_uri (str): A URI do arquivo no GCS (ex: gs://bucket/hash.jpg).
               - file_hash (str): O hash SHA-256 do arquivo.
               - media_type (str): O tipo MIME do arquivo.
               Retorna (None, None, None) em caso de erro.
    """
    if not storage_client:
        logging.error("Cliente GCS não inicializado. Upload cancelado.")
        return None, None, None

    try:
        # 1. Calcular o hash do arquivo
        file_hash = calculate_file_hash(file_path)
        
        # 2. Determinar a extensão e o tipo MIME
        original_filename = os.path.basename(file_path)
        _, extension = os.path.splitext(original_filename)
        media_type, _ = mimetypes.guess_type(original_filename)
        media_type = media_type or 'application/octet-stream' # Default se não conseguir adivinhar

        # 3. Criar o nome do blob usando o hash
        blob_name = f"{file_hash}{extension}"
        gcs_uri = f"gs://{bucket_name}/{blob_name}"

        # 4. Verificar se o blob já existe (lógica de idempotência)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        if blob.exists():
            logging.info(f"Mídia já existe no GCS (hash: {file_hash}). Pulando upload de '{original_filename}'.")
            return gcs_uri, file_hash, media_type

        # 5. Fazer o upload se não existir
        logging.info(f"Fazendo upload de '{original_filename}' para '{gcs_uri}'...")
        blob.upload_from_filename(file_path, content_type=media_type)
        
        logging.info(f"Upload de '{original_filename}' concluído com sucesso.")
        return gcs_uri, file_hash, media_type

    except FileNotFoundError:
        logging.error(f"Arquivo de mídia não encontrado em: {file_path}")
        return None, None, None
    except GoogleAPICallError as e:
        logging.error(f"Erro de API do Google Cloud Storage ao fazer upload de '{file_path}': {e}")
        return None, None, None
    except Exception as e:
        logging.error(f"Erro inesperado durante o upload de '{file_path}' para o GCS: {e}", exc_info=True)
        return None, None, None
