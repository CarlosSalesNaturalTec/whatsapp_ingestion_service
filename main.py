from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
import os
import shutil
import uuid
from datetime import datetime
import logging
from dotenv import load_dotenv

from parser import parse_whatsapp_chat
from firestore_service import process_and_save_messages, log_system_event
from gcs_service import upload_media_to_gcs

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI(
    title="WhatsApp Ingestion Service",
    description="Serviço para processar uploads de arquivos de exportação de conversas do WhatsApp.",
    version="1.0.0"
)

# Variáveis de ambiente (carregadas no início)
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

if not GCP_PROJECT_ID or not GCS_BUCKET_NAME:
    raise RuntimeError("Variáveis de ambiente GCP_PROJECT_ID e GCS_BUCKET_NAME devem ser definidas.")

def background_processing_task(temp_dir: str, original_filename: str):
    """
    Tarefa executada em background para processar o arquivo .zip.
    """
    task_id = str(uuid.uuid4())
    log_system_event(
        task_id,
        source='whatsapp_ingestion',
        details=f"Iniciando processamento para o arquivo: {original_filename}",
        status='running'
    )

    try:
        logging.info(f"Iniciando processamento em background para: {temp_dir}")

        # 1. Encontrar o arquivo .txt
        txt_file_path = None
        media_files = []
        for root, _, files in os.walk(temp_dir):
            for file in files:
                if file.endswith('.txt'):
                    txt_file_path = os.path.join(root, file)
                elif not file.startswith('.'): # Ignorar arquivos ocultos
                    media_files.append(os.path.join(root, file))
        
        if not txt_file_path:
            raise ValueError("Nenhum arquivo .txt encontrado no .zip.")

        # 2. Parsear o arquivo de chat
        logging.info(f"Arquivo de chat encontrado: {txt_file_path}")
        group_name, parsed_messages = parse_whatsapp_chat(txt_file_path)
        
        if not group_name or not parsed_messages:
            raise ValueError("Falha ao parsear o nome do grupo ou as mensagens.")

        logging.info(f"Grupo '{group_name}' parseado com {len(parsed_messages)} mensagens.")

        # 3. Processar e fazer upload de mídias
        media_metadata_map = {}
        if media_files:
            logging.info(f"Encontradas {len(media_files)} mídias para processar.")
            for media_path in media_files:
                original_media_filename = os.path.basename(media_path)
                gcs_uri, file_hash, media_type = upload_media_to_gcs(media_path, GCS_BUCKET_NAME)
                if gcs_uri:
                    media_metadata_map[original_media_filename] = {
                        "gcs_uri": gcs_uri,
                        "hash_sha256": file_hash,
                        "media_type": media_type
                    }
            logging.info("Processamento de mídias concluído.")

        # 4. Salvar mensagens e metadados no Firestore
        process_and_save_messages(group_name, parsed_messages, media_metadata_map)
        
        logging.info("Todas as mensagens foram salvas no Firestore com sucesso.")
        log_system_event(
            task_id,
            source='whatsapp_ingestion',
            details=f"Processamento para '{original_filename}' concluído com sucesso.",
            status='completed'
        )

    except Exception as e:
        logging.error(f"Erro durante o processamento em background: {e}", exc_info=True)
        log_system_event(
            task_id,
            source='whatsapp_ingestion',
            details=f"Erro no processamento de '{original_filename}': {str(e)}",
            status='error'
        )
    finally:
        # 5. Limpar o diretório temporário
        shutil.rmtree(temp_dir)
        logging.info(f"Diretório temporário {temp_dir} limpo.")


@app.post("/ingest/upload", status_code=202)
async def upload_whatsapp_zip(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    """
    Recebe um arquivo .zip exportado do WhatsApp, o descompacta e inicia
    uma tarefa em background para processar seu conteúdo.

    - **file**: Arquivo .zip contendo a conversa e as mídias.
    """
    if not file.filename.endswith('.zip'):
        raise HTTPException(status_code=400, detail="Tipo de arquivo inválido. Apenas .zip é aceito.")

    # Criar um diretório temporário seguro
    temp_dir = f"/tmp/{uuid.uuid4()}"
    os.makedirs(temp_dir, exist_ok=True)
    
    file_path = os.path.join(temp_dir, file.filename)

    try:
        # Salvar o arquivo .zip
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Descompactar o arquivo
        shutil.unpack_archive(file_path, temp_dir)
        
        # Adicionar a tarefa de processamento para ser executada em background
        background_tasks.add_task(background_processing_task, temp_dir, file.filename)

        return JSONResponse(
            status_code=202,
            content={
                "message": "Arquivo recebido. O processamento foi iniciado em background.",
                "filename": file.filename
            }
        )
    except Exception as e:
        # Em caso de erro antes da task, limpar
        shutil.rmtree(temp_dir)
        logging.error(f"Erro ao receber ou descompactar o arquivo: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno ao processar o arquivo: {str(e)}")

@app.get("/health", status_code=200)
async def health_check():
    """
    Endpoint para verificação de saúde do serviço.
    """
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
