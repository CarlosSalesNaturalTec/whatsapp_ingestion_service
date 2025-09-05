# Módulo `whatsapp_ingestion_service`

**Versão:** 1.0
**Responsabilidade:** Ingestão, parsing e armazenamento de dados exportados de grupos do WhatsApp.

---

## 1. Visão Geral

Este micro-serviço, construído em FastAPI (Python), atua como o ponto de entrada para dados de conversas do WhatsApp na plataforma de Social Listening. Ele é projetado para ser robusto, escalável e idempotente, garantindo que os dados sejam processados de forma eficiente e sem duplicidade.

O serviço expõe um único endpoint que recebe um arquivo `.zip` contendo a exportação de uma conversa. O processamento pesado é delegado a uma tarefa em background para garantir uma resposta rápida à requisição e evitar timeouts.

## 2. Arquitetura e Fluxo

1.  **Endpoint de Upload:** `POST /ingest/upload` recebe o arquivo `.zip`.
2.  **Validação e Armazenamento Temporário:** O arquivo é validado e salvo em um diretório temporário no ambiente de execução (Cloud Run).
3.  **Tarefa em Background:** Uma tarefa assíncrona é iniciada para:
    *   Descompactar o arquivo.
    *   Localizar e parsear o arquivo de texto (`.txt`) para extrair mensagens, autores, timestamps e metadados.
    *   Identificar arquivos de mídia.
    *   Calcular o hash SHA-256 de cada arquivo de mídia.
    *   Fazer o upload da mídia para o Google Cloud Storage (GCS), usando o hash como nome do arquivo para desduplicação.
    *   Gerar um ID único e determinístico para cada mensagem (baseado no timestamp, autor e conteúdo).
    *   Salvar os dados das mensagens na sub-coleção `messages` do grupo correspondente no Firestore, usando o ID da mensagem para desduplicação.
4.  **Logging:** O início, sucesso ou falha do processo são registrados na coleção `system_logs` no Firestore.
5.  **Limpeza:** O diretório temporário é removido ao final do processo.

## 3. Modelo de Dados no Firestore

-   **Coleção:** `whatsapp_groups`
    -   **Documento (ID):** `group_id` (hash SHA-1 do nome do grupo)
        -   `group_name`: (string) Nome do grupo.
        -   `last_ingestion_date`: (timestamp) Data da última ingestão.
    -   **Sub-coleção:** `messages`
        -   **Documento (ID):** `message_id` (hash SHA-256 de timestamp + autor + preview do texto)
            -   `timestamp_utc`: (timestamp) Data e hora da mensagem.
            -   `author`: (string) Nome do autor.
            -   `message_text`: (string) Conteúdo da mensagem.
            -   `is_system_message`: (boolean) Se é uma mensagem gerada pelo sistema.
            -   `has_media`: (boolean) Se a mensagem contém mídia.
            -   `nlp_status`: (string) Status para o processamento de NLP (default: `pending`).
            -   `media_analysis_status`: (string) Status para análise de mídia.
            -   `media`: (map, opcional)
                -   `original_filename`: (string) Nome original do arquivo.
                -   `gcs_uri`: (string) URI do arquivo no GCS.
                -   `hash_sha256`: (string) Hash do conteúdo do arquivo.
                -   `media_type`: (string) Tipo MIME do arquivo.

## 4. Configuração e Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto a partir do `.env.example` e configure as seguintes variáveis:

-   `GCP_PROJECT_ID`: ID do seu projeto no Google Cloud.
-   `GCS_BUCKET_NAME`: Nome do bucket no Cloud Storage onde as mídias serão armazenadas. O padrão sugerido é `[GCP_PROJECT_ID]-whatsapp-media`.

## 5. Execução Local

1.  **Crie e ative um ambiente virtual:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # Linux/macOS
    .\venv\Scripts\activate    # Windows
    ```

2.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Autentique-se no Google Cloud (se necessário):**
    ```bash
    gcloud auth application-default login
    ```

4.  **Inicie o servidor:**
    ```bash
    uvicorn main:app --reload
    ```
    O serviço estará disponível em `http://127.0.0.1:8000`. A documentação interativa da API (Swagger UI) estará em `http://127.0.0.1:8000/docs`.

## 6. Deploy no Google Cloud Run

1.  **Construa a imagem do container:**
    ```bash
    gcloud builds submit --tag gcr.io/[GCP_PROJECT_ID]/whatsapp-ingestion-service .
    ```

2.  **Faça o deploy do serviço:**
    ```bash
    gcloud run deploy whatsapp-ingestion-service \
      --image gcr.io/[GCP_PROJECT_ID]/whatsapp-ingestion-service \
      --platform managed \
      --region [SUA_REGIAO] \
      --allow-unauthenticated \
      --set-env-vars="GCP_PROJECT_ID=[GCP_PROJECT_ID],GCS_BUCKET_NAME=[GCS_BUCKET_NAME]"
      --port 8080
    ```
    **Nota:** `--allow-unauthenticated` é usado para simplificar. Em um ambiente de produção real, você deve proteger este endpoint, por exemplo, usando o API Gateway ou a autenticação do Cloud Run com IAP.

## 7. Relação com Outros Módulos

-   **Frontend:** O frontend irá consumir o endpoint `/ingest/upload` para enviar os arquivos `.zip` selecionados pelo usuário.
-   **API NLP:** O módulo de NLP irá monitorar a sub-coleção `whatsapp_groups/{group_id}/messages` por documentos com `nlp_status: 'pending'` para realizar as análises de texto.
-   **Media Analysis:** Um futuro módulo de análise de mídia irá monitorar a mesma coleção por documentos com `media_analysis_status: 'pending'`.
