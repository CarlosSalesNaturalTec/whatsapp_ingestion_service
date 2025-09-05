# Use a imagem oficial do Python
FROM python:3.9-slim

# Define o diretório de trabalho no container
WORKDIR /app

# Copia o arquivo de dependências
COPY requirements.txt .

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do código da aplicação
COPY . .

# Expõe a porta que o Uvicorn irá rodar
EXPOSE 8000

# Comando para iniciar a aplicação
# O Gunicorn é recomendado para produção como um worker manager.
# Uvicorn é o servidor ASGI que o Gunicorn irá gerenciar.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
