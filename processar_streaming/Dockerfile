FROM python:3.12-slim

RUN pip install --upgrade pip

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc

# Definir o diretório de trabalho
WORKDIR /app

# Copiar o código da aplicação
COPY . .


# Ajustar o PYTHONPATH para incluir o diretório raiz do projeto
ENV PYTHONPATH=/app

# Instalar dependências do Python
RUN pip install -r processar_streaming/requirements.txt

# Comando para rodar a aplicação
CMD ["python", "processar_streaming/app.py"]
