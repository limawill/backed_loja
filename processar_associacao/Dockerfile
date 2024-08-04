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
# Primeiro, instale as dependências da pasta app1
#RUN pip install -r app1/requirements.txt
# Depois, instale as dependências da pasta processar_associacao
RUN pip install -r processar_associacao/requirements.txt

# Comando para rodar a aplicação
CMD ["python", "processar_associacao/app.py"]
