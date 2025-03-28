FROM python:3.13.1

WORKDIR /app

COPY app/requirements.txt .

# Instalar dependências do arquivo requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r  requirements.txt 

COPY . /app

EXPOSE 8501

# Comando para rodar a aplicação
CMD ["streamlit", "run", "app/stream.py", "--server.port=8501", "--server.address=0.0.0.0"]
