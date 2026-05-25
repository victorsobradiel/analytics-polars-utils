import os
from dotenv import load_dotenv
from pathlib import Path

# --- Carregamento Centralizado de Configuração ---
# Este arquivo é executado sempre que qualquer módulo de 'polars_utils' é importado.

# Define o diretório do projeto de forma robusta
# __file__ -> .../src/polars_utils/__init__.py
# .parents[2] -> .../ (raiz do projeto, ex: analytics-polars-utils)
PROJECT_DIR = Path(__file__).resolve().parents[2]
DOTENV_PATH = PROJECT_DIR / ".env"

if DOTENV_PATH.exists():
    load_dotenv(DOTENV_PATH)
else:
    # Em ambientes de produção (ex: Docker, Airflow), as variáveis
    # geralmente são injetadas diretamente, então um .env não é necessário.
    print(f"Aviso: arquivo .env não encontrado em {DOTENV_PATH}. Usando variáveis de ambiente existentes.")

def get_app_env() -> str:
    """
    Determina o ambiente da aplicação ('DEV', 'PRD', 'LAKE') com base na variável de ambiente APP_ENV.
    O padrão é 'DEV' se a variável não estiver definida.
    """
    return os.getenv("APP_ENV", "dev").upper()