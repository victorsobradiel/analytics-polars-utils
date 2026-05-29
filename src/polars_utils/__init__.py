import os
from dotenv import load_dotenv
from pathlib import Path

# --- Carregamento Centralizado de Configuração ---
def find_project_root(markers: list = None) -> Path:
    """
    Encontra a raiz do projeto procurando por arquivos marcadores típicos.
    Se não encontrar nenhum, retorna o diretório pai do arquivo atual.
    """
    if markers is None:
        # Lista de arquivos que geralmente definem a raiz de um projeto Python
        markers = ["requirements.txt", ".git", ".venv", "src"]

    current_dir = Path(__file__).resolve()

    # Sobe a árvore de diretórios
    for parent in [current_dir] + list(current_dir.parents):
        if any((parent / marker).exists() for marker in markers):
            return parent

PROJECT_DIR = find_project_root()
DOTENV_PATH = PROJECT_DIR.parent.parent / ".env"

# Carrega as variáveis de ambiente se o arquivo .env for encontrado
if DOTENV_PATH.is_file():
    load_dotenv(DOTENV_PATH)

def get_app_env() -> str:
    """
    Determina o ambiente da aplicação ('DEV', 'PRD', 'LAKE') com base na variável de ambiente APP_ENV.
    O padrão é 'DEV' se a variável não estiver definida.
    """
    return os.getenv("APP_ENV", "local").upper()