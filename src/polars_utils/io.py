import os
import uuid
import datetime as _dt
import time
from datetime import timedelta
import logging
import polars as pl
from pathlib import Path
from sqlalchemy import create_engine, text
from connectorx import read_sql

import urllib

from . import get_app_env, PROJECT_DIR

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DbConnector:
    """
    Classe utilitária para gerar URIs de conexão com bancos de dados.
    As configurações são lidas de variáveis de ambiente.
    """
    @staticmethod
    def get_oracle_uri() -> str:
        """URI para Leitura de Dimensões (Protheus11 Oracle)"""
        user = os.getenv("ORACLE_USER")
        password = os.getenv("ORACLE_PASSWORD")
        host = os.getenv("ORACLE_HOST")
        port = os.getenv("ORACLE_PORT")
        service = os.getenv("ORACLE_SERVICE")
        if not all([user, password, host, port, service]):
            raise ValueError("Faltam variáveis de ambiente para conexão Oracle.")
        # Usando protocolo oracle+oracledb ou cx_oracle dependendo da lib instalada
        return f"oracle://{user}:{password}@{host}:{port}/{service}"

    @staticmethod
    def get_sqlserver_uri() -> str:
        """URI para Leitura de Fatos (Replicate SQL Server)"""
        user = os.getenv("SQL_SERVER_USER")
        password = os.getenv("SQL_SERVER_PASSWORD")
        host = os.getenv("SQL_SERVER_HOST")
        db = os.getenv("SQL_SERVER_DB")
        db_driver = "ODBC Driver 17 for SQL Server"
        # Driver deve ser url encoded se conter espaços, connectorx lida bem com strings padrão
        return (
            f"mssql://{user}:{urllib.parse.quote_plus(password)}@"
            f"{host}/{db}?driver={urllib.parse.quote_plus(db_driver)}"
    )

    @staticmethod
    def get_postgres_uri(ambiente: str | None = None) -> str:
        """
        URI para Escrita (dev, prd, lake).
        A lógica de ambiente é:
        1. Usa o valor do parâmetro `ambiente`, se fornecido.
        2. Se não, usa o valor da variável de ambiente `APP_ENV`.
        3. Se nenhum dos dois, o padrão é 'DEV'.
        """
        # O parâmetro da função tem prioridade sobre a variável de ambiente
        if ambiente:
            env_final = ambiente.upper()
        else:
            env_final = get_app_env() # Retorna 'DEV', 'PRD', etc.

        # 3. Busca as variáveis baseadas no ambiente decidido
        user = os.getenv(f"PG_USER_{env_final}")
        password = os.getenv(f"PG_PASSWORD_{env_final}")
        host = os.getenv(f"PG_HOST_{env_final}")
        port = os.getenv(f"PG_PORT_{env_final}")
        db = os.getenv(f"PG_DB_{env_final}")
        
        return f"postgresql://{user}:{urllib.parse.quote_plus(str(password))}@{host}:{port}/{db}"

class DbReader:
    """
    Gerencia operações de extração e leitura em bancos de dados.
    Armazena o URI e a pasta de destino para evitar repetição de parâmetros.
    """
    def __init__(self, uri: str, pasta: str = "bronze"):
        self.uri = uri
        self.pasta = pasta

    def fetch(self, query: str, table_name: str, **kwargs) -> pl.LazyFrame:
        """
        Executa uma query SQL, cronometra o tempo, aplica limpezas básicas
        e salva o resultado em um arquivo .parquet.
        """
        start_time = time.time()
        logging.info(f"Iniciando extração: {table_name}...")

        df = read_sql(self.uri, query, **kwargs)

        df = df.select(pl.all().name.to_lowercase())
        df = df.with_columns(pl.col(pl.String).str.strip_chars())

        data_dir = get_data_dir(self.pasta)
        data_dir.mkdir(parents=True, exist_ok=True)
        
        parquet_path = data_dir / f"{table_name}.parquet"
        df.write_parquet(parquet_path)

        end_time = time.time()
        duration = str(timedelta(seconds=round(end_time - start_time))).zfill(8)

        logging.info(f"[{duration}] {table_name} extraído e salvo em {parquet_path.name}. ({df.height} linhas)")
        
        return df.lazy()

    def get_incremental(
        self,
        query: str,
        table_name: str,
        primary_key: str | list[str] = "r_e_c_n_o_",
        dtcommit_col: str = "dtcommitrep",
        partition_on: str | None = None,
        partition_num: int = 1,
    ) -> pl.LazyFrame:
        """
        Carrega dados incrementalmente comparando chaves primárias e data de modificação.
        """
        data_dir = get_data_dir(self.pasta)
        parquet_path = data_dir / f"{table_name}.parquet"
        
        # Normaliza primary_key para lista
        if isinstance(primary_key, str):
            primary_key = [primary_key]

        # ── helpers ──────────────────────────────────────────────────────────────
        def _find_col(cols: list[str], name: str) -> str:
            name_lower = name.lower()
            for c in cols:
                if c.lower() == name_lower:
                    return c
            raise KeyError(f"Coluna '{name}' não encontrada no schema.")

        def _fmt(val) -> str:
            if val is None: return "NULL"
            if isinstance(val, (int, float)): return str(val)
            if isinstance(val, (_dt.date, _dt.datetime)): return f"'{val.isoformat()}'"
            return "'" + str(val).replace("'", "''") + "'"

        read_kwargs: dict = {"return_type": "polars"}
        if partition_on:
            read_kwargs["partition_on"] = partition_on
            read_kwargs["partition_num"] = partition_num

        def _force_full_download() -> pl.LazyFrame:
            logging.warning(f"Arquivo local para '{table_name}' não encontrado ou schema divergente. Forçando carga completa.")
            return self.fetch(query=query, table_name=table_name, **read_kwargs)

        # 1. Se não existe arquivo local, baixa tudo
        if not parquet_path.exists():
            return _force_full_download()

        # 2. Verificar schema
        try:
            lf_local = pl.scan_parquet(str(parquet_path))
            local_cols = lf_local.collect_schema().names()
            
            # Check rápido de schema no DB
            schema_query = f"SELECT * FROM ({query}) _ WHERE 1=0"
            db_cols = read_sql(self.uri, schema_query, return_type="polars").columns
            
            if {c.lower() for c in local_cols} != {c.lower() for c in db_cols}:
                return _force_full_download()
        except Exception as e:
            logging.error(f"Erro ao validar schema para '{table_name}': {e}")
            return _force_full_download()

        # 3. Mapear nomes reais das colunas (case-insensitive)
        actual_dt_col = _find_col(local_cols, dtcommit_col)
        actual_pk_cols = [_find_col(local_cols, pk) for pk in primary_key]
        
        # 4. Pegar o maior dtcommit local (Global)
        max_dt = lf_local.select(pl.col(actual_dt_col).max()).collect().item()

        # 5. Montar query incremental
        if max_dt is None:
            incremental_query = query
        else:
            incremental_query = f"SELECT * FROM ({query}) __t WHERE {actual_dt_col} > {_fmt(max_dt)}"

        logging.info(f"Buscando incrementais de '{table_name}' a partir de {max_dt}")

        try:
            df_new = read_sql(self.uri, incremental_query, **read_kwargs)
            df_new = df_new.select(pl.all().name.to_lowercase())
            df_new = df_new.with_columns(pl.col(pl.String).str.strip_chars())
        except Exception as e:
            raise RuntimeError(f"Falha na leitura incremental: {e}")

        if df_new.height == 0:
            logging.info(f"Nenhum registro novo encontrado para '{table_name}'.")
            return lf_local

        # 6. Merge e Deduplicação (Upsert)
        lf_final = (
            pl.concat([lf_local, df_new.lazy()], how="vertical_relaxed")
            .sort(actual_dt_col)
            .unique(
                subset=actual_pk_cols, 
                keep="last", 
                maintain_order=False
            )
        )

        try:
            df_final = lf_final.collect()
            df_final.write_parquet(str(parquet_path))
            logging.info(f"Arquivo local atualizado: {parquet_path.name} ({df_final.height} linhas)")
            return df_final.lazy()
        except Exception as e:
            logging.warning(f"Falha ao gravar parquet atualizado para '{table_name}': {e}")
            return lf_final


# ── WRAPPERS PARA RETROCOMPATIBILIDADE ──────────────────────────────────────
def fetch_data(uri: str, query: str, pasta: str, table_name: str, **kwargs) -> pl.LazyFrame:
    """Função de atalho para leitura total via classe DataFetcher."""
    fetcher = DbReader(uri=uri, pasta=pasta)
    return fetcher.fetch(query=query, table_name=table_name, **kwargs)

def get_incremental_data(
    query: str,
    table_name: str,
    uri: str,
    primary_key: str | list[str] = "r_e_c_n_o_",
    dtcommit_col: str = "dtcommitrep",
    partition_on: str | None = None,
    partition_num: int = 1,
    pasta: str = "bronze",
) -> pl.LazyFrame:
    """Função de atalho para leitura incremental via classe DbReader."""
    fetcher = DbReader(uri=uri, pasta=pasta)
    return fetcher.get_incremental(
        query=query, 
        table_name=table_name, 
        primary_key=primary_key, 
        dtcommit_col=dtcommit_col, 
        partition_on=partition_on, 
        partition_num=partition_num
    )

class PostgresWriter:
    """
    Gerencia operações de escrita em um banco de dados PostgreSQL.

    A classe é inicializada com um ambiente e schema, que são reutilizados
    em todas as operações de escrita, simplificando a chamada dos métodos.
    """
    def __init__(self, ambiente: str = "dev", schema: str = "public"):
        self.ambiente = ambiente
        self.schema = schema
        self.uri = DbConnector.get_postgres_uri(self.ambiente)
        self._engine = None

    @property
    def engine(self):
        """Cria o engine do SQLAlchemy de forma lazy (apenas quando necessário)."""
        if self._engine is None:
            self._engine = create_engine(self.uri)
        return self._engine

    def replace(self, df: pl.DataFrame, table_name: str):
        """Substitui completamente a tabela com os dados do DataFrame (DROP e CREATE)."""
        tabela_alvo = f"{self.schema}.{table_name}"
        try:
            df.write_database(
                table_name=tabela_alvo,
                connection=self.uri,
                if_table_exists="replace",
                engine="adbc"
            )
            logging.info(f"Tabela {tabela_alvo} substituída com sucesso. Linhas: {df.height}")
        except Exception as e:
            logging.error(f"Erro ao substituir a tabela {tabela_alvo} no Postgres: {e}")
            raise

    def snapshot(self, df: pl.DataFrame, table_name: str):
        """
        Atualiza os dados da tabela para um Snapshot exato do DataFrame,
        sem downtime para leitura (usa TRUNCATE e INSERT dentro de uma transação).
        """
        unique_id = str(uuid.uuid4()).replace("-", "_")
        staging_table_name = f"{table_name}_stg_{unique_id}"
        
        tabela_oficial = f"{self.schema}.{table_name}"
        tabela_staging = f"{self.schema}.{staging_table_name}"
        
        colunas = ", ".join([f'"{col}"' for col in df.columns])

        try:
            # 1. Escreve os dados na tabela de staging
            df.write_database(
                table_name=tabela_staging,
                connection=self.uri,
                if_table_exists="replace",
                engine="adbc"
            )

            # 2. Inicia a transação para a troca de dados
            with self.engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE {tabela_oficial};"))
                conn.execute(text(f"""
                    INSERT INTO {tabela_oficial} ({colunas})
                    SELECT {colunas} FROM {tabela_staging};
                """))
                conn.execute(text(f"DROP TABLE {tabela_staging};"))

            logging.info(f"Snapshot da tabela {tabela_oficial} atualizado com sucesso. Linhas: {df.height}")

        except Exception as e:
            logging.error(f"Erro ao atualizar o snapshot da tabela {tabela_oficial}: {e}")
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {tabela_staging};"))
            except:
                pass
            raise

    def incremental(
        self,
        df: pl.DataFrame,
        table_name: str,
        date_column: str,
        start_date: str,
        end_date: str,
    ):
        """
        Escreve dados de forma incremental para um período específico.
        Deleta os registros do período na tabela oficial e insere os novos dados.
        """
        unique_id = str(uuid.uuid4()).replace("-", "_")
        staging_table_name = f"{table_name}_stg_{unique_id}"

        tabela_oficial = f"{self.schema}.{table_name}"
        tabela_staging = f"{self.schema}.{staging_table_name}"

        colunas = ", ".join([f'"{col}"' for col in df.columns])

        try:
            df.write_database(
                table_name=tabela_staging,
                connection=self.uri,
                if_table_exists="replace",
                engine="adbc",
            )
            logging.info(f"Dados carregados na tabela de staging: {tabela_staging}")

            with self.engine.begin() as conn:
                delete_sql = text(f'DELETE FROM {tabela_oficial} WHERE "{date_column}" >= \'{start_date}\' AND "{date_column}" < \'{end_date}\';')
                conn.execute(delete_sql)

                insert_sql = text(f"INSERT INTO {tabela_oficial} ({colunas}) SELECT {colunas} FROM {tabela_staging};")
                conn.execute(insert_sql)

                conn.execute(text(f"DROP TABLE {tabela_staging};"))

            logging.info(f"Carga incremental em {tabela_oficial} concluída. Linhas inseridas: {df.height}")

        except Exception as e:
            logging.error(f"Erro durante a carga incremental na tabela {tabela_oficial}: {e}")
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {tabela_staging};"))
            except Exception as cleanup_e:
                logging.error(f"Erro adicional durante a limpeza de emergência: {cleanup_e}")
            raise

def write_postgres(df: pl.DataFrame, table_name: str, ambiente: str = "dev"):
    """Função de atalho para uma escrita simples com substituição total da tabela."""
    writer = PostgresWriter(ambiente=ambiente)
    writer.replace(df, table_name)

def write_postgres_snapshot(df: pl.DataFrame, table_name: str, ambiente: str = "dev"):
    """Função de atalho para uma escrita transacional de snapshot."""
    writer = PostgresWriter(ambiente=ambiente)
    writer.snapshot(df, table_name)

def write_postgres_incremental(
    df: pl.DataFrame,
    table_name: str,
    schema: str,
    date_column: str,
    start_date: str,
    end_date: str,
    ambiente: str = "dev",
):
    """Função de atalho para uma escrita incremental transacional."""
    writer = PostgresWriter(ambiente=ambiente, schema=schema)
    writer.incremental(df, table_name, date_column, start_date, end_date)

def get_data_dir(pasta: str = "bronze") -> Path:
    """
    Determina o diretório de dados para staging com base no ambiente da aplicação.
    - Em ambiente 'PRD', usa o caminho do servidor '/repolake'.
    - Em outros ambientes ('DEV'), usa um caminho local dentro do projeto.
    """
    app_env = get_app_env().upper()
    
    if app_env in ["PRD", "DEV"]:
        # Caminho padrão para ambientes de servidor
        data_dir = Path("/repolake") / pasta
    else:
        # Caminho local para outros ambientes (ex: 'LOCAL')
        # Usa o PROJECT_DIR robusto definido no __init__.py
        data_dir = PROJECT_DIR / "src" / "data" / pasta

    data_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Diretório de dados ({app_env}) para staging: {data_dir}")
    return data_dir
