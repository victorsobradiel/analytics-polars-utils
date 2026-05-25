import os
import uuid
import datetime as _dt
import time
from datetime import timedelta
import polars as pl
from pathlib import Path
from sqlalchemy import create_engine, text
from connectorx import read_sql

import urllib

from . import get_app_env

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

        print(f"Ambiente de conexão Postgres definido como: {env_final}")

        # 3. Busca as variáveis baseadas no ambiente decidido
        user = os.getenv(f"PG_USER_{env_final}")
        password = os.getenv(f"PG_PASSWORD_{env_final}")
        host = os.getenv(f"PG_HOST_{env_final}")
        port = os.getenv(f"PG_PORT_{env_final}")
        db = os.getenv(f"PG_DB_{env_final}")
        
        return f"postgresql://{user}:{urllib.parse.quote_plus(str(password))}@{host}:{port}/{db}"

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
            print(f"Tabela {tabela_alvo} substituída com sucesso. Linhas: {df.height}")
        except Exception as e:
            print(f"Erro ao substituir a tabela no Postgres: {e}")
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

            print(f"Snapshot atualizado em {tabela_oficial} com sucesso. Linhas: {df.height}")

        except Exception as e:
            print(f"Erro ao atualizar o snapshot no Postgres: {e}")
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
            print(f"Dados carregados na tabela de staging: {tabela_staging}")

            with self.engine.begin() as conn:
                delete_sql = text(f'DELETE FROM {tabela_oficial} WHERE "{date_column}" >= \'{start_date}\' AND "{date_column}" < \'{end_date}\';')
                conn.execute(delete_sql)

                insert_sql = text(f"INSERT INTO {tabela_oficial} ({colunas}) SELECT {colunas} FROM {tabela_staging};")
                conn.execute(insert_sql)

                conn.execute(text(f"DROP TABLE {tabela_staging};"))

            print(f"Carga incremental em {tabela_oficial} concluída. Linhas inseridas: {df.height}")

        except Exception as e:
            print(f"Erro durante a carga incremental no Postgres: {e}")
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {tabela_staging};"))
            except Exception as cleanup_e:
                print(f"Erro adicional durante a limpeza de emergência: {cleanup_e}")
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
    app_env = get_app_env()
    
    if app_env == "PRD":
        # Caminho no servidor de produção
        data_dir = Path("/repolake") / pasta
    elif app_env == "DEV":
        # Caminho no servidor de produção
        data_dir = Path("/repolake") / pasta
    else:
        # Caminho local para desenvolvimento
        # .../src/polars_utils/io.py -> parents[2] é a raiz do projeto
        project_root = Path(__file__).resolve().parents[2]
        data_dir = project_root / "src" / "data" / pasta

    data_dir.mkdir(parents=True, exist_ok=True)
    print(f"Diretório de dados ({app_env}) para staging: {data_dir}")
    return data_dir

def fetch_data(label: str, conn: str, query: str, **kwargs) -> pl.DataFrame:
    """
    Executa uma query SQL, cronometra o tempo e aplica limpezas básicas.
    - Cronometra o tempo de execução.
    - Converte nomes de colunas para minúsculas.
    - Remove espaços em branco (trim) de todas as colunas de string.
    """
    start_time = time.time()
    print(f"Lendo {label}...", end="\r")

    # Executa a query usando connectorx
    df = read_sql(conn, query, **kwargs)

    # Padronizando nomes de colunas para minúsculas
    df = df.rename({col: col.lower() for col in df.columns})

    # Trim em colunas de string
    df = df.with_columns(pl.col(pl.String).str.strip_chars())

    end_time = time.time()
    duration = str(timedelta(seconds=round(end_time - start_time))).zfill(8)

    print(f"[{duration}] {label} extraído com sucesso. ({df.height} linhas)")
    return df


def get_incremental_data(
    query: str,
    table_name: str,
    uri: str,
    dtcommit_col: str = "dtcommitrep",
    empresa_col: str | None = None,
    partition_on: str | None = None,
    partition_num: int = 1,
    pasta: str = "bronze",
) -> pl.LazyFrame:
    
    """Carrega dados incrementalmente usando um arquivo parquet de staging.
    Retorna um LazyFrame com os dados combinados.
 
    Regras:
    - Arquivo de staging no servidor: /repolake/{pasta}/{table_name}.parquet
    - Arquivo de staging local: <project_root>/src/data/{pasta}/{table_name}.parquet
    - Se existir arquivo local: calcula o maior `dtcommit` por empresa e busca
      do banco apenas registros novos (dtcommit > max_local) por empresa.
    - Merge/upsert usa a chave composta (r_e_c_n_o_, empresa_col): mantém
      sempre o registro com dtcommit mais recente.
    - Grava o parquet atualizado ao final.
 
    Parâmetros:
    - query         : SQL base que retorna a tabela (pode conter filtros).
    - table_name    : nome curto da tabela (ex: sd1, sf1).
    - uri           : string de conexão para pl.read_database_uri / read_sql.
    - dtcommit_col  : coluna de data/hora da última modificação (padrão: dtcommitrep).
    - empresa_col   : coluna que identifica a empresa/filial. Se None, tentará
                      descobrir automaticamente.
    - partition_on  : repassa para read_sql quando necessário.
    - partition_num : repassa para read_sql quando necessário.
    - pasta         : subpasta dentro de data/ onde o parquet será salvo.
    """
 
    data_dir = get_data_dir(pasta)
    parquet_path = data_dir / f"{table_name}.parquet"
 
    # ── helpers ──────────────────────────────────────────────────────────────
 
    def _find_col(cols: list[str], name: str) -> str:
        """Retorna o nome real da coluna na lista, ignorando capitalização."""
        name_lower = name.lower()
        for c in cols:
            if c.lower() == name_lower:
                return c
        raise KeyError(f"Coluna esperada '{name}' não encontrada na lista de colunas.")
 
    def _fmt(val) -> str:
        """Formata um valor Python para uso literal em SQL."""
        if val is None:
            return "NULL"
        if isinstance(val, (int, float)):
            return str(val)
        if isinstance(val, (_dt.date, _dt.datetime)):
            return f"'{val.isoformat()}'"
        return f"""'{str(val).replace("'", "''")}'"""
 
    read_kwargs: dict = {"return_type": "polars"}
    if partition_on:
        read_kwargs["partition_on"] = partition_on
        read_kwargs["partition_num"] = partition_num
 
    # ── download completo (fallback) ──────────────────────────────────────────
    def _force_full_download() -> pl.LazyFrame:
        print(f"Staging para {table_name} não existe ou schema mudou. Fazendo download completo...")
        df = fetch_data(
            label=f"Download completo de {table_name}",
            conn=uri,
            query=query,
            **read_kwargs,
        )
        try:
            df.write_parquet(str(parquet_path))
        except Exception as e:
            print(f"Aviso: falha ao gravar parquet {parquet_path}: {e}")
        return df.lazy()
 
    # ── 1. Parquet local não existe → baixar tudo ─────────────────────────────
 
    if not parquet_path.exists():
        return _force_full_download()
 
    # ── 2. Verificar se o schema (colunas) mudou ──────────────────────────────
 
    try:
        # Usamos scan para checar o schema sem carregar os dados
        lf_local = pl.scan_parquet(str(parquet_path))
        local_cols_set = {c.lower() for c in lf_local.collect_schema().names()}
 
        schema_query = f"SELECT * FROM ({query}) _ WHERE 1=0"
        try:
            db_schema_df = read_sql(uri, schema_query, return_type="polars")
        except Exception:
            # Fallback para bancos que não suportam subquery + WHERE 1=0
            if uri.startswith("oracle"):
                schema_query = f"SELECT * FROM ({query}) WHERE ROWNUM <= 1"
            else:
                schema_query = f"SELECT * FROM ({query}) LIMIT 1"
            db_schema_df = read_sql(uri, schema_query, return_type="polars")
 
        db_cols = {c.lower() for c in db_schema_df.columns}
 
        if local_cols_set != db_cols:
            print(f"Detectada mudança de schema para '{table_name}'. Forçando recarga completa.")
            if db_cols - local_cols_set:
                print(f"  Colunas novas na query : {sorted(db_cols - local_cols_set)}")
            if local_cols_set - db_cols:
                print(f"  Colunas removidas      : {sorted(local_cols_set - db_cols)}")
            return _force_full_download()
 
    except Exception as e:
        print(f"Não foi possível verificar schema de '{table_name}': {e}. Forçando recarga completa.")
        return _force_full_download()
 
    print(f"Schema OK para '{table_name}'. Iniciando carga incremental.")
 
    # ── 3. Descobrir colunas-chave ────────────────────────────────────────────
    local_cols_list = lf_local.collect_schema().names()
    # Coluna de empresa
    found_empresa_col = False
    if empresa_col is None:
        candidates = [
            f"{table_name}_empresa",
            f"id_{table_name}empresa",
            "id_empresacth",
            "id_empresa",
            f"{table_name}_filial",
            f"{table_name}_id",
        ]
        for candidate in candidates:
            try:
                empresa_col = _find_col(local_cols_list, candidate)
                found_empresa_col = True
                break
            except KeyError:
                continue
        if not found_empresa_col:
            raise KeyError(
                f"Não foi possível identificar a coluna de empresa para '{table_name}'."
                " Especifique o argumento empresa_col."
            )
    else:
        empresa_col = _find_col(local_cols_list, empresa_col)
 
    # Detectar nomes reais no arquivo local
    dt_col = _find_col(local_cols_list, dtcommit_col)
    recno_col = _find_col(local_cols_list, "r_e_c_n_o_")
 
    # Nomes normalizados (lowercase) para uso no Polars
    df_empresa_col = empresa_col.lower()
    df_dt_col      = dt_col.lower()
    df_recno_col   = recno_col.lower()
 
    # ── 4. Calcular max(dtcommit) por empresa no parquet local ────────────────
 
    max_per_empresa: list[dict] = (
        lf_local
        .group_by(df_empresa_col)
        .agg(pl.col(df_dt_col).max().alias("max_dt"))
        .collect().to_dicts()
    )
 
    # ── 5. Montar cláusula WHERE incremental ──────────────────────────────────
 
    where_parts: list[str] = []
    empresas_locais: list = []
 
    for row in max_per_empresa:
        emp    = row[df_empresa_col]
        max_dt = row["max_dt"]
        empresas_locais.append(emp)
 
        if max_dt is None:
            # Sem dados para essa empresa localmente → puxar tudo dela
            where_parts.append(f"({empresa_col} = {_fmt(emp)})")
        else:
            # Puxar apenas registros mais novos que o max local
            where_parts.append(
                f"({empresa_col} = {_fmt(emp)} AND {dt_col} > {_fmt(max_dt)})"
            )
 
    # Empresas presentes no banco mas ausentes localmente
    if empresas_locais:
        fmt_list = ",".join(_fmt(e) for e in empresas_locais)
        where_parts.append(f"({empresa_col} NOT IN ({fmt_list}))")
 
    if not where_parts:
        incremental_query = query
        where_clause      = "FULL"
    else:
        where_clause      = " OR ".join(where_parts)
        incremental_query = f"SELECT * FROM ({query}) __t WHERE {where_clause}"
 
    print(f"Buscando incrementais de '{table_name}' | filtro: {where_clause}")
 
    # ── 6. Executar query incremental ─────────────────────────────────────────
 
    try:
        df_new = fetch_data(
            label=f"Registros incrementais de {table_name}",
            conn=uri,
            query=incremental_query,
            **read_kwargs,
        )
    except Exception as e:
        raise RuntimeError(f"Falha na leitura incremental de '{table_name}': {e}") from e
 
    if df_new.height == 0:
        print(f"Nenhum registro novo para '{table_name}'. Usando staging local.")
        return lf_local

    # ── 7. Combinar, fazer upsert e salvar ───────────────────────────────────

    # Concatena o LazyFrame local com o novo DataFrame (convertido para lazy)
    # how='vertical_relaxed' lida com pequenas divergências de schema (ex: ordem de colunas)
    lf_combined = pl.concat([lf_local, df_new.lazy()], how="vertical_relaxed")

    # A lógica de upsert é feita com sort + unique.
    # Mantém o registro mais recente (maior dt_col) para cada chave (recno, empresa)
    lf_final = lf_combined.sort(df_dt_col).unique(
        subset=[df_recno_col, df_empresa_col],  # chave primária composta
        keep="last",                            # mantém o registro mais recente
        maintain_order=False,                   # Mais rápido
    )
 
    try:
        # Coleta o resultado final para um DataFrame antes de salvar
        df_final = lf_final.collect()
        df_final.write_parquet(str(parquet_path))
        print(f"Parquet atualizado salvo em: {parquet_path} ({df_final.height} linhas)")
    except Exception as e:
        print(f"Aviso: falha ao gravar parquet atualizado {parquet_path}: {e}")
 
    return lf_final