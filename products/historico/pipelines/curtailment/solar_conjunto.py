"""
Daily Pipeline - restricao_coff_fotovoltaica_tm
Pipeline AUTO-CORRETIVO que processa todos os meses pendentes desde a ultima data

Logica:
1. Busca ultima data no ClickHouse
2. Identifica meses incompletos (ultima_data -> hoje)
3. Processa cada mes incrementalmente

Autor: Breno Loureiro
Projeto: BrazilGrid
"""

from datetime import datetime, timedelta
from pathlib import Path
import os
from typing import List

from prefect import flow, task, get_run_logger
from prefect.cache_policies import NONE
from dotenv import load_dotenv

# Carregar variaveis de ambiente
load_dotenv('config/.env')

from shared.handlers.clickhouse_handler import ClickHouseHandler
from shared.handlers.config_secrets import get_clickhouse_config, wake_clickhouse
from shared.handlers.s3_handler import S3Handler
from shared.handlers.manifest import ManifestManager


# ============================================================================
# CONFIGURACOES
# ============================================================================

DATASET = "restricao_coff_fotovoltaica_tm"
S3_PREFIX = "dataset/restricao_coff_fotovoltaica_tm/"
TABLE = "curtailment_solar_conjunto"
DATABASE = "brazilgrid_historico"
RAW_DATA_DIR = Path("data/raw")
MANIFEST_PATH = Path("data/processed") / f"{DATASET}_manifest.json"


def get_logger():
    """Helper para obter logger do Prefect com fallback"""
    try:
        return get_run_logger()
    except:
        import logging
        return logging.getLogger(__name__)


# ============================================================================
# TASKS
# ============================================================================

@task(name="Download arquivo S3", retries=2, retry_delay_seconds=5, cache_policy=NONE)
def download_s3_task(s3_handler: S3Handler, s3_key: str, local_path: Path) -> str:
    """
    Download do arquivo Parquet do S3 ONS
    Sempre redownload para pegar atualizacoes
    """
    logger = get_logger()

    # Sempre baixar arquivo (pode ter atualizacoes)
    if local_path.exists():
        local_path.unlink()
        logger.info(f"Removendo arquivo antigo para redownload")

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(f"Baixando: {s3_key}")
    md5_hash = s3_handler.download_file(s3_key, local_path)

    size_mb = local_path.stat().st_size / (1024 * 1024)

    logger.info(f"Download: {size_mb:.2f} MB | MD5: {md5_hash[:8]}...")
    return str(local_path)


@task(name="Inserir no ClickHouse", retries=2, retry_delay_seconds=5, cache_policy=NONE)
def insert_clickhouse_task(
    ch_handler: ClickHouseHandler,
    parquet_path: Path,
    table: str,
    source_file: str
) -> int:
    """
    Insere dados do Parquet no ClickHouse (apenas novos registros)
    """
    logger = get_logger()

    try:
        # Buscar ultima data no ClickHouse para este arquivo
        max_date_query = f"""
            SELECT max(din_instante) as max_date
            FROM {DATABASE}.{table}
        """
        result = ch_handler.client.query(max_date_query)
        max_date = result.result_rows[0][0] if result.result_rows else None

        if max_date:
            logger.info(f"Ultima data no CH: {max_date}")
        else:
            logger.info(f"Primeira insercao na tabela")

        # Inserir dados incrementalmente
        rows = ch_handler.insert_parquet_incremental(
            parquet_path=parquet_path,
            table=table,
            source_file=source_file,
            max_date=max_date
        )

        return rows

    except Exception as e:
        logger.error(f"Erro na insercao: {str(e)}")
        raise


@task(name="Processar mes", cache_policy=NONE)
def process_month_task(
    s3_handler: S3Handler,
    ch_handler: ClickHouseHandler,
    manifest: ManifestManager,
    year_month: str,
    download_date: str
) -> dict:
    """
    Processa um mes (download + insert incremental)
    """
    logger = get_logger()

    logger.info(f"Processando: {year_month}")

    # Construir paths
    year, month = year_month.split('-')
    s3_key = f"{S3_PREFIX}RESTRICAO_COFF_FOTOVOLTAICA_{year}_{month}.parquet"
    local_filename = f"RESTRICAO_COFF_FOTOVOLTAICA_{year}_{month}_{download_date}.parquet"
    local_path = RAW_DATA_DIR / local_filename

    # Download (retorna path como string)
    parquet_path_str = download_s3_task(s3_handler, s3_key, local_path)
    parquet_path = Path(parquet_path_str)

    # Calcular MD5 e tamanho
    import hashlib
    md5_hash = hashlib.md5(parquet_path.read_bytes()).hexdigest()
    size_mb = parquet_path.stat().st_size / (1024 * 1024)

    # Insert incremental
    rows = insert_clickhouse_task(
        ch_handler,
        parquet_path,
        TABLE,
        local_filename
    )

    # Atualizar manifest
    if rows > 0:
        manifest.add_month_entry(
            year_month=year_month,
            download_info={
                'rows_inserted': rows,
                'download_date': download_date,
                'file_name': local_filename,
                'md5': md5_hash,
                'file_size_mb': round(size_mb, 2)
            }
        )
        logger.info(f"{year_month}: +{rows:,} registros")
    else:
        logger.info(f"{year_month}: nenhum registro novo")

    return {
        "month": year_month,
        "rows_inserted": rows,
        "status": "success"
    }


def get_months_to_process(last_date_in_db: datetime, today: datetime) -> List[str]:
    """
    Retorna lista de meses (YYYY-MM) que precisam ser processados

    Args:
        last_date_in_db: Ultima data com dados no ClickHouse
        today: Data atual

    Returns:
        Lista de meses no formato ['2025-12', '2026-01']
    """
    # Data inicial dos dados solares no ONS (Abril/2024)
    SOLAR_DATA_START = datetime(2024, 4, 1)

    months = []

    # Se nao tem dados ou data invalida (1970), comecar de Abril/2024
    if last_date_in_db is None or last_date_in_db.year < 2020:
        current = SOLAR_DATA_START
    else:
        # Comecar do mes da ultima data
        current = last_date_in_db.replace(day=1)

    end = today

    while current <= end:
        months.append(current.strftime('%Y-%m'))

        # Proximo mes
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return months


# ============================================================================
# FLOW PRINCIPAL
# ============================================================================

@flow(
    name="Daily Update AUTO-CORRETIVO - restricao_coff_fotovoltaica_tm",
    description="Processa TODOS os meses pendentes desde ultima data ate hoje",
    log_prints=True
)
def daily_restricao_solar_tm():
    """
    Pipeline diario AUTO-CORRETIVO

    Logica:
    1. Busca ultima data no ClickHouse
    2. Identifica meses incompletos (ultima_data -> hoje)
    3. Processa cada mes incrementalmente

    Se pipeline ficar 1 semana sem rodar, processa tudo de uma vez!
    """
    logger = get_logger()

    # Header
    logger.info("="*80)
    logger.info("DAILY UPDATE AUTO-CORRETIVO - restricao_coff_fotovoltaica_tm")
    logger.info("="*80)

    # Data atual
    today = datetime.now()
    download_date = today.strftime('%Y%m%d')

    logger.info(f"Data: {today.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")

    # Inicializar handlers
    try:
        # Acordar ClickHouse se estiver idle
        logger.info("Verificando se ClickHouse esta acordado...")
        if wake_clickhouse():
            logger.info("ClickHouse respondendo")
        else:
            logger.warning("ClickHouse pode estar lento para acordar")

        ch_config = get_clickhouse_config()
        ch_handler = ClickHouseHandler(
            host=ch_config["host"],
            port=ch_config["port"],
            user=ch_config["user"],
            password=ch_config["password"],
            database=DATABASE
        )
        logger.info(f"Conectado ao ClickHouse: {ch_config['host']}")
    except Exception as e:
        logger.error(f"Erro ao conectar ClickHouse: {e}")
        raise

    s3_handler = S3Handler()
    manifest = ManifestManager(MANIFEST_PATH)

    # Buscar ultima data no ClickHouse
    try:
        max_date_query = f"SELECT max(din_instante) FROM {DATABASE}.{TABLE}"
        result = ch_handler.client.query(max_date_query)
        last_date = result.result_rows[0][0] if result.result_rows else None

        if last_date:
            logger.info(f"Ultima data no ClickHouse: {last_date}")

            # Calcular dias de atraso
            days_behind = (today - last_date).days
            logger.info(f"Atraso: {days_behind} dias")
        else:
            logger.info(f"Tabela vazia - primeira execucao")
            last_date = None

    except Exception as e:
        logger.error(f"Erro ao buscar ultima data: {e}")
        raise

    # Identificar meses para processar
    months_to_process = get_months_to_process(last_date, today)

    logger.info(f"Meses para processar: {', '.join(months_to_process)}")
    logger.info("")

    # Processar cada mes
    results = []
    total_rows = 0

    for year_month in months_to_process:
        try:
            result = process_month_task(
                s3_handler=s3_handler,
                ch_handler=ch_handler,
                manifest=manifest,
                year_month=year_month,
                download_date=download_date
            )

            results.append(result)
            total_rows += result['rows_inserted']

        except Exception as e:
            logger.error(f"Erro processando {year_month}: {str(e)}")
            continue

    # Resumo
    logger.info("")
    logger.info("="*80)
    logger.info("RESUMO")
    logger.info("="*80)
    logger.info(f"Meses processados: {len(results)}")
    logger.info(f"Total registros inseridos: {total_rows:,}")

    # Estatisticas do ClickHouse
    try:
        stats_query = f"""
            SELECT
                count() as total_rows,
                min(din_instante) as min_date,
                max(din_instante) as max_date,
                count(DISTINCT toDate(din_instante)) as distinct_days
            FROM {DATABASE}.{TABLE}
        """
        stats = ch_handler.client.query(stats_query).result_rows[0]

        logger.info(f"Total no ClickHouse: {stats[0]:,} registros")
        logger.info(f"Periodo: {stats[1]} -> {stats[2]}")
        logger.info(f"Dias distintos: {stats[3]}")
    except Exception as e:
        logger.warning(f"Erro ao obter stats: {e}")

    logger.info("="*80)

    return {
        'months_processed': len(results),
        'total_rows': total_rows,
        'results': results
    }


# ============================================================================
# EXECUCAO LOCAL (TESTE)
# ============================================================================

if __name__ == "__main__":
    """
    Para testar localmente:
    python products/historico/pipelines/curtailment/solar_conjunto.py
    """
    daily_restricao_solar_tm()
