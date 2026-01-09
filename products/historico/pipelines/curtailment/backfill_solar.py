"""
Backfill Pipeline - restricao_coff_fotovoltaica_tm
Carrega dados historicos mensais do S3 ONS para ClickHouse

IMPORTANTE:
- Arquivos ONS sao MENSAIS e INCREMENTAIS
- Cada mes e um arquivo que cresce diariamente
- Backfill carrega meses FECHADOS (ate mes anterior)
- Daily pipeline carrega mes ATUAL
- Dados solares comecam em Abril/2024
"""
import os
import re
from pathlib import Path
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from prefect.cache_policies import NONE
from prefect.task_runners import ConcurrentTaskRunner

from shared.handlers import S3Handler, ClickHouseHandler, ManifestManager
from shared.handlers.config_secrets import get_clickhouse_config


def get_logger():
    """Retorna logger Prefect"""
    try:
        return get_run_logger()
    except:
        import logging
        return logging.getLogger(__name__)


# Configuracao
load_dotenv('config/.env')

BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / 'data' / 'raw' / 'restricao_coff_fotovoltaica_tm'
MANIFEST_PATH = BASE_DIR / 'data' / 'processed' / 'restricao_coff_fotovoltaica_tm_manifest.json'

S3_PREFIX = 'dataset/restricao_coff_fotovoltaica_tm/'
TABLE_NAME = 'curtailment_solar_conjunto'

# Padrao de arquivo ONS: RESTRICAO_COFF_FOTOVOLTAICA_YYYY_MM.parquet
FILE_PATTERN = re.compile(r'RESTRICAO_COFF_FOTOVOLTAICA_(\d{4})_(\d{2})\.parquet')


@task(name="Download arquivo S3", retries=3, retry_delay_seconds=10, cache_policy=NONE)
def download_file_task(
    s3_handler: S3Handler,
    s3_key: str,
    year: str,
    month: str,
    download_date: str,
    force: bool = False
) -> Optional[dict]:
    """
    Task: Download de arquivo do S3 com versionamento

    Salva como: RESTRICAO_COFF_FOTOVOLTAICA_2024_04_20241230.parquet
                |-- nome original --|  |-- data download --|
    """

    try:
        # Nome versionado
        original_name = f"RESTRICAO_COFF_FOTOVOLTAICA_{year}_{month}"
        versioned_name = f"{original_name}_{download_date}.parquet"
        local_path = DATA_DIR / versioned_name

        md5_hash = s3_handler.download_file(s3_key, local_path, force=force)

        if md5_hash is None:
            return None

        file_size_mb = local_path.stat().st_size / (1024 * 1024)

        return {
            'file_name': versioned_name,
            'original_name': f"{original_name}.parquet",
            'file_size_mb': round(file_size_mb, 2),
            'md5': md5_hash,
            's3_key': s3_key,
            'local_path': str(local_path),
            'download_date': download_date
        }

    except Exception as e:
        get_logger().error(f"Erro no download: {e}")
        raise


@task(name="Inserir no ClickHouse", retries=2, retry_delay_seconds=5, cache_policy=NONE)
def insert_clickhouse_task(
    ch_handler: ClickHouseHandler,
    parquet_path: Path,
    source_file: str,
    year_month: str
) -> dict:
    """
    Task: Insercao INCREMENTAL no ClickHouse
    Insere apenas registros novos
    """

    try:
        # Buscar ultima data no ClickHouse para este mes
        max_date_query = f"""
        SELECT max(din_instante) as max_date
        FROM {TABLE_NAME}
        WHERE toYYYYMM(din_instante) = {year_month}
        """

        result = ch_handler.client.query(max_date_query)

        # Tratar NULL corretamente
        if result.result_rows and result.result_rows[0][0] is not None:
            max_date = result.result_rows[0][0]
            # Ignorar datas invalidas (1970)
            if max_date.year < 2020:
                max_date = None
                get_logger().info(f"Primeira insercao para {year_month}")
            else:
                get_logger().info(f"Ultima data no CH: {max_date}")
        else:
            max_date = None
            get_logger().info(f"Primeira insercao para {year_month}")

        # Inserir (handler ja filtra novos registros internamente)
        rows = ch_handler.insert_parquet_incremental(
            parquet_path=parquet_path,
            table=TABLE_NAME,
            source_file=source_file,
            max_date=max_date
        )

        return {'rows_inserted': rows, 'status': 'success'}

    except Exception as e:
        get_logger().error(f"Erro na insercao: {e}")
        raise


@task(name="Processar mes", log_prints=True, cache_policy=NONE)
def process_month(
    s3_handler: S3Handler,
    ch_handler: ClickHouseHandler,
    manifest: ManifestManager,
    s3_key: str,
    year: str,
    month: str,
    download_date: str,
    force: bool = False
) -> dict:
    """Task: Processa um mes completo (download + insert incremental)"""

    year_month_key = f"{year}-{month}"
    get_logger().info(f"Processando: {year_month_key}")

    # Verificar se ja foi processado hoje
    if not force and manifest.is_processed_today(year_month_key, download_date):
        get_logger().info(f"Ja processado hoje (skip): {year_month_key}")
        return {'status': 'skipped', 'year_month': year_month_key}

    # Download versionado
    download_info = download_file_task(
        s3_handler=s3_handler,
        s3_key=s3_key,
        year=year,
        month=month,
        download_date=download_date,
        force=force
    )

    if download_info is None:
        return {'status': 'skipped', 'year_month': year_month_key}

    # Insert incremental ClickHouse
    local_path = Path(download_info['local_path'])
    year_month_num = f"{year}{month}"

    insert_result = insert_clickhouse_task(
        ch_handler=ch_handler,
        parquet_path=local_path,
        source_file=download_info['file_name'],
        year_month=year_month_num
    )

    # Atualizar manifest
    manifest.add_month_entry(
        year_month=year_month_key,
        download_info={
            **download_info,
            **insert_result
        }
    )

    return {
        'status': 'success',
        'year_month': year_month_key,
        'rows_inserted': insert_result['rows_inserted'],
        'md5': download_info['md5']
    }


@flow(
    name="Backfill restricao_coff_fotovoltaica_tm",
    description="Carrega dados historicos MENSAIS do ONS S3 para ClickHouse",
    task_runner=ConcurrentTaskRunner()
)
def backfill_restricao_solar(
    start_month: str = "2024-04",
    end_month: Optional[str] = None,
    force: bool = False
):
    """
    Backfill de dados historicos MENSAIS - SOLAR

    Args:
        start_month: Mes inicio 'YYYY-MM' (default: Abr/2024 - inicio dados solares)
        end_month: Mes fim 'YYYY-MM' (default: mes anterior ao atual)
        force: Forcar reprocessamento de arquivos existentes

    Nota: Backfill carrega apenas meses FECHADOS (ate mes anterior)
          Mes atual e carregado pelo daily pipeline
    """

    get_logger().info("="*80)
    get_logger().info("BACKFILL - restricao_coff_fotovoltaica_tm (MESES FECHADOS)")
    get_logger().info("="*80)

    # Data atual
    today = datetime.now()
    current_month = today.strftime('%Y-%m')
    download_date = today.strftime('%Y%m%d')

    # Configurar end_month (mes anterior por padrao)
    if end_month is None:
        if today.month == 1:
            end_month = f"{today.year - 1}-12"
        else:
            end_month = f"{today.year}-{today.month-1:02d}"

    get_logger().info(f"Periodo: {start_month} -> {end_month}")
    get_logger().info(f"Mes atual (skip): {current_month}")
    get_logger().info(f"Data download: {download_date}")
    get_logger().info(f"Force reprocess: {force}")

    # Criar diretorio de dados se nao existir
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Inicializar handlers
    s3_handler = S3Handler()

    ch_config = get_clickhouse_config()
    ch_handler = ClickHouseHandler(
        host=ch_config["host"],
        port=ch_config["port"],
        user=ch_config["user"],
        password=ch_config["password"],
        database='brazilgrid_historico'
    )

    manifest = ManifestManager(MANIFEST_PATH)

    # Listar arquivos no S3
    get_logger().info("Listando arquivos no S3 ONS...")
    s3_files = s3_handler.list_files(S3_PREFIX, suffix='.parquet')

    if not s3_files:
        get_logger().error("Nenhum arquivo encontrado no S3!")
        return

    get_logger().info(f"Total de arquivos no S3: {len(s3_files)}")

    # Parse e filtrar arquivos por periodo
    files_to_process = []
    start_year, start_mon = map(int, start_month.split('-'))
    end_year, end_mon = map(int, end_month.split('-'))

    for s3_key in s3_files:
        match = FILE_PATTERN.search(s3_key)
        if not match:
            get_logger().warning(f"Arquivo com formato inesperado: {s3_key}")
            continue

        year, month = match.groups()
        year_int, month_int = int(year), int(month)

        # Converter para numero comparavel (YYYYMM)
        file_ym = year_int * 100 + month_int
        start_ym = start_year * 100 + start_mon
        end_ym = end_year * 100 + end_mon

        # Filtrar periodo (excluir mes atual)
        if start_ym <= file_ym <= end_ym and f"{year}-{month}" != current_month:
            files_to_process.append((s3_key, year, month))

    get_logger().info(f"Arquivos no periodo: {len(files_to_process)}")

    if not files_to_process:
        get_logger().warning("Nenhum arquivo para processar!")
        return

    # Processar meses
    get_logger().info("Iniciando processamento...")

    results = []
    success_count = 0
    skip_count = 0
    error_count = 0
    total_rows = 0

    for s3_key, year, month in sorted(files_to_process):
        try:
            result = process_month(
                s3_handler=s3_handler,
                ch_handler=ch_handler,
                manifest=manifest,
                s3_key=s3_key,
                year=year,
                month=month,
                download_date=download_date,
                force=force
            )

            results.append(result)

            if result['status'] == 'success':
                success_count += 1
                total_rows += result.get('rows_inserted', 0)
            elif result['status'] == 'skipped':
                skip_count += 1

        except Exception as e:
            get_logger().error(f"Erro processando {year}-{month}: {e}")
            error_count += 1
            continue

    # Resumo
    get_logger().info("="*80)
    get_logger().info("RESUMO DO BACKFILL")
    get_logger().info("="*80)
    get_logger().info(f"Total meses: {len(files_to_process)}")
    get_logger().info(f"Sucesso: {success_count}")
    get_logger().info(f"Pulados: {skip_count}")
    get_logger().info(f"Erros: {error_count}")
    get_logger().info(f"Total registros inseridos: {total_rows:,}")

    # Stats do manifest
    manifest_stats = manifest.get_stats()
    get_logger().info(f"Manifest: {manifest_stats}")

    # Stats do ClickHouse
    try:
        ch_stats = ch_handler.get_stats(TABLE_NAME)
        get_logger().info(f"ClickHouse: {ch_stats}")
    except Exception as e:
        get_logger().warning(f"Erro ao obter stats ClickHouse: {e}")

    get_logger().info("="*80)

    return {
        'success': success_count,
        'skipped': skip_count,
        'errors': error_count,
        'total_rows': total_rows
    }


if __name__ == "__main__":
    import sys

    # Argumentos via CLI
    args = sys.argv[1:]

    start = "2024-04"  # Abr/2024 (inicio dados solares ONS)
    end = None          # Mes anterior por padrao
    force = False

    if len(args) >= 1:
        start = args[0]
    if len(args) >= 2:
        end = args[1]
    if "--force" in args:
        force = True

    get_logger().info(f"Iniciando backfill solar: {start} -> {end or 'mes anterior'}")

    backfill_restricao_solar(
        start_month=start,
        end_month=end,
        force=force
    )
