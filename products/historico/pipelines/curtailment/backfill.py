"""
Backfill Pipeline - restricao_coff_eolica_tm
Carrega dados históricos mensais do S3 ONS para ClickHouse

IMPORTANTE: 
- Arquivos ONS são MENSAIS e INCREMENTAIS
- Cada mês é um arquivo que cresce diariamente
- Backfill carrega meses FECHADOS (até mês anterior)
- Daily pipeline carrega mês ATUAL
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

from utils import S3Handler, ClickHouseHandler, ManifestManager


def get_logger():
    """Retorna logger Prefect"""
    try:
        return get_run_logger()
    except:
        import logging
        return logging.getLogger(__name__)


# Configuração
load_dotenv('config/.env')

BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / 'data' / 'raw' / 'restricao_coff_eolica_tm'
MANIFEST_PATH = BASE_DIR / 'data' / 'processed' / 'restricao_coff_eolica_tm_manifest.json'

S3_PREFIX = 'dataset/restricao_coff_eolica_tm/'
TABLE_NAME = 'restricao_coff_eolica_tm'

# Padrão de arquivo ONS: RESTRICAO_COFF_EOLICA_YYYY_MM.parquet
FILE_PATTERN = re.compile(r'RESTRICAO_COFF_EOLICA_(\d{4})_(\d{2})\.parquet')


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
    
    Salva como: RESTRICAO_COFF_EOLICA_2024_12_20241230.parquet
                └─ nome original ──┘ └─ data download ─┘
    """
    
    try:
        # Nome versionado
        original_name = f"RESTRICAO_COFF_EOLICA_{year}_{month}"
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
    Task: Inserção INCREMENTAL no ClickHouse
    Insere apenas registros novos
    """
    
    try:
        # Buscar última data no ClickHouse para este mês
        max_date_query = f"""
        SELECT max(din_instante) as max_date
        FROM {TABLE_NAME}
        WHERE toYYYYMM(din_instante) = {year_month}
        """
        
        result = ch_handler.client.query(max_date_query)
        
        # Tratar NULL corretamente
        if result.result_rows and result.result_rows[0][0] is not None:
            max_date = result.result_rows[0][0]
            get_logger().info(f"Última data no CH: {max_date}")
        else:
            max_date = None
            get_logger().info(f"Primeira inserção para {year_month}")
        
        # Inserir (handler já filtra novos registros internamente)
        rows = ch_handler.insert_parquet_incremental(
            parquet_path=parquet_path,
            table=TABLE_NAME,
            source_file=source_file,
            max_date=max_date
        )
        
        return {'rows_inserted': rows, 'status': 'success'}
        
    except Exception as e:
        get_logger().error(f"Erro na inserção: {e}")
        raise


@task(name="Processar mês", log_prints=True, cache_policy=NONE)
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
    """Task: Processa um mês completo (download + insert incremental)"""
    
    year_month_key = f"{year}-{month}"
    get_logger().info(f"Processando: {year_month_key}")
    
    # Verificar se já foi processado hoje
    if not force and manifest.is_processed_today(year_month_key, download_date):
        get_logger().info(f"Já processado hoje (skip): {year_month_key}")
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
    name="Backfill restricao_coff_eolica_tm",
    description="Carrega dados históricos MENSAIS do ONS S3 para ClickHouse",
    task_runner=ConcurrentTaskRunner()
)
def backfill_restricao(
    start_month: str = "2023-10",
    end_month: Optional[str] = None,
    force: bool = False
):
    """
    Backfill de dados históricos MENSAIS
    
    Args:
        start_month: Mês início 'YYYY-MM' (default: Out/2023)
        end_month: Mês fim 'YYYY-MM' (default: mês anterior ao atual)
        force: Forçar reprocessamento de arquivos existentes
    
    Nota: Backfill carrega apenas meses FECHADOS (até mês anterior)
          Mês atual é carregado pelo daily pipeline
    """
    
    get_logger().info("="*80)
    get_logger().info("BACKFILL - restricao_coff_eolica_tm (MESES FECHADOS)")
    get_logger().info("="*80)
    
    # Data atual
    today = datetime.now()
    current_month = today.strftime('%Y-%m')
    download_date = today.strftime('%Y%m%d')
    
    # Configurar end_month (mês anterior por padrão)
    if end_month is None:
        if today.month == 1:
            end_month = f"{today.year - 1}-12"
        else:
            end_month = f"{today.year}-{today.month-1:02d}"
    
    get_logger().info(f"Período: {start_month} → {end_month}")
    get_logger().info(f"Mês atual (skip): {current_month}")
    get_logger().info(f"Data download: {download_date}")
    get_logger().info(f"Force reprocess: {force}")
    
    # Inicializar handlers
    s3_handler = S3Handler()
    
    ch_handler = ClickHouseHandler(
        host=os.getenv('CLICKHOUSE_HOST'),
        port=int(os.getenv('CLICKHOUSE_PORT', 8443)),
        user=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        database='dev_brazilgrid_raw'
    )
    
    manifest = ManifestManager(MANIFEST_PATH)
    
    # Listar arquivos no S3
    get_logger().info("Listando arquivos no S3 ONS...")
    s3_files = s3_handler.list_files(S3_PREFIX, suffix='.parquet')
    
    if not s3_files:
        get_logger().error("Nenhum arquivo encontrado no S3!")
        return
    
    get_logger().info(f"Total de arquivos no S3: {len(s3_files)}")
    
    # Parse e filtrar arquivos por período
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
        
        # Converter para número comparável (YYYYMM)
        file_ym = year_int * 100 + month_int
        start_ym = start_year * 100 + start_mon
        end_ym = end_year * 100 + end_mon
        
        # Filtrar período (excluir mês atual)
        if start_ym <= file_ym <= end_ym and f"{year}-{month}" != current_month:
            files_to_process.append((s3_key, year, month))
    
    get_logger().info(f"Arquivos no período: {len(files_to_process)}")
    
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
    
    start = "2023-10"  # Out/2023 (início do curtailment)
    end = None          # Mês anterior por padrão
    force = False
    
    if len(args) >= 1:
        start = args[0]
    if len(args) >= 2:
        end = args[1]
    if "--force" in args:
        force = True
    
    get_logger().info(f"Iniciando backfill: {start} → {end or 'mês anterior'}")
    
    backfill_restricao(
        start_month=start,
        end_month=end,
        force=force
    )
