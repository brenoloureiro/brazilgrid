"""
ClickHouse Handler - Inserção de dados com Polars
"""
from pathlib import Path
from datetime import datetime
from typing import Optional, List
import polars as pl
import pandas as pd
import clickhouse_connect
from prefect import get_run_logger

def get_logger():
    """Retorna logger Prefect se disponível, senão usa print"""
    try:
        return get_run_logger()
    except Exception:
        # Fallback para quando não está em contexto Prefect
        import logging
        return logging.getLogger(__name__)


class ClickHouseHandler:
    """Gerencia inserção de dados no ClickHouse"""
    
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
            secure=True
        )
        get_logger().info(f"Conectado ao ClickHouse: {host}")
    
    def insert_parquet_incremental(
        self,
        parquet_path: Path,
        table: str,
        source_file: str,
        max_date: Optional[datetime] = None,
        required_cols: Optional[List[str]] = None
    ) -> int:
        """
        Insere dados INCREMENTALMENTE de arquivo Parquet no ClickHouse
        Usa Polars para melhor performance e timezone handling
        
        Args:
            parquet_path: Caminho do arquivo Parquet
            table: Nome da tabela
            source_file: Nome do arquivo original (para metadata)
            max_date: Data máxima já existente no CH (inserir apenas > max_date)
            required_cols: Lista de colunas obrigatórias (None = padrão conjunto)
            
        Returns:
            Número de registros inseridos
        """
        get_logger().info(f"Lendo Parquet com Polars: {parquet_path.name}")
        
        try:
            # Ler Parquet com Polars
            df = pl.read_parquet(parquet_path)
            rows_total = len(df)
            
            get_logger().info(f"Parquet carregado: {rows_total:,} registros")
            
            # LIMPEZA: Substituir strings vazias por None em colunas numéricas
            # Fix para mudança de schema ONS a partir de Ago/2025
            numeric_cols = [
                'val_geracao', 'val_geracaolimitada', 'val_disponibilidade',
                'val_geracaoreferencia', 'val_geracaoreferenciafinal'
            ]
            for col in numeric_cols:
                if col in df.columns:
                    # Se coluna for String, limpar e converter
                    if df[col].dtype in [pl.Utf8, pl.String]:
                        df = df.with_columns(
                            pl.when(pl.col(col) == '').then(None).otherwise(pl.col(col)).alias(col)
                        )
                        df = df.with_columns(pl.col(col).cast(pl.Float64))
            
            # Validar colunas obrigatórias (default: conjunto, pode ser override)
            if required_cols is None:
                required_cols = [
                    'id_subsistema', 'nom_subsistema', 'id_estado', 'nom_estado',
                    'id_ons', 'ceg', 'din_instante', 'val_geracao'
                ]
            
            if required_cols:  # Se lista não vazia, validar
                missing_cols = set(required_cols) - set(df.columns)
                if missing_cols:
                    raise ValueError(f"Colunas obrigatórias ausentes: {missing_cols}")
            
            # BEST PRACTICE: Armazenar em UTC
            # ONS envia dados naive em BRT (UTC-3)
            # 1. Marcar como BRT
            # 2. Converter para UTC
            
            df = df.with_columns([
                pl.col('din_instante')
                  .dt.replace_time_zone('America/Sao_Paulo')  # Marcar como BRT
                  .dt.convert_time_zone('UTC')                 # Converter para UTC
            ])
            
            get_logger().info("Dados convertidos: BRT → UTC")
            
            # Converter para Pandas para insert
            df_pandas = df.to_pandas()
            
            get_logger().info(f"Dtype final: {df_pandas['din_instante'].dtype}, TZ: {df_pandas['din_instante'].dt.tz}")
            
            # Filtrar apenas novos registros
            if max_date:
                # Converter max_date para UTC com timezone
                max_date_utc = pl.lit(max_date).cast(pl.Datetime('ns', 'UTC'))
                df = df.filter(pl.col('din_instante') > max_date_utc)
                get_logger().info(f"Filtrado > {max_date}: {len(df):,} registros novos")
            
            if len(df) == 0:
                get_logger().info("Nenhum registro novo para inserir")
                return 0
            
            # Adicionar metadados
            df = df.with_columns([
                pl.lit(source_file).alias('_source_file')
            ])
            
            # Inserir no ClickHouse
            get_logger().info(f"Inserindo no ClickHouse: {table}")
            
            self.client.insert_df(
                table=table,
                df=df_pandas
            )
            
            get_logger().info(f"✅ Inserção completa: {len(df):,} novos registros → {table}")
            return len(df)
            
        except Exception as e:
            get_logger().error(f"Erro na inserção incremental: {e}")
            raise
    
    def insert_parquet(
        self,
        parquet_path: Path,
        table: str,
        source_file: str
    ) -> int:
        """
        Insere dados COMPLETOS de arquivo Parquet no ClickHouse
        Usa Polars para melhor performance
        
        Args:
            parquet_path: Caminho do arquivo Parquet
            table: Nome da tabela (ex: restricao_coff_eolica_tm)
            source_file: Nome do arquivo original (para metadata)
            
        Returns:
            Número de registros inseridos
        """
        get_logger().info(f"Lendo Parquet com Polars: {parquet_path.name}")
        
        try:
            # Ler Parquet com Polars
            df = pl.read_parquet(parquet_path)
            rows_before = len(df)
            
            get_logger().info(f"Parquet carregado: {rows_before:,} registros")
            
            # Validar colunas obrigatórias (default: conjunto, pode ser override)
            if required_cols is None:
                required_cols = [
                    'id_subsistema', 'nom_subsistema', 'id_estado', 'nom_estado',
                    'id_ons', 'ceg', 'din_instante', 'val_geracao'
                ]
            
            if required_cols:  # Se lista não vazia, validar
                missing_cols = set(required_cols) - set(df.columns)
                if missing_cols:
                    raise ValueError(f"Colunas obrigatórias ausentes: {missing_cols}")
            
            # Adicionar metadados
            df = df.with_columns([
                pl.lit(source_file).alias('_source_file')
            ])
            
            # Converter BRT → UTC
            df = df.with_columns([
                pl.col('din_instante')
                  .dt.replace_time_zone('America/Sao_Paulo')
                  .dt.convert_time_zone('UTC')
            ])
            
            # Converter para Pandas
            df_pandas = df.to_pandas()
            
            # Inserir no ClickHouse
            get_logger().info(f"Inserindo no ClickHouse: {table}")
            
            self.client.insert_df(
                table=table,
                df=df_pandas
            )
            
            get_logger().info(f"✅ Inserção completa: {rows_before:,} registros → {table}")
            return rows_before
            
        except Exception as e:
            get_logger().error(f"Erro na inserção: {e}")
            raise
    
    def check_duplicate(
        self,
        table: str,
        date: str
    ) -> bool:
        """
        Verifica se já existem dados para uma data específica
        
        Args:
            table: Nome da tabela
            date: Data no formato 'YYYY-MM-DD'
            
        Returns:
            True se já existem dados
        """
        query = f"""
        SELECT count() as total
        FROM {table}
        WHERE toDate(din_instante) = '{date}'
        """
        
        result = self.client.query(query)
        count = result.result_rows[0][0]
        
        return count > 0
    
    def get_stats(self, table: str) -> dict:
        """Retorna estatísticas da tabela"""
        query = f"""
        SELECT
            count() as total_rows,
            min(din_instante) as min_date,
            max(din_instante) as max_date,
            count(DISTINCT toDate(din_instante)) as distinct_days
        FROM {table}
        """
        
        result = self.client.query(query)
        row = result.result_rows[0]
        
        return {
            'total_rows': row[0],
            'min_date': row[1],
            'max_date': row[2],
            'distinct_days': row[3]
        }
