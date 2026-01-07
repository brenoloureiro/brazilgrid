"""
Manifest Manager - Rastreabilidade de arquivos processados
"""
import json
from pathlib import Path
from datetime import datetime
from typing import Optional

# Prefect logger
try:
    from prefect import get_run_logger
    def get_logger():
        try:
            return get_run_logger()
        except:
            import logging
            return logging.getLogger(__name__)
except ImportError:
    import logging
    def get_logger():
        return logging.getLogger(__name__)



class ManifestManager:
    """Gerencia manifest de arquivos processados"""
    
    def __init__(self, manifest_path: Path):
        self.manifest_path = manifest_path
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self.data = self._load()
    
    def _load(self) -> dict:
        """Carrega manifest existente"""
        if self.manifest_path.exists():
            with open(self.manifest_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    
    def _save(self):
        """Salva manifest"""
        with open(self.manifest_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False, default=str)
    
    def add_month_entry(
        self,
        year_month: str,
        download_info: dict
    ):
        """
        Adiciona entrada MENSAL no manifest com versionamento
        
        Args:
            year_month: Mês no formato 'YYYY-MM'
            download_info: Dict com informações do download
                {
                    'file_name': str,
                    'original_name': str,
                    'download_date': str,
                    'file_size_mb': float,
                    'md5': str,
                    'rows_inserted': int,
                    's3_key': str,
                    'status': str
                }
        """
        if year_month not in self.data:
            self.data[year_month] = {'downloads': []}
        
        # Adicionar download ao histórico
        self.data[year_month]['downloads'].append({
            **download_info,
            'ingested_at': datetime.now().isoformat()
        })
        
        # Atualizar último download
        self.data[year_month]['last_download'] = download_info['download_date']
        self.data[year_month]['last_md5'] = download_info['md5']
        
        self._save()
        get_logger().info(f"Manifest atualizado: {year_month}")
    
    def is_processed_today(self, year_month: str, today: str) -> bool:
        """
        Verifica se mês já foi processado hoje
        
        Args:
            year_month: Mês 'YYYY-MM'
            today: Data hoje 'YYYYMMDD'
            
        Returns:
            True se já foi processado hoje
        """
        if year_month not in self.data:
            return False
        
        return self.data[year_month].get('last_download') == today
    
    def get_month_history(self, year_month: str) -> list:
        """Retorna histórico de downloads de um mês"""
        if year_month not in self.data:
            return []
        return self.data[year_month].get('downloads', [])
    
    def detect_changes(self, year_month: str, new_md5: str) -> bool:
        """
        Detecta se arquivo mudou desde último download
        
        Args:
            year_month: Mês 'YYYY-MM'
            new_md5: MD5 do novo download
            
        Returns:
            True se MD5 mudou (arquivo foi alterado pelo ONS)
        """
        if year_month not in self.data:
            return False
        
        last_md5 = self.data[year_month].get('last_md5')
        
        if last_md5 and last_md5 != new_md5:
            get_logger().warning(f"⚠️  MUDANÇA DETECTADA em {year_month}!")
            get_logger().warning(f"MD5 anterior: {last_md5[:8]}...")
            get_logger().warning(f"MD5 novo: {new_md5[:8]}...")
            return True
        
        return False
    
    def add_entry(
        self,
        date: str,
        file_info: dict
    ):
        """
        Adiciona entrada no manifest
        
        Args:
            date: Data no formato 'YYYY-MM-DD'
            file_info: Dict com informações do arquivo
                {
                    'file_name': str,
                    'file_size_mb': float,
                    'md5': str,
                    'rows': int,
                    's3_key': str,
                    'ingested_at': datetime,
                    'status': str
                }
        """
        self.data[date] = {
            **file_info,
            'ingested_at': datetime.now().isoformat()
        }
        self._save()
        get_logger().info(f"Manifest atualizado: {date}")
    
    def get_entry(self, date: str) -> Optional[dict]:
        """Retorna entrada do manifest para uma data"""
        return self.data.get(date)
    
    def is_processed(self, date: str) -> bool:
        """Verifica se data já foi processada"""
        entry = self.get_entry(date)
        return entry is not None and entry.get('status') == 'success'
    
    def get_stats(self) -> dict:
        """Retorna estatísticas do manifest"""
        if not self.data:
            return {
                'total_files': 0,
                'total_rows': 0,
                'total_size_mb': 0,
                'date_range': None
            }
        
        dates = sorted(self.data.keys())
        total_rows = sum(entry.get('rows', 0) for entry in self.data.values())
        total_size = sum(entry.get('file_size_mb', 0) for entry in self.data.values())
        
        return {
            'total_files': len(self.data),
            'total_rows': total_rows,
            'total_size_mb': round(total_size, 2),
            'date_range': f"{dates[0]} → {dates[-1]}" if dates else None
        }
    
    def list_missing_dates(self, start_date: str, end_date: str) -> list[str]:
        """
        Lista datas faltantes no intervalo
        
        Args:
            start_date: Data início 'YYYY-MM-DD'
            end_date: Data fim 'YYYY-MM-DD'
            
        Returns:
            Lista de datas faltantes
        """
        from datetime import datetime, timedelta
        
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        missing = []
        current = start
        
        while current <= end:
            date_str = current.strftime('%Y-%m-%d')
            if not self.is_processed(date_str):
                missing.append(date_str)
            current += timedelta(days=1)
        
        return missing
