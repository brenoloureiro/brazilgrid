"""
S3 Handler - Download de arquivos do S3 ONS
"""
import hashlib
from pathlib import Path
from typing import Optional
import boto3
from botocore import UNSIGNED
from botocore.config import Config

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



class S3Handler:
    """Gerencia download de arquivos do S3 ONS (acesso público)"""
    
    def __init__(self, bucket: str = "ons-aws-prod-opendata"):
        self.bucket = bucket
        # S3 público - sem credenciais
        self.s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        
    def list_files(self, prefix: str, suffix: str = ".parquet") -> list[str]:
        """Lista arquivos no S3 com filtro"""
        get_logger().info(f"Listando arquivos em s3://{self.bucket}/{prefix}")
        
        try:
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                get_logger().warning(f"Nenhum arquivo encontrado em {prefix}")
                return []
            
            files = [
                obj['Key'] for obj in response['Contents']
                if obj['Key'].endswith(suffix)
            ]
            
            get_logger().info(f"Encontrados {len(files)} arquivos")
            return sorted(files)
            
        except Exception as e:
            get_logger().error(f"Erro ao listar arquivos: {e}")
            raise
    
    def download_file(
        self, 
        s3_key: str, 
        local_path: Path,
        force: bool = False
    ) -> Optional[str]:
        """
        Download de arquivo do S3
        
        Args:
            s3_key: Chave do arquivo no S3
            local_path: Caminho local para salvar
            force: Forçar download mesmo se arquivo existe
            
        Returns:
            MD5 hash do arquivo ou None se skipado
        """
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Skip se já existe
        if local_path.exists() and not force:
            get_logger().info(f"Arquivo já existe (skip): {local_path.name}")
            return self._calculate_md5(local_path)
        
        try:
            get_logger().info(f"Baixando: {s3_key} → {local_path.name}")
            
            self.s3.download_file(
                Bucket=self.bucket,
                Key=s3_key,
                Filename=str(local_path)
            )
            
            md5_hash = self._calculate_md5(local_path)
            size_mb = local_path.stat().st_size / (1024 * 1024)
            
            get_logger().info(f"✅ Download completo: {size_mb:.2f} MB | MD5: {md5_hash[:8]}...")
            return md5_hash
            
        except Exception as e:
            get_logger().error(f"Erro no download: {e}")
            if local_path.exists():
                local_path.unlink()
            raise
    
    @staticmethod
    def _calculate_md5(file_path: Path) -> str:
        """Calcula MD5 hash de um arquivo"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
