"""
Logging configuration - Integração Loguru + Prefect
"""
import sys
import logging
from loguru import logger
from prefect import get_run_logger


class InterceptHandler(logging.Handler):
    """
    Intercepta logs do Python padrão e redireciona para Loguru
    """
    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


class PrefectHandler(logging.Handler):
    """
    Envia logs do Loguru para o Prefect Cloud
    """
    def emit(self, record):
        try:
            # Tentar obter o logger do Prefect
            prefect_logger = get_run_logger()
            
            # Mapear níveis
            level_map = {
                'TRACE': logging.DEBUG,
                'DEBUG': logging.DEBUG,
                'INFO': logging.INFO,
                'SUCCESS': logging.INFO,
                'WARNING': logging.WARNING,
                'ERROR': logging.ERROR,
                'CRITICAL': logging.CRITICAL,
            }
            
            log_level = level_map.get(record.levelname, logging.INFO)
            prefect_logger.log(log_level, record.getMessage())
            
        except Exception:
            # Se não estiver em contexto Prefect, ignora
            pass


def setup_logging(enable_prefect: bool = True):
    """
    Configura logging para desenvolvimento + produção
    
    Args:
        enable_prefect: Se True, envia logs para Prefect Cloud
    """
    
    # Remover handler padrão do Loguru
    logger.remove()
    
    # Handler para console (desenvolvimento)
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO",
        colorize=True
    )
    
    # Handler para Prefect Cloud (produção)
    if enable_prefect:
        # Adicionar handler Python padrão para enviar ao Prefect
        logging.basicConfig(
            handlers=[PrefectHandler()],
            level=logging.INFO,
            force=True
        )
        
        # Interceptar logs Python padrão e enviar para Loguru também
        logging.getLogger().addHandler(InterceptHandler())
        
        logger.info("Logging configurado: Console + Prefect Cloud")
    else:
        logger.info("Logging configurado: Console apenas")


def get_logger(name: str = None):
    """
    Retorna logger configurado
    
    Args:
        name: Nome do módulo (opcional)
        
    Returns:
        Logger instance
    """
    if name:
        return logger.bind(name=name)
    return logger
