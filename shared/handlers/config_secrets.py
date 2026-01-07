"""
Carrega secrets do Prefect Cloud e gerencia conexão ClickHouse
"""
import time
import requests
from prefect.blocks.system import Secret

def get_clickhouse_config():
    """Retorna configuração do ClickHouse via Prefect Secrets"""
    return {
        "host": Secret.load("clickhouse-host").get(),
        "port": int(Secret.load("clickhouse-port").get()),
        "user": Secret.load("clickhouse-user").get(),
        "password": Secret.load("clickhouse-password").get(),
    }

def wake_clickhouse(max_retries=3, wait_seconds=15):
    """
    Acorda o ClickHouse Cloud se estiver idle.
    Faz ping até o serviço responder.
    """
    config = get_clickhouse_config()
    url = f"https://{config['host']}:{config['port']}/ping"
    
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                return True
        except Exception as e:
            print(f"Wake attempt {attempt}/{max_retries}: {e}")
        
        if attempt < max_retries:
            print(f"Aguardando {wait_seconds}s para ClickHouse acordar...")
            time.sleep(wait_seconds)
    
    return False
