"""
Serve dos pipelines diarios com agendamento
- Eolico: restricao_coff_eolica_tm
- Solar: restricao_coff_fotovoltaica_tm
"""
from prefect import serve

from products.historico.pipelines.curtailment.eolico_conjunto import daily_restricao_tm
from products.historico.pipelines.curtailment.solar_conjunto import daily_restricao_solar_tm

if __name__ == "__main__":
    # Criar deployments para ambos os pipelines
    eolico_deployment = daily_restricao_tm.to_deployment(
        name="daily-restricao-eolica",
        cron="0 8 * * *",  # Todo dia as 08:00 UTC (05:00 BRT)
        tags=["brazilgrid", "ons", "eolica"]
    )

    solar_deployment = daily_restricao_solar_tm.to_deployment(
        name="daily-restricao-solar",
        cron="0 8 * * *",  # Todo dia as 08:00 UTC (05:00 BRT)
        tags=["brazilgrid", "ons", "solar"]
    )

    # Servir ambos os pipelines
    serve(eolico_deployment, solar_deployment)
