"""
Serve dos pipelines diarios com agendamento
- Eolico Conjunto: restricao_coff_eolica_tm
- Eolico Usina: restricao_coff_eolica_detail_tm
- Solar Conjunto: restricao_coff_fotovoltaica_tm
- Solar Usina: restricao_coff_fotovoltaica_detail_tm
"""
from prefect import serve

from products.historico.pipelines.curtailment.eolico_conjunto import daily_restricao_tm
from products.historico.pipelines.curtailment.eolico_usina import daily_restricao_usina
from products.historico.pipelines.curtailment.solar_conjunto import daily_restricao_solar_tm
from products.historico.pipelines.curtailment.solar_usina import daily_restricao_solar_usina

if __name__ == "__main__":
    # Criar deployments para os pipelines
    eolico_deployment = daily_restricao_tm.to_deployment(
        name="daily-restricao-eolica",
        cron="0 8 * * *",  # Todo dia as 08:00 UTC (05:00 BRT)
        tags=["brazilgrid", "ons", "eolica"]
    )

    eolico_usina_deployment = daily_restricao_usina.to_deployment(
        name="daily-restricao-eolica-usina",
        cron="0 8 * * *",  # Todo dia as 08:00 UTC (05:00 BRT)
        tags=["brazilgrid", "ons", "eolica", "usina"]
    )

    solar_deployment = daily_restricao_solar_tm.to_deployment(
        name="daily-restricao-solar",
        cron="0 8 * * *",  # Todo dia as 08:00 UTC (05:00 BRT)
        tags=["brazilgrid", "ons", "solar"]
    )

    solar_usina_deployment = daily_restricao_solar_usina.to_deployment(
        name="daily-restricao-solar-usina",
        cron="0 8 * * *",  # Todo dia as 08:00 UTC (05:00 BRT)
        tags=["brazilgrid", "ons", "solar", "usina"]
    )

    # Servir todos os pipelines
    serve(eolico_deployment, eolico_usina_deployment, solar_deployment, solar_usina_deployment)
