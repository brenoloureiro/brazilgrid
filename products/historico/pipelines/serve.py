"""
Serve do pipeline diário com agendamento
"""
from products.historico.pipelines.curtailment.eolico_conjunto import daily_restricao_tm

if __name__ == "__main__":
    daily_restricao_tm.serve(
        name="daily-restricao-eolica",
        cron="0 8 * * *",  # Todo dia às 08:00 UTC (05:00 BRT)
        tags=["brazilgrid", "ons", "eolica"]
    )
