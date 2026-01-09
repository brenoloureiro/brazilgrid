"""
BrazilGrid - Dashboard de Curtailment
MVP v0.3 - Eolico + Solar
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import clickhouse_connect

st.set_page_config(
    page_title="BrazilGrid - Curtailment",
    page_icon="⚡",
    layout="wide"
)

FONTES = {
    "Eolica": {"tabela": "curtailment_eolico_conjunto", "icone": "🌬️", "cor": "Reds"},
    "Solar": {"tabela": "curtailment_solar_conjunto", "icone": "☀️", "cor": "Oranges"}
}

def get_clickhouse_config():
    try:
        return dict(st.secrets["clickhouse"])
    except Exception:
        from shared.handlers.config_secrets import get_clickhouse_config as prefect_config
        return prefect_config()

@st.cache_resource
def get_client():
    config = get_clickhouse_config()
    return clickhouse_connect.get_client(
        host=config["host"], port=config["port"],
        user=config["user"], password=config["password"], secure=True
    )

@st.cache_data(ttl=3600)
def load_summary_data(tabela):
    client = get_client()
    query = f"""SELECT count(), min(din_instante), max(din_instante),
        count(DISTINCT id_ons), count(DISTINCT id_estado)
        FROM brazilgrid_historico.{tabela}"""
    return client.query(query).result_rows[0]

@st.cache_data(ttl=3600)
def load_estados(tabela):
    client = get_client()
    query = f"SELECT DISTINCT id_estado FROM brazilgrid_historico.{tabela} ORDER BY id_estado"
    return [row[0] for row in client.query(query).result_rows]

@st.cache_data(ttl=3600)
def load_curtailment_by_state(tabela, data_inicio, data_fim):
    client = get_client()
    query = f"""SELECT id_estado,
        round(sum(val_geracaoreferencia) / 2, 2),
        round(sum(val_geracao) / 2, 2),
        round(sum(CASE WHEN cod_razaorestricao IS NOT NULL AND cod_razaorestricao != ''
            THEN greatest(val_geracaoreferencia - val_geracao, 0) ELSE 0 END) / 2, 2)
        FROM brazilgrid_historico.{tabela}
        WHERE din_instante >= '{data_inicio}' AND din_instante <= '{data_fim}'
        GROUP BY id_estado ORDER BY 4 DESC"""
    return pd.DataFrame(client.query(query).result_rows,
        columns=['Estado', 'Geracao Ref (MWh)', 'Geracao Real (MWh)', 'Curtailment (MWh)'])

@st.cache_data(ttl=3600)
def load_curtailment_timeline(tabela, data_inicio, data_fim, estado=None, granularidade='auto'):
    client = get_client()
    where_estado = f"AND id_estado = '{estado}'" if estado else ""
    dias = (data_fim - data_inicio).days
    if granularidade == 'auto':
        agg, label = ('toStartOfMonth', 'Mes') if dias > 90 else ('toStartOfWeek', 'Semana') if dias > 30 else ('toDate', 'Dia')
    elif granularidade == 'mensal':
        agg, label = 'toStartOfMonth', 'Mes'
    elif granularidade == 'semanal':
        agg, label = 'toStartOfWeek', 'Semana'
    else:
        agg, label = 'toDate', 'Dia'
    query = f"""SELECT {agg}(din_instante),
        round(sum(CASE WHEN cod_razaorestricao IS NOT NULL AND cod_razaorestricao != ''
            THEN greatest(val_geracaoreferencia - val_geracao, 0) ELSE 0 END) / 2, 2)
        FROM brazilgrid_historico.{tabela}
        WHERE din_instante >= '{data_inicio}' AND din_instante <= '{data_fim}' {where_estado}
        GROUP BY 1 ORDER BY 1"""
    return pd.DataFrame(client.query(query).result_rows, columns=['Periodo', 'Curtailment (MWh)']), label

@st.cache_data(ttl=3600)
def load_curtailment_by_reason(tabela, data_inicio, data_fim):
    client = get_client()
    query = f"""SELECT cod_razaorestricao,
        round(sum(greatest(val_geracaoreferencia - val_geracao, 0)) / 2, 2)
        FROM brazilgrid_historico.{tabela}
        WHERE din_instante >= '{data_inicio}' AND din_instante <= '{data_fim}'
          AND cod_razaorestricao IS NOT NULL AND cod_razaorestricao != ''
        GROUP BY 1 ORDER BY 2 DESC"""
    df = pd.DataFrame(client.query(query).result_rows, columns=['Razao', 'Curtailment (MWh)'])
    razao_map = {'CNF': 'Confiabilidade', 'ENE': 'Razao Energetica', 'REL': 'Indisp. Externa', 'PAR': 'Parecer Acesso'}
    df['Razao'] = df['Razao'].map(lambda x: razao_map.get(x, x))
    return df

@st.cache_data(ttl=3600)
def load_total_curtailment(tabela, data_inicio, data_fim):
    client = get_client()
    query = f"""SELECT
        round(sum(CASE WHEN cod_razaorestricao IS NOT NULL AND cod_razaorestricao != ''
            THEN greatest(val_geracaoreferencia - val_geracao, 0) ELSE 0 END) / 2, 2),
        round(sum(val_geracao) / 2, 2),
        round(sum(val_geracaoreferencia) / 2, 2)
        FROM brazilgrid_historico.{tabela}
        WHERE din_instante >= '{data_inicio}' AND din_instante <= '{data_fim}'"""
    return client.query(query).result_rows[0]

# Interface
st.sidebar.header("Filtros")
fonte_selecionada = st.sidebar.radio("Fonte de Energia", list(FONTES.keys()), horizontal=True)
fonte_config = FONTES[fonte_selecionada]
tabela, icone, cor = fonte_config["tabela"], fonte_config["icone"], fonte_config["cor"]

st.title(f"{icone} BrazilGrid - Curtailment {fonte_selecionada}")
st.markdown(f"Analise de restricoes de geracao {fonte_selecionada.lower()} no Brasil (Fonte: ONS)")

try:
    summary = load_summary_data(tabela)
    estados = load_estados(tabela)

    periodo_preset = st.sidebar.selectbox("Periodo",
        ["Ultimos 30 dias", "Ultimos 90 dias", "Ultimo ano", "Todo historico", "Personalizado"])

    if periodo_preset == "Ultimos 30 dias":
        data_fim, data_inicio = datetime.now().date(), datetime.now().date() - timedelta(days=30)
    elif periodo_preset == "Ultimos 90 dias":
        data_fim, data_inicio = datetime.now().date(), datetime.now().date() - timedelta(days=90)
    elif periodo_preset == "Ultimo ano":
        data_fim, data_inicio = datetime.now().date(), datetime.now().date() - timedelta(days=365)
    elif periodo_preset == "Todo historico":
        data_inicio, data_fim = summary[1].date(), summary[2].date()
    else:
        col1, col2 = st.sidebar.columns(2)
        data_inicio = col1.date_input("Data Inicio", datetime.now().date() - timedelta(days=30))
        data_fim = col2.date_input("Data Fim", datetime.now().date())

    estado_selecionado = st.sidebar.selectbox("Estado", ["Todos"] + estados)
    estado_filtro = None if estado_selecionado == "Todos" else estado_selecionado
    granularidade = st.sidebar.selectbox("Granularidade", ["auto", "diario", "semanal", "mensal"])

    totais = load_total_curtailment(tabela, data_inicio, data_fim)
    curtailment_total, geracao_total, referencia_total = totais[0] or 0, totais[1] or 0, totais[2] or 0
    percentual = (curtailment_total / referencia_total * 100) if referencia_total > 0 else 0

    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Curtailment Total", f"{curtailment_total/1000:,.1f} GWh")
    col2.metric("Geracao Verificada", f"{geracao_total/1000:,.1f} GWh")
    col3.metric("Geracao Referencia", f"{referencia_total/1000:,.1f} GWh")
    col4.metric("% Curtailment", f"{percentual:.1f}%")

    st.markdown("---")
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Curtailment por Estado")
        df_estados = load_curtailment_by_state(tabela, data_inicio, data_fim)
        fig = px.bar(df_estados, x='Estado', y='Curtailment (MWh)', color='Curtailment (MWh)', color_continuous_scale=cor)
        fig.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Curtailment por Razao")
        df_razao = load_curtailment_by_reason(tabela, data_inicio, data_fim)
        fig = px.pie(df_razao, values='Curtailment (MWh)', names='Razao', color_discrete_sequence=px.colors.qualitative.Set2)
        st.plotly_chart(fig, use_container_width=True)

    df_timeline, label_tempo = load_curtailment_timeline(tabela, data_inicio, data_fim, estado_filtro, granularidade)
    titulo = f"Curtailment por {label_tempo}" + (f" - {estado_selecionado}" if estado_selecionado != "Todos" else "")
    st.subheader(titulo)
    fig = px.bar(df_timeline, x='Periodo', y='Curtailment (MWh)', color='Curtailment (MWh)', color_continuous_scale=cor)
    fig.update_layout(showlegend=False, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Ver dados por estado"):
        st.dataframe(df_estados, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Erro ao conectar: {e}")
    st.info("Verifique as credenciais em Settings > Secrets no Streamlit Cloud")

st.markdown("---")
st.caption("**BrazilGrid** | Dados: ONS | Formula: Curtailment = val_geracaoreferencia - val_geracao (quando cod_razaorestricao != null)")
