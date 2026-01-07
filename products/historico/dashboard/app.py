"""
BrazilGrid - Dashboard de Curtailment Eolico
MVP v0.2
"""
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import clickhouse_connect

# Configuracao da pagina
st.set_page_config(
    page_title="BrazilGrid - Curtailment Eolico",
    page_icon="🌬️",
    layout="wide"
)

# Carregar variaveis de ambiente
load_dotenv('../../config/.env')

@st.cache_resource
def get_client():
    """Conexao com ClickHouse"""
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST"),
        port=int(os.getenv("CLICKHOUSE_PORT", 8443)),
        user=os.getenv("CLICKHOUSE_USER"),
        password=os.getenv("CLICKHOUSE_PASSWORD"),
        secure=True
    )

@st.cache_data(ttl=3600)
def load_summary_data():
    """Carrega resumo dos dados"""
    client = get_client()
    query = """
        SELECT 
            count() as total_registros,
            min(din_instante) as data_inicio,
            max(din_instante) as data_fim,
            count(DISTINCT id_ons) as total_usinas,
            count(DISTINCT id_estado) as total_estados
        FROM brazilgrid_historico.curtailment_eolico_conjunto
    """
    result = client.query(query)
    return result.result_rows[0]

@st.cache_data(ttl=3600)
def load_estados():
    """Lista de estados"""
    client = get_client()
    query = """
        SELECT DISTINCT id_estado 
        FROM brazilgrid_historico.curtailment_eolico_conjunto 
        ORDER BY id_estado
    """
    result = client.query(query)
    return [row[0] for row in result.result_rows]

@st.cache_data(ttl=3600)
def load_curtailment_by_state(data_inicio, data_fim):
    """Curtailment agregado por estado"""
    client = get_client()
    query = f"""
        SELECT 
            id_estado,
            round(sum(val_geracaoreferencia) / 2, 2) as geracao_referencia_mwh,
            round(sum(val_geracao) / 2, 2) as geracao_real_mwh,
            round(sum(
                CASE 
                    WHEN cod_razaorestricao IS NOT NULL AND cod_razaorestricao != ''
                    THEN greatest(val_geracaoreferencia - val_geracao, 0)
                    ELSE 0
                END
            ) / 2, 2) as curtailment_mwh
        FROM brazilgrid_historico.curtailment_eolico_conjunto
        WHERE din_instante >= '{data_inicio}'
          AND din_instante <= '{data_fim}'
        GROUP BY id_estado
        ORDER BY curtailment_mwh DESC
    """
    result = client.query(query)
    return pd.DataFrame(result.result_rows, columns=['Estado', 'Geracao Ref (MWh)', 'Geracao Real (MWh)', 'Curtailment (MWh)'])

@st.cache_data(ttl=3600)
def load_curtailment_timeline(data_inicio, data_fim, estado=None, granularidade='auto'):
    """Curtailment ao longo do tempo com granularidade dinamica"""
    client = get_client()
    where_estado = f"AND id_estado = '{estado}'" if estado else ""
    
    # Determinar granularidade automaticamente
    dias = (data_fim - data_inicio).days
    if granularidade == 'auto':
        if dias > 90:
            agg = 'toStartOfMonth'
            label = 'Mes'
        elif dias > 30:
            agg = 'toStartOfWeek'
            label = 'Semana'
        else:
            agg = 'toDate'
            label = 'Dia'
    elif granularidade == 'mensal':
        agg = 'toStartOfMonth'
        label = 'Mes'
    elif granularidade == 'semanal':
        agg = 'toStartOfWeek'
        label = 'Semana'
    else:
        agg = 'toDate'
        label = 'Dia'
    
    query = f"""
        SELECT 
            {agg}(din_instante) as periodo,
            round(sum(
                CASE 
                    WHEN cod_razaorestricao IS NOT NULL AND cod_razaorestricao != ''
                    THEN greatest(val_geracaoreferencia - val_geracao, 0)
                    ELSE 0
                END
            ) / 2, 2) as curtailment_mwh
        FROM brazilgrid_historico.curtailment_eolico_conjunto
        WHERE din_instante >= '{data_inicio}'
          AND din_instante <= '{data_fim}'
          {where_estado}
        GROUP BY periodo
        ORDER BY periodo
    """
    result = client.query(query)
    return pd.DataFrame(result.result_rows, columns=['Periodo', 'Curtailment (MWh)']), label

@st.cache_data(ttl=3600)
def load_curtailment_by_reason(data_inicio, data_fim):
    """Curtailment por razao de restricao"""
    client = get_client()
    query = f"""
        SELECT 
            cod_razaorestricao,
            round(sum(greatest(val_geracaoreferencia - val_geracao, 0)) / 2, 2) as curtailment_mwh
        FROM brazilgrid_historico.curtailment_eolico_conjunto
        WHERE din_instante >= '{data_inicio}'
          AND din_instante <= '{data_fim}'
          AND cod_razaorestricao IS NOT NULL 
          AND cod_razaorestricao != ''
        GROUP BY cod_razaorestricao
        ORDER BY curtailment_mwh DESC
    """
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=['Razao', 'Curtailment (MWh)'])
    razao_map = {
        'CNF': 'Confiabilidade',
        'ENE': 'Razao Energetica',
        'REL': 'Indisp. Externa',
        'PAR': 'Parecer Acesso'
    }
    df['Razao'] = df['Razao'].map(lambda x: razao_map.get(x, x))
    return df

@st.cache_data(ttl=3600)
def load_total_curtailment(data_inicio, data_fim):
    """Curtailment total do periodo"""
    client = get_client()
    query = f"""
        SELECT 
            round(sum(
                CASE 
                    WHEN cod_razaorestricao IS NOT NULL AND cod_razaorestricao != ''
                    THEN greatest(val_geracaoreferencia - val_geracao, 0)
                    ELSE 0
                END
            ) / 2, 2) as curtailment_mwh,
            round(sum(val_geracao) / 2, 2) as geracao_mwh,
            round(sum(val_geracaoreferencia) / 2, 2) as referencia_mwh
        FROM brazilgrid_historico.curtailment_eolico_conjunto
        WHERE din_instante >= '{data_inicio}'
          AND din_instante <= '{data_fim}'
    """
    result = client.query(query)
    return result.result_rows[0]

# ============================================================================
# INTERFACE
# ============================================================================

st.title("🌬️ BrazilGrid - Curtailment Eolico")
st.markdown("Analise de restricoes de geracao eolica no Brasil (Fonte: ONS)")

# Sidebar - Filtros
st.sidebar.header("Filtros")

try:
    summary = load_summary_data()
    estados = load_estados()
    
    # Filtro de periodo com presets
    periodo_preset = st.sidebar.selectbox(
        "Periodo",
        ["Ultimos 30 dias", "Ultimos 90 dias", "Ultimo ano", "Todo historico", "Personalizado"]
    )
    
    if periodo_preset == "Ultimos 30 dias":
        data_fim = datetime.now().date()
        data_inicio = data_fim - timedelta(days=30)
    elif periodo_preset == "Ultimos 90 dias":
        data_fim = datetime.now().date()
        data_inicio = data_fim - timedelta(days=90)
    elif periodo_preset == "Ultimo ano":
        data_fim = datetime.now().date()
        data_inicio = data_fim - timedelta(days=365)
    elif periodo_preset == "Todo historico":
        data_inicio = summary[1].date()
        data_fim = summary[2].date()
    else:
        col1, col2 = st.sidebar.columns(2)
        data_inicio = col1.date_input("Data Inicio", datetime.now().date() - timedelta(days=30))
        data_fim = col2.date_input("Data Fim", datetime.now().date())
    
    # Filtro de estado
    estado_selecionado = st.sidebar.selectbox(
        "Estado",
        ["Todos"] + estados
    )
    estado_filtro = None if estado_selecionado == "Todos" else estado_selecionado
    
    # Granularidade
    granularidade = st.sidebar.selectbox(
        "Granularidade",
        ["auto", "diario", "semanal", "mensal"],
        help="Auto ajusta baseado no periodo selecionado"
    )

    # Carregar dados do periodo
    totais = load_total_curtailment(data_inicio, data_fim)
    curtailment_total = totais[0] or 0
    geracao_total = totais[1] or 0
    referencia_total = totais[2] or 0
    percentual = (curtailment_total / referencia_total * 100) if referencia_total > 0 else 0

    # Metricas principais
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    
    col1.metric("Curtailment Total", f"{curtailment_total/1000:,.1f} GWh")
    col2.metric("Geracao Verificada", f"{geracao_total/1000:,.1f} GWh")
    col3.metric("Geracao Referencia", f"{referencia_total/1000:,.1f} GWh")
    col4.metric("% Curtailment", f"{percentual:.1f}%")

    # Graficos
    st.markdown("---")
    
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.subheader("Curtailment por Estado")
        df_estados = load_curtailment_by_state(data_inicio, data_fim)
        
        fig_estados = px.bar(
            df_estados, 
            x='Estado', 
            y='Curtailment (MWh)',
            color='Curtailment (MWh)',
            color_continuous_scale='Reds'
        )
        fig_estados.update_layout(showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig_estados, use_container_width=True)

    with col_right:
        st.subheader("Curtailment por Razao")
        df_razao = load_curtailment_by_reason(data_inicio, data_fim)
        
        fig_razao = px.pie(
            df_razao,
            values='Curtailment (MWh)',
            names='Razao',
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        st.plotly_chart(fig_razao, use_container_width=True)

    # Timeline
    df_timeline, label_tempo = load_curtailment_timeline(data_inicio, data_fim, estado_filtro, granularidade)
    
    titulo_timeline = f"Curtailment por {label_tempo}"
    if estado_selecionado != "Todos":
        titulo_timeline += f" - {estado_selecionado}"
    
    st.subheader(titulo_timeline)
    
    fig_timeline = px.bar(
        df_timeline,
        x='Periodo',
        y='Curtailment (MWh)',
        color='Curtailment (MWh)',
        color_continuous_scale='Reds'
    )
    fig_timeline.update_layout(showlegend=False, coloraxis_showscale=False)
    st.plotly_chart(fig_timeline, use_container_width=True)

    # Tabela de dados
    with st.expander("Ver dados por estado"):
        st.dataframe(df_estados, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Erro ao conectar: {e}")
    st.info("Verifique as credenciais em config/.env")

# Footer
st.markdown("---")
st.caption("**BrazilGrid** | Dados: ONS | Formula: Curtailment = val_geracaoreferencia - val_geracao (quando cod_razaorestricao != null)")
