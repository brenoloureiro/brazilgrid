"""
BrazilGrid - Dashboard de Curtailment Unificado
v0.4 - Curtailment Conjunto + Detalhamento por Usina
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import clickhouse_connect

st.set_page_config(
    page_title="BrazilGrid - Curtailment",
    page_icon="⚡",
    layout="wide"
)


# ============================================================================
# CONEXAO CLICKHOUSE (COMPARTILHADA)
# ============================================================================

DATABASE = "brazilgrid_historico"


@st.cache_data(ttl=3600)
def get_clickhouse_config():
    """Obter config do ClickHouse (Streamlit secrets ou Prefect)"""
    try:
        return dict(st.secrets["clickhouse"])
    except Exception:
        from shared.handlers.config_secrets import get_clickhouse_config as prefect_config
        return prefect_config()


def get_client():
    """Criar novo cliente ClickHouse (sem cache para evitar concurrent queries)"""
    config = get_clickhouse_config()
    return clickhouse_connect.get_client(
        host=config["host"], port=config["port"],
        user=config["user"], password=config["password"], secure=True
    )


# ============================================================================
# MODULO 1: CURTAILMENT CONJUNTO (Eolico/Solar)
# ============================================================================

FONTES_CONJUNTO = {
    "Eolica": {"tabela": "curtailment_eolico_conjunto", "icone": "🌬️", "cor": "Reds"},
    "Solar": {"tabela": "curtailment_solar_conjunto", "icone": "☀️", "cor": "Oranges"}
}


@st.cache_data(ttl=3600)
def conjunto_load_summary_data(tabela):
    client = get_client()
    query = f"""SELECT count(), min(din_instante), max(din_instante),
        count(DISTINCT id_ons), count(DISTINCT id_estado)
        FROM {DATABASE}.{tabela}"""
    return client.query(query).result_rows[0]


@st.cache_data(ttl=3600)
def conjunto_load_estados(tabela):
    client = get_client()
    query = f"SELECT DISTINCT id_estado FROM {DATABASE}.{tabela} ORDER BY id_estado"
    return [row[0] for row in client.query(query).result_rows]


@st.cache_data(ttl=3600)
def conjunto_load_curtailment_by_state(tabela, data_inicio, data_fim):
    client = get_client()
    query = f"""SELECT id_estado,
        round(sum(val_geracaoreferencia) / 2, 2),
        round(sum(val_geracao) / 2, 2),
        round(sum(CASE WHEN cod_razaorestricao IS NOT NULL AND cod_razaorestricao != ''
            THEN greatest(val_geracaoreferencia - val_geracao, 0) ELSE 0 END) / 2, 2)
        FROM {DATABASE}.{tabela}
        WHERE din_instante >= '{data_inicio}' AND din_instante <= '{data_fim}'
        GROUP BY id_estado ORDER BY 4 DESC"""
    return pd.DataFrame(client.query(query).result_rows,
        columns=['Estado', 'Geracao Ref (MWh)', 'Geracao Real (MWh)', 'Curtailment (MWh)'])


@st.cache_data(ttl=3600)
def conjunto_load_curtailment_timeline(tabela, data_inicio, data_fim, estado=None, granularidade='auto'):
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
        FROM {DATABASE}.{tabela}
        WHERE din_instante >= '{data_inicio}' AND din_instante <= '{data_fim}' {where_estado}
        GROUP BY 1 ORDER BY 1"""
    return pd.DataFrame(client.query(query).result_rows, columns=['Periodo', 'Curtailment (MWh)']), label


@st.cache_data(ttl=3600)
def conjunto_load_curtailment_by_reason(tabela, data_inicio, data_fim):
    client = get_client()
    query = f"""SELECT cod_razaorestricao,
        round(sum(greatest(val_geracaoreferencia - val_geracao, 0)) / 2, 2)
        FROM {DATABASE}.{tabela}
        WHERE din_instante >= '{data_inicio}' AND din_instante <= '{data_fim}'
          AND cod_razaorestricao IS NOT NULL AND cod_razaorestricao != ''
        GROUP BY 1 ORDER BY 2 DESC"""
    df = pd.DataFrame(client.query(query).result_rows, columns=['Razao', 'Curtailment (MWh)'])
    razao_map = {'CNF': 'Confiabilidade', 'ENE': 'Razao Energetica', 'REL': 'Indisp. Externa', 'PAR': 'Parecer Acesso'}
    df['Razao'] = df['Razao'].map(lambda x: razao_map.get(x, x))
    return df


@st.cache_data(ttl=3600)
def conjunto_load_total_curtailment(tabela, data_inicio, data_fim):
    client = get_client()
    query = f"""SELECT
        round(sum(CASE WHEN cod_razaorestricao IS NOT NULL AND cod_razaorestricao != ''
            THEN greatest(val_geracaoreferencia - val_geracao, 0) ELSE 0 END) / 2, 2),
        round(sum(val_geracao) / 2, 2),
        round(sum(val_geracaoreferencia) / 2, 2)
        FROM {DATABASE}.{tabela}
        WHERE din_instante >= '{data_inicio}' AND din_instante <= '{data_fim}'"""
    return client.query(query).result_rows[0]


def render_conjunto_tab():
    """Renderiza aba de Curtailment Conjunto (Eolico/Solar)"""

    # Sidebar filtros
    st.sidebar.header("Filtros - Conjunto")
    fonte_selecionada = st.sidebar.radio("Fonte de Energia", list(FONTES_CONJUNTO.keys()), horizontal=True, key="fonte_conjunto")
    fonte_config = FONTES_CONJUNTO[fonte_selecionada]
    tabela, icone, cor = fonte_config["tabela"], fonte_config["icone"], fonte_config["cor"]

    try:
        summary = conjunto_load_summary_data(tabela)
        estados = conjunto_load_estados(tabela)

        periodo_preset = st.sidebar.selectbox("Periodo",
            ["Ultimos 30 dias", "Ultimos 90 dias", "Ultimo ano", "Todo historico", "Personalizado"],
            key="periodo_conjunto")

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
            data_inicio = col1.date_input("Data Inicio", datetime.now().date() - timedelta(days=30), key="di_conjunto")
            data_fim = col2.date_input("Data Fim", datetime.now().date(), key="df_conjunto")

        estado_selecionado = st.sidebar.selectbox("Estado", ["Todos"] + estados, key="estado_conjunto")
        estado_filtro = None if estado_selecionado == "Todos" else estado_selecionado
        granularidade = st.sidebar.selectbox("Granularidade", ["auto", "diario", "semanal", "mensal"], key="gran_conjunto")

        # Header
        st.header(f"{icone} Curtailment {fonte_selecionada}")
        st.markdown(f"Analise de restricoes de geracao {fonte_selecionada.lower()} no Brasil (Fonte: ONS)")

        # Metricas
        totais = conjunto_load_total_curtailment(tabela, data_inicio, data_fim)
        curtailment_total, geracao_total, referencia_total = totais[0] or 0, totais[1] or 0, totais[2] or 0
        percentual = (curtailment_total / referencia_total * 100) if referencia_total > 0 else 0

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
            df_estados = conjunto_load_curtailment_by_state(tabela, data_inicio, data_fim)
            fig = px.bar(df_estados, x='Estado', y='Curtailment (MWh)', color='Curtailment (MWh)', color_continuous_scale=cor)
            fig.update_layout(showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.subheader("Curtailment por Razao")
            df_razao = conjunto_load_curtailment_by_reason(tabela, data_inicio, data_fim)
            fig = px.pie(df_razao, values='Curtailment (MWh)', names='Razao', color_discrete_sequence=px.colors.qualitative.Set2)
            st.plotly_chart(fig, use_container_width=True)

        df_timeline, label_tempo = conjunto_load_curtailment_timeline(tabela, data_inicio, data_fim, estado_filtro, granularidade)
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


# ============================================================================
# MODULO 2: DETALHAMENTO POR USINA
# ============================================================================

TABLE_USINA = "curtailment_eolico_usina"


@st.cache_data(ttl=300)
def usina_search(texto: str):
    """Buscar usinas por nome ou CEG"""
    if len(texto) < 3:
        return []
    client = get_client()
    texto_escaped = texto.replace("'", "''")
    query = f"""
        SELECT DISTINCT nom_usina, ceg, id_estado, id_subsistema, nom_conjuntousina
        FROM {DATABASE}.{TABLE_USINA}
        WHERE nom_usina ILIKE '%{texto_escaped}%' OR ceg ILIKE '%{texto_escaped}%'
        ORDER BY nom_usina
        LIMIT 50
    """
    return client.query(query).result_rows


@st.cache_data(ttl=3600)
def usina_load_subsistemas():
    client = get_client()
    query = f"SELECT DISTINCT id_subsistema FROM {DATABASE}.{TABLE_USINA} ORDER BY id_subsistema"
    return [row[0] for row in client.query(query).result_rows]


@st.cache_data(ttl=3600)
def usina_load_estados(subsistemas: tuple):
    client = get_client()
    if subsistemas:
        subsistemas_str = ", ".join([f"'{s}'" for s in subsistemas])
        where = f"WHERE id_subsistema IN ({subsistemas_str})"
    else:
        where = ""
    query = f"SELECT DISTINCT id_estado FROM {DATABASE}.{TABLE_USINA} {where} ORDER BY id_estado"
    return [row[0] for row in client.query(query).result_rows]


@st.cache_data(ttl=3600)
def usina_load_conjuntos(subsistemas: tuple, estados: tuple):
    client = get_client()
    conditions = []
    if subsistemas:
        subsistemas_str = ", ".join([f"'{s}'" for s in subsistemas])
        conditions.append(f"id_subsistema IN ({subsistemas_str})")
    if estados:
        estados_str = ", ".join([f"'{e}'" for e in estados])
        conditions.append(f"id_estado IN ({estados_str})")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"""
        SELECT DISTINCT nom_conjuntousina
        FROM {DATABASE}.{TABLE_USINA}
        {where}
        ORDER BY nom_conjuntousina
    """
    result = [row[0] for row in client.query(query).result_rows if row[0]]
    return result if result else []


@st.cache_data(ttl=3600)
def usina_load_usinas(subsistemas: tuple, estados: tuple, conjuntos: tuple):
    client = get_client()
    conditions = []
    if subsistemas:
        subsistemas_str = ", ".join([f"'{s}'" for s in subsistemas])
        conditions.append(f"id_subsistema IN ({subsistemas_str})")
    if estados:
        estados_str = ", ".join([f"'{e}'" for e in estados])
        conditions.append(f"id_estado IN ({estados_str})")
    if conjuntos:
        conjuntos_str = ", ".join([f"'{c}'" for c in conjuntos])
        conditions.append(f"nom_conjuntousina IN ({conjuntos_str})")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"""
        SELECT DISTINCT nom_usina, ceg, id_ons
        FROM {DATABASE}.{TABLE_USINA}
        {where}
        ORDER BY nom_usina
    """
    return client.query(query).result_rows


@st.cache_data(ttl=3600)
def usina_load_info(usina_nome: str):
    client = get_client()
    usina_escaped = usina_nome.replace("'", "''")
    query = f"""
        SELECT
            nom_usina, ceg, id_ons, id_estado, id_subsistema,
            nom_modalidadeoperacao, nom_conjuntousina
        FROM {DATABASE}.{TABLE_USINA}
        WHERE nom_usina = '{usina_escaped}'
        LIMIT 1
    """
    result = client.query(query).result_rows
    if result:
        row = result[0]
        return {
            'nome': row[0], 'ceg': row[1], 'id_ons': row[2], 'estado': row[3],
            'subsistema': row[4], 'modalidade': row[5],
            'conjunto': row[6] if row[6] else 'Usina Individual'
        }
    return None


@st.cache_data(ttl=3600)
def usina_load_date_range():
    client = get_client()
    query = f"SELECT min(din_instante), max(din_instante) FROM {DATABASE}.{TABLE_USINA}"
    result = client.query(query).result_rows[0]
    return result[0], result[1]


def usina_build_where_clause(subsistemas, estados, conjuntos, usinas, data_inicio, data_fim):
    conditions = [
        f"din_instante >= '{data_inicio}'",
        f"din_instante <= '{data_fim} 23:59:59'"
    ]

    if usinas:
        usinas_escaped = [u.replace("'", "''") for u in usinas]
        usinas_str = ", ".join([f"'{u}'" for u in usinas_escaped])
        conditions.append(f"nom_usina IN ({usinas_str})")
    else:
        if subsistemas:
            subsistemas_str = ", ".join([f"'{s}'" for s in subsistemas])
            conditions.append(f"id_subsistema IN ({subsistemas_str})")
        if estados:
            estados_str = ", ".join([f"'{e}'" for e in estados])
            conditions.append(f"id_estado IN ({estados_str})")
        if conjuntos:
            conjuntos_str = ", ".join([f"'{c}'" for c in conjuntos])
            conditions.append(f"nom_conjuntousina IN ({conjuntos_str})")

    return " AND ".join(conditions)


@st.cache_data(ttl=600)
def usina_load_metrics(where_clause: str):
    client = get_client()
    query = f"""
        SELECT
            count() as total_registros,
            round(avg(val_geracaoestimada), 2) as media_estimada,
            round(avg(val_geracaoverificada), 2) as media_verificada,
            round(countIf(flg_dadoventoinvalido = 0 OR flg_dadoventoinvalido IS NULL) * 100.0 / count(), 1) as pct_vento_valido,
            round(avg(val_geracaoestimada - val_geracaoverificada), 2) as diff_media
        FROM {DATABASE}.{TABLE_USINA}
        WHERE {where_clause}
    """
    result = client.query(query).result_rows[0]
    return {
        'total_registros': result[0],
        'media_estimada': result[1] or 0,
        'media_verificada': result[2] or 0,
        'pct_vento_valido': result[3] or 0,
        'diff_media': result[4] or 0
    }


@st.cache_data(ttl=600)
def usina_load_timeseries(where_clause: str, granularidade: str, usinas: tuple):
    client = get_client()

    if granularidade == 'hora':
        time_agg = "toStartOfHour(din_instante)"
        label = "Hora"
    elif granularidade == 'dia':
        time_agg = "toDate(din_instante)"
        label = "Dia"
    else:
        time_agg = "toStartOfWeek(din_instante)"
        label = "Semana"

    if usinas and len(usinas) > 1:
        query = f"""
            SELECT
                {time_agg} as periodo,
                nom_usina,
                round(avg(val_geracaoestimada), 2) as estimada,
                round(avg(val_geracaoverificada), 2) as verificada
            FROM {DATABASE}.{TABLE_USINA}
            WHERE {where_clause}
            GROUP BY periodo, nom_usina
            ORDER BY periodo, nom_usina
        """
        df = pd.DataFrame(
            client.query(query).result_rows,
            columns=['Periodo', 'Usina', 'Estimada', 'Verificada']
        )
    else:
        query = f"""
            SELECT
                {time_agg} as periodo,
                round(avg(val_geracaoestimada), 2) as estimada,
                round(avg(val_geracaoverificada), 2) as verificada
            FROM {DATABASE}.{TABLE_USINA}
            WHERE {where_clause}
            GROUP BY periodo
            ORDER BY periodo
        """
        df = pd.DataFrame(
            client.query(query).result_rows,
            columns=['Periodo', 'Estimada', 'Verificada']
        )

    return df, label


@st.cache_data(ttl=600)
def usina_load_correlation_data(where_clause: str, limit: int = 10000):
    client = get_client()
    query = f"""
        SELECT val_geracaoestimada, val_geracaoverificada
        FROM {DATABASE}.{TABLE_USINA}
        WHERE {where_clause}
            AND val_geracaoestimada IS NOT NULL
            AND val_geracaoverificada IS NOT NULL
        LIMIT {limit}
    """
    return pd.DataFrame(
        client.query(query).result_rows,
        columns=['Estimada', 'Verificada']
    )


@st.cache_data(ttl=600)
def usina_load_wind_quality(where_clause: str):
    client = get_client()
    query = f"""
        SELECT
            toStartOfMonth(din_instante) as mes,
            countIf(flg_dadoventoinvalido = 0 OR flg_dadoventoinvalido IS NULL) as validos,
            countIf(flg_dadoventoinvalido = 1) as invalidos
        FROM {DATABASE}.{TABLE_USINA}
        WHERE {where_clause}
        GROUP BY mes
        ORDER BY mes
    """
    return pd.DataFrame(
        client.query(query).result_rows,
        columns=['Mes', 'Validos', 'Invalidos']
    )


@st.cache_data(ttl=600)
def usina_load_raw_data(where_clause: str, limit: int = 1000):
    client = get_client()
    query = f"""
        SELECT
            din_instante, nom_usina, ceg, id_estado,
            val_geracaoestimada, val_geracaoverificada,
            val_ventoverificado, flg_dadoventoinvalido
        FROM {DATABASE}.{TABLE_USINA}
        WHERE {where_clause}
        ORDER BY din_instante DESC
        LIMIT {limit}
    """
    return pd.DataFrame(
        client.query(query).result_rows,
        columns=['Data/Hora', 'Usina', 'CEG', 'Estado', 'Ger. Estimada', 'Ger. Verificada', 'Vento', 'Vento Invalido']
    )


def render_usina_tab():
    """Renderiza aba de Detalhamento por Usina"""

    try:
        min_date, max_date = usina_load_date_range()

        # Sidebar
        st.sidebar.header("Filtros - Usina")

        # Busca rapida
        st.sidebar.subheader("Busca Rapida")
        search_text = st.sidebar.text_input(
            "Buscar usina (nome ou CEG)",
            placeholder="Ex: Serra, EOL.CV, Ventos...",
            help="Digite 3+ caracteres para buscar",
            key="search_usina"
        )

        usina_from_search = None
        search_filters = None

        if len(search_text) >= 3:
            search_results = usina_search(search_text)

            if search_results:
                search_options = ["-- Selecione --"] + [
                    f"{row[0]} ({row[1]}) - {row[2]}" for row in search_results
                ]
                search_map = {
                    f"{row[0]} ({row[1]}) - {row[2]}": {
                        'nome': row[0], 'ceg': row[1], 'estado': row[2],
                        'subsistema': row[3], 'conjunto': row[4]
                    } for row in search_results
                }

                selected_search = st.sidebar.selectbox(
                    f"Resultados ({len(search_results)})",
                    options=search_options,
                    key="search_results_usina"
                )

                if selected_search != "-- Selecione --":
                    usina_from_search = search_map[selected_search]['nome']
                    search_filters = search_map[selected_search]
                    st.sidebar.success(f"{usina_from_search}")
            else:
                st.sidebar.warning("Nenhuma usina encontrada")

        st.sidebar.markdown("---")

        # Filtros em cascata
        st.sidebar.subheader("Filtros em Cascata")

        if usina_from_search:
            st.sidebar.info(f"Filtros auto-preenchidos pela busca")
            st.sidebar.text(f"Subsistema: {search_filters['subsistema']}")
            st.sidebar.text(f"Estado: {search_filters['estado']}")
            st.sidebar.text(f"Conjunto: {search_filters['conjunto'] or 'Individual'}")
            st.sidebar.text(f"Usina: {usina_from_search}")

            selected_subsistemas = []
            selected_estados = []
            selected_conjuntos = []
            selected_usinas = [usina_from_search]

            if st.sidebar.button("Limpar busca", key="limpar_usina"):
                st.rerun()
        else:
            all_subsistemas = usina_load_subsistemas()
            selected_subsistemas = st.sidebar.multiselect(
                "Subsistema", options=all_subsistemas, default=[],
                help="Filtra estados disponiveis", key="subsistema_usina"
            )

            available_estados = usina_load_estados(tuple(selected_subsistemas))
            selected_estados = st.sidebar.multiselect(
                "Estado", options=available_estados, default=[],
                help="Filtra conjuntos disponiveis", key="estado_usina"
            )

            available_conjuntos = usina_load_conjuntos(
                tuple(selected_subsistemas), tuple(selected_estados)
            )
            selected_conjuntos = st.sidebar.multiselect(
                "Conjunto", options=available_conjuntos, default=[],
                help="Filtra usinas disponiveis", key="conjunto_usina"
            )

            available_usinas = usina_load_usinas(
                tuple(selected_subsistemas), tuple(selected_estados), tuple(selected_conjuntos)
            )

            usina_options = ["Todas (agregado)"] + [f"{row[0]} ({row[1]})" for row in available_usinas]
            usina_map = {f"{row[0]} ({row[1]})": row[0] for row in available_usinas}

            selected_usina_display = st.sidebar.selectbox(
                "Usina", options=usina_options, index=0,
                help="Selecione uma usina especifica", key="usina_select"
            )

            if selected_usina_display == "Todas (agregado)":
                selected_usinas = []
            else:
                selected_usinas = [usina_map[selected_usina_display]]

            if not selected_usinas:
                compare_usinas = st.sidebar.multiselect(
                    "Comparar usinas",
                    options=[row[0] for row in available_usinas],
                    default=[], max_selections=5,
                    help="Selecione ate 5 usinas para comparar",
                    key="compare_usina"
                )
                if compare_usinas:
                    selected_usinas = compare_usinas

        st.sidebar.markdown("---")

        # Periodo
        st.sidebar.subheader("Periodo")
        periodo_preset = st.sidebar.selectbox(
            "Preset",
            ["Ultimos 30 dias", "Ultimos 90 dias", "Ultimo ano", "Todo historico", "Personalizado"],
            key="periodo_usina"
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
            data_inicio = min_date.date()
            data_fim = max_date.date()
        else:
            col1, col2 = st.sidebar.columns(2)
            data_inicio = col1.date_input("Inicio", datetime.now().date() - timedelta(days=30), key="di_usina")
            data_fim = col2.date_input("Fim", datetime.now().date(), key="df_usina")

        dias = (data_fim - data_inicio).days
        if dias <= 7:
            granularidade_default = 'hora'
        elif dias <= 90:
            granularidade_default = 'dia'
        else:
            granularidade_default = 'semana'

        granularidade = st.sidebar.selectbox(
            "Granularidade", ['hora', 'dia', 'semana'],
            index=['hora', 'dia', 'semana'].index(granularidade_default),
            key="gran_usina"
        )

        # Construir where
        where_clause = usina_build_where_clause(
            tuple(selected_subsistemas), tuple(selected_estados),
            tuple(selected_conjuntos), tuple(selected_usinas),
            data_inicio, data_fim
        )

        # Header
        st.header("Detalhamento por Usina")
        st.markdown("Analise detalhada de geracao eolica por usina individual")

        # Card usina selecionada
        if len(selected_usinas) == 1:
            usina_info = usina_load_info(selected_usinas[0])
            if usina_info:
                st.markdown("---")
                st.subheader(f"{usina_info['nome']}")

                col1, col2, col3, col4, col5, col6 = st.columns(6)
                col1.markdown(f"**CEG:** {usina_info['ceg']}")
                col2.markdown(f"**ID ONS:** {usina_info['id_ons']}")
                col3.markdown(f"**Estado:** {usina_info['estado']}")
                col4.markdown(f"**Subsistema:** {usina_info['subsistema']}")
                col5.markdown(f"**Modalidade:** {usina_info['modalidade']}")
                col6.markdown(f"**Conjunto:** {usina_info['conjunto']}")

        # Metricas
        st.markdown("---")
        metrics = usina_load_metrics(where_clause)

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Total Registros", f"{metrics['total_registros']:,}")
        col2.metric("Ger. Estimada Media", f"{metrics['media_estimada']:.1f} MW")
        col3.metric("Ger. Verificada Media", f"{metrics['media_verificada']:.1f} MW")
        col4.metric("% Vento Valido", f"{metrics['pct_vento_valido']:.1f}%")
        col5.metric(
            "Diferenca Media", f"{metrics['diff_media']:.1f} MW",
            delta=f"{metrics['diff_media']:.1f}" if metrics['diff_media'] != 0 else None,
            delta_color="inverse"
        )

        # Graficos
        st.markdown("---")

        tab1, tab2, tab3 = st.tabs(["Serie Temporal", "Correlacao", "Qualidade do Vento"])

        with tab1:
            st.subheader("Geracao Estimada vs Verificada")
            df_ts, label = usina_load_timeseries(where_clause, granularidade, tuple(selected_usinas))

            if not df_ts.empty:
                if 'Usina' in df_ts.columns:
                    fig = make_subplots(rows=1, cols=2, subplot_titles=('Estimada', 'Verificada'))
                    colors = px.colors.qualitative.Set2
                    for i, usina in enumerate(df_ts['Usina'].unique()):
                        df_usina = df_ts[df_ts['Usina'] == usina]
                        color = colors[i % len(colors)]
                        fig.add_trace(
                            go.Scatter(x=df_usina['Periodo'], y=df_usina['Estimada'],
                                       name=f"{usina}", line=dict(color=color), legendgroup=usina),
                            row=1, col=1
                        )
                        fig.add_trace(
                            go.Scatter(x=df_usina['Periodo'], y=df_usina['Verificada'],
                                       name=f"{usina}", line=dict(color=color, dash='dash'),
                                       legendgroup=usina, showlegend=False),
                            row=1, col=2
                        )
                    fig.update_layout(height=500, hovermode='x unified')
                    fig.update_xaxes(title_text=label)
                    fig.update_yaxes(title_text="MW")
                else:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=df_ts['Periodo'], y=df_ts['Estimada'],
                        name='Estimada', line=dict(color='#2E86AB', width=2)
                    ))
                    fig.add_trace(go.Scatter(
                        x=df_ts['Periodo'], y=df_ts['Verificada'],
                        name='Verificada', line=dict(color='#A23B72', width=2)
                    ))
                    fig.update_layout(xaxis_title=label, yaxis_title="MW", hovermode='x unified', height=500)

                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Nenhum dado encontrado para os filtros selecionados.")

        with tab2:
            st.subheader("Correlacao: Estimada vs Verificada")
            df_corr = usina_load_correlation_data(where_clause)

            if not df_corr.empty and len(df_corr) > 10:
                correlation = df_corr['Estimada'].corr(df_corr['Verificada'])
                r_squared = correlation ** 2

                fig = px.scatter(df_corr, x='Estimada', y='Verificada', opacity=0.5, color_discrete_sequence=['#2E86AB'])

                max_val = max(df_corr['Estimada'].max(), df_corr['Verificada'].max())
                fig.add_trace(go.Scatter(
                    x=[0, max_val], y=[0, max_val], mode='lines',
                    name='Referencia (45)', line=dict(color='red', dash='dash', width=2)
                ))

                fig.update_layout(xaxis_title="Geracao Estimada (MW)", yaxis_title="Geracao Verificada (MW)", height=500)

                col1, col2 = st.columns([3, 1])
                with col1:
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    st.markdown("### Estatisticas")
                    st.metric("R2", f"{r_squared:.3f}")
                    st.metric("Correlacao", f"{correlation:.3f}")
                    st.metric("Pontos", f"{len(df_corr):,}")

                    if r_squared > 0.9:
                        st.success("Alta correlacao")
                    elif r_squared > 0.7:
                        st.info("Correlacao moderada")
                    else:
                        st.warning("Baixa correlacao")
            else:
                st.info("Dados insuficientes para analise de correlacao.")

        with tab3:
            st.subheader("Qualidade dos Dados de Vento por Mes")
            df_wind = usina_load_wind_quality(where_clause)

            if not df_wind.empty:
                df_wind_long = df_wind.melt(
                    id_vars=['Mes'], value_vars=['Validos', 'Invalidos'],
                    var_name='Status', value_name='Quantidade'
                )

                fig = px.bar(
                    df_wind_long, x='Mes', y='Quantidade', color='Status', barmode='group',
                    color_discrete_map={'Validos': '#28a745', 'Invalidos': '#dc3545'}
                )
                fig.update_layout(xaxis_title="Mes", yaxis_title="Quantidade de Registros", height=400)

                st.plotly_chart(fig, use_container_width=True)

                total_validos = df_wind['Validos'].sum()
                total_invalidos = df_wind['Invalidos'].sum()
                pct_validos = total_validos / (total_validos + total_invalidos) * 100 if (total_validos + total_invalidos) > 0 else 0

                col1, col2, col3 = st.columns(3)
                col1.metric("Total Validos", f"{total_validos:,}")
                col2.metric("Total Invalidos", f"{total_invalidos:,}")
                col3.metric("% Validos", f"{pct_validos:.1f}%")
            else:
                st.info("Nenhum dado encontrado para os filtros selecionados.")

        # Tabela de dados
        st.markdown("---")
        with st.expander("Ver Dados Brutos", expanded=False):
            n_registros = st.slider("Numero de registros", 100, 5000, 1000, 100, key="slider_usina")
            df_raw = usina_load_raw_data(where_clause, n_registros)

            if not df_raw.empty:
                st.dataframe(df_raw, use_container_width=True, hide_index=True)

                csv = df_raw.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download CSV", data=csv,
                    file_name=f"curtailment_usina_{data_inicio}_{data_fim}.csv",
                    mime="text/csv", key="download_usina"
                )
            else:
                st.info("Nenhum dado encontrado para os filtros selecionados.")

    except Exception as e:
        st.error(f"Erro ao conectar: {e}")
        st.info("Verifique as credenciais em Settings > Secrets no Streamlit Cloud")
        st.exception(e)


# ============================================================================
# INTERFACE PRINCIPAL
# ============================================================================

st.title("BrazilGrid - Dashboard de Curtailment")

# Tabs principais
tab_conjunto, tab_usina = st.tabs(["Curtailment Conjunto", "Detalhamento Usina"])

with tab_conjunto:
    render_conjunto_tab()

with tab_usina:
    render_usina_tab()

# Footer
st.markdown("---")
st.caption(
    "**BrazilGrid** | Dados: ONS | "
    "Formula Conjunto: Curtailment = val_geracaoreferencia - val_geracao (quando cod_razaorestricao != null) | "
    "Campos Usina: val_geracaoestimada, val_geracaoverificada"
)
