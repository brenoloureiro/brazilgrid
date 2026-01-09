"""
BrazilGrid - Dashboard de Curtailment por Usina
Analise detalhada de geracao eolica por usina individual
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
    page_title="BrazilGrid - Analise por Usina",
    page_icon="🌬️",
    layout="wide"
)

# ============================================================================
# CONFIGURACAO
# ============================================================================

DATABASE = "brazilgrid_historico"
TABLE = "curtailment_eolico_usina"


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
# FUNCOES DE DADOS - BUSCA POR NOME/CEG
# ============================================================================

@st.cache_data(ttl=300)
def search_usinas(texto: str):
    """Buscar usinas por nome ou CEG"""
    if len(texto) < 3:
        return []

    client = get_client()
    # Escapar aspas simples
    texto_escaped = texto.replace("'", "''")
    query = f"""
        SELECT DISTINCT nom_usina, ceg, id_estado, id_subsistema, nom_conjuntousina
        FROM {DATABASE}.{TABLE}
        WHERE nom_usina ILIKE '%{texto_escaped}%' OR ceg ILIKE '%{texto_escaped}%'
        ORDER BY nom_usina
        LIMIT 50
    """
    result = client.query(query).result_rows
    return result


# ============================================================================
# FUNCOES DE DADOS - FILTROS EM CASCATA
# ============================================================================

@st.cache_data(ttl=3600)
def load_subsistemas():
    """Carregar lista de subsistemas"""
    client = get_client()
    query = f"SELECT DISTINCT id_subsistema FROM {DATABASE}.{TABLE} ORDER BY id_subsistema"
    return [row[0] for row in client.query(query).result_rows]


@st.cache_data(ttl=3600)
def load_estados(subsistemas: tuple):
    """Carregar estados filtrados por subsistema"""
    client = get_client()
    if subsistemas:
        subsistemas_str = ", ".join([f"'{s}'" for s in subsistemas])
        where = f"WHERE id_subsistema IN ({subsistemas_str})"
    else:
        where = ""
    query = f"SELECT DISTINCT id_estado FROM {DATABASE}.{TABLE} {where} ORDER BY id_estado"
    return [row[0] for row in client.query(query).result_rows]


@st.cache_data(ttl=3600)
def load_conjuntos(subsistemas: tuple, estados: tuple):
    """Carregar conjuntos filtrados"""
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
        FROM {DATABASE}.{TABLE}
        {where}
        ORDER BY nom_conjuntousina
    """
    result = [row[0] for row in client.query(query).result_rows if row[0]]
    return result if result else []


@st.cache_data(ttl=3600)
def load_usinas(subsistemas: tuple, estados: tuple, conjuntos: tuple):
    """Carregar usinas filtradas com CEG para busca"""
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
        FROM {DATABASE}.{TABLE}
        {where}
        ORDER BY nom_usina
    """
    return client.query(query).result_rows


@st.cache_data(ttl=3600)
def load_usina_info(usina_nome: str):
    """Carregar informacoes detalhadas da usina"""
    client = get_client()
    # Escapar aspas simples
    usina_escaped = usina_nome.replace("'", "''")
    query = f"""
        SELECT
            nom_usina, ceg, id_ons, id_estado, id_subsistema,
            nom_modalidadeoperacao, nom_conjuntousina
        FROM {DATABASE}.{TABLE}
        WHERE nom_usina = '{usina_escaped}'
        LIMIT 1
    """
    result = client.query(query).result_rows
    if result:
        row = result[0]
        return {
            'nome': row[0],
            'ceg': row[1],
            'id_ons': row[2],
            'estado': row[3],
            'subsistema': row[4],
            'modalidade': row[5],
            'conjunto': row[6] if row[6] else 'Usina Individual'
        }
    return None


@st.cache_data(ttl=3600)
def load_date_range():
    """Carregar range de datas disponiveis"""
    client = get_client()
    query = f"SELECT min(din_instante), max(din_instante) FROM {DATABASE}.{TABLE}"
    result = client.query(query).result_rows[0]
    return result[0], result[1]


# ============================================================================
# FUNCOES DE DADOS - METRICAS E GRAFICOS
# ============================================================================

def build_where_clause(subsistemas, estados, conjuntos, usinas, data_inicio, data_fim):
    """Construir clausula WHERE para os filtros"""
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
def load_metrics(where_clause: str):
    """Carregar metricas agregadas"""
    client = get_client()
    query = f"""
        SELECT
            count() as total_registros,
            round(avg(val_geracaoestimada), 2) as media_estimada,
            round(avg(val_geracaoverificada), 2) as media_verificada,
            round(countIf(flg_dadoventoinvalido = 0 OR flg_dadoventoinvalido IS NULL) * 100.0 / count(), 1) as pct_vento_valido,
            round(avg(val_geracaoestimada - val_geracaoverificada), 2) as diff_media
        FROM {DATABASE}.{TABLE}
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
def load_timeseries(where_clause: str, granularidade: str, usinas: tuple):
    """Carregar serie temporal de geracao"""
    client = get_client()

    # Definir agregacao temporal
    if granularidade == 'hora':
        time_agg = "toStartOfHour(din_instante)"
        label = "Hora"
    elif granularidade == 'dia':
        time_agg = "toDate(din_instante)"
        label = "Dia"
    else:  # semana
        time_agg = "toStartOfWeek(din_instante)"
        label = "Semana"

    # Se multiplas usinas, agrupar por usina tambem
    if usinas and len(usinas) > 1:
        query = f"""
            SELECT
                {time_agg} as periodo,
                nom_usina,
                round(avg(val_geracaoestimada), 2) as estimada,
                round(avg(val_geracaoverificada), 2) as verificada
            FROM {DATABASE}.{TABLE}
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
            FROM {DATABASE}.{TABLE}
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
def load_correlation_data(where_clause: str, limit: int = 10000):
    """Carregar dados para grafico de correlacao"""
    client = get_client()
    query = f"""
        SELECT
            val_geracaoestimada,
            val_geracaoverificada
        FROM {DATABASE}.{TABLE}
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
def load_wind_quality(where_clause: str):
    """Carregar qualidade dos dados de vento por mes"""
    client = get_client()
    query = f"""
        SELECT
            toStartOfMonth(din_instante) as mes,
            countIf(flg_dadoventoinvalido = 0 OR flg_dadoventoinvalido IS NULL) as validos,
            countIf(flg_dadoventoinvalido = 1) as invalidos
        FROM {DATABASE}.{TABLE}
        WHERE {where_clause}
        GROUP BY mes
        ORDER BY mes
    """
    return pd.DataFrame(
        client.query(query).result_rows,
        columns=['Mes', 'Validos', 'Invalidos']
    )


@st.cache_data(ttl=600)
def load_raw_data(where_clause: str, limit: int = 1000):
    """Carregar dados brutos para tabela"""
    client = get_client()
    query = f"""
        SELECT
            din_instante,
            nom_usina,
            ceg,
            id_estado,
            val_geracaoestimada,
            val_geracaoverificada,
            val_ventoverificado,
            flg_dadoventoinvalido
        FROM {DATABASE}.{TABLE}
        WHERE {where_clause}
        ORDER BY din_instante DESC
        LIMIT {limit}
    """
    return pd.DataFrame(
        client.query(query).result_rows,
        columns=['Data/Hora', 'Usina', 'CEG', 'Estado', 'Ger. Estimada', 'Ger. Verificada', 'Vento', 'Vento Invalido']
    )


# ============================================================================
# INTERFACE
# ============================================================================

st.title("🌬️ BrazilGrid - Analise por Usina")
st.markdown("Analise detalhada de geracao eolica por usina individual")

try:
    # Carregar dados iniciais
    min_date, max_date = load_date_range()

    # ========================================================================
    # SIDEBAR - BUSCA RAPIDA E FILTROS EM CASCATA
    # ========================================================================

    st.sidebar.header("Filtros")

    # ========================================================================
    # BUSCA RAPIDA POR NOME/CEG
    # ========================================================================

    st.sidebar.subheader("🔍 Busca Rapida")
    search_text = st.sidebar.text_input(
        "Buscar usina (nome ou CEG)",
        placeholder="Ex: Serra, EOL.CV, Ventos...",
        help="Digite 3+ caracteres para buscar"
    )

    # Variaveis para usina selecionada via busca
    usina_from_search = None
    search_filters = None

    if len(search_text) >= 3:
        search_results = search_usinas(search_text)

        if search_results:
            # Criar opcoes para selectbox
            search_options = ["-- Selecione --"] + [
                f"{row[0]} ({row[1]}) - {row[2]}" for row in search_results
            ]
            search_map = {
                f"{row[0]} ({row[1]}) - {row[2]}": {
                    'nome': row[0],
                    'ceg': row[1],
                    'estado': row[2],
                    'subsistema': row[3],
                    'conjunto': row[4]
                } for row in search_results
            }

            selected_search = st.sidebar.selectbox(
                f"Resultados ({len(search_results)})",
                options=search_options,
                key="search_results"
            )

            if selected_search != "-- Selecione --":
                usina_from_search = search_map[selected_search]['nome']
                search_filters = search_map[selected_search]
                st.sidebar.success(f"✅ {usina_from_search}")
        else:
            st.sidebar.warning("Nenhuma usina encontrada")

    st.sidebar.markdown("---")

    # ========================================================================
    # FILTROS EM CASCATA (desabilitados se usina via busca)
    # ========================================================================

    st.sidebar.subheader("📊 Filtros em Cascata")

    if usina_from_search:
        st.sidebar.info(f"Filtros auto-preenchidos pela busca")
        # Mostrar filtros preenchidos (somente leitura)
        st.sidebar.text(f"Subsistema: {search_filters['subsistema']}")
        st.sidebar.text(f"Estado: {search_filters['estado']}")
        st.sidebar.text(f"Conjunto: {search_filters['conjunto'] or 'Individual'}")
        st.sidebar.text(f"Usina: {usina_from_search}")

        # Usar valores da busca
        selected_subsistemas = []
        selected_estados = []
        selected_conjuntos = []
        selected_usinas = [usina_from_search]

        # Botao para limpar busca
        if st.sidebar.button("🗑️ Limpar busca"):
            st.rerun()
    else:
        # 1. Subsistema (multiselect)
        all_subsistemas = load_subsistemas()
        selected_subsistemas = st.sidebar.multiselect(
            "Subsistema",
            options=all_subsistemas,
            default=[],
            help="Filtra estados disponiveis"
        )

        # 2. Estado (multiselect) - filtrado por subsistema
        available_estados = load_estados(tuple(selected_subsistemas))
        selected_estados = st.sidebar.multiselect(
            "Estado",
            options=available_estados,
            default=[],
            help="Filtra conjuntos disponiveis"
        )

        # 3. Conjunto (multiselect) - filtrado por subsistema e estado
        available_conjuntos = load_conjuntos(
            tuple(selected_subsistemas),
            tuple(selected_estados)
        )
        selected_conjuntos = st.sidebar.multiselect(
            "Conjunto",
            options=available_conjuntos,
            default=[],
            help="Filtra usinas disponiveis"
        )

        # 4. Usina (selectbox com busca) - filtrado pelos anteriores
        available_usinas = load_usinas(
            tuple(selected_subsistemas),
            tuple(selected_estados),
            tuple(selected_conjuntos)
        )

        # Criar opcoes com nome e CEG para busca
        usina_options = ["Todas (agregado)"] + [
            f"{row[0]} ({row[1]})" for row in available_usinas
        ]
        usina_map = {f"{row[0]} ({row[1]})": row[0] for row in available_usinas}

        selected_usina_display = st.sidebar.selectbox(
            "Usina",
            options=usina_options,
            index=0,
            help="Selecione uma usina especifica"
        )

        # Extrair nome da usina selecionada
        if selected_usina_display == "Todas (agregado)":
            selected_usinas = []
        else:
            selected_usinas = [usina_map[selected_usina_display]]

        # Opcao de comparar multiplas usinas
        if not selected_usinas:
            compare_usinas = st.sidebar.multiselect(
                "Comparar usinas",
                options=[row[0] for row in available_usinas],
                default=[],
                max_selections=5,
                help="Selecione ate 5 usinas para comparar"
            )
            if compare_usinas:
                selected_usinas = compare_usinas

    st.sidebar.markdown("---")

    # ========================================================================
    # PERIODO
    # ========================================================================

    st.sidebar.subheader("📅 Periodo")
    periodo_preset = st.sidebar.selectbox(
        "Preset",
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
        data_inicio = min_date.date()
        data_fim = max_date.date()
    else:
        col1, col2 = st.sidebar.columns(2)
        data_inicio = col1.date_input("Inicio", datetime.now().date() - timedelta(days=30))
        data_fim = col2.date_input("Fim", datetime.now().date())

    # Granularidade
    dias = (data_fim - data_inicio).days
    if dias <= 7:
        granularidade_default = 'hora'
    elif dias <= 90:
        granularidade_default = 'dia'
    else:
        granularidade_default = 'semana'

    granularidade = st.sidebar.selectbox(
        "Granularidade",
        ['hora', 'dia', 'semana'],
        index=['hora', 'dia', 'semana'].index(granularidade_default)
    )

    # ========================================================================
    # CONSTRUIR WHERE CLAUSE
    # ========================================================================

    where_clause = build_where_clause(
        tuple(selected_subsistemas),
        tuple(selected_estados),
        tuple(selected_conjuntos),
        tuple(selected_usinas),
        data_inicio,
        data_fim
    )

    # ========================================================================
    # CARD DE INFORMACOES DA USINA
    # ========================================================================

    if len(selected_usinas) == 1:
        usina_info = load_usina_info(selected_usinas[0])
        if usina_info:
            st.markdown("---")
            st.subheader(f"📍 {usina_info['nome']}")

            col1, col2, col3, col4, col5, col6 = st.columns(6)
            col1.markdown(f"**CEG:** {usina_info['ceg']}")
            col2.markdown(f"**ID ONS:** {usina_info['id_ons']}")
            col3.markdown(f"**Estado:** {usina_info['estado']}")
            col4.markdown(f"**Subsistema:** {usina_info['subsistema']}")
            col5.markdown(f"**Modalidade:** {usina_info['modalidade']}")
            col6.markdown(f"**Conjunto:** {usina_info['conjunto']}")

    # ========================================================================
    # METRICAS
    # ========================================================================

    st.markdown("---")
    metrics = load_metrics(where_clause)

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Registros", f"{metrics['total_registros']:,}")
    col2.metric("Ger. Estimada Media", f"{metrics['media_estimada']:.1f} MW")
    col3.metric("Ger. Verificada Media", f"{metrics['media_verificada']:.1f} MW")
    col4.metric("% Vento Valido", f"{metrics['pct_vento_valido']:.1f}%")
    col5.metric(
        "Diferenca Media",
        f"{metrics['diff_media']:.1f} MW",
        delta=f"{metrics['diff_media']:.1f}" if metrics['diff_media'] != 0 else None,
        delta_color="inverse"
    )

    # ========================================================================
    # GRAFICOS
    # ========================================================================

    st.markdown("---")

    # Tabs para graficos
    tab1, tab2, tab3 = st.tabs(["📈 Serie Temporal", "🔄 Correlacao", "💨 Qualidade do Vento"])

    # Tab 1: Serie Temporal
    with tab1:
        st.subheader("Geracao Estimada vs Verificada")

        df_ts, label = load_timeseries(where_clause, granularidade, tuple(selected_usinas))

        if not df_ts.empty:
            if 'Usina' in df_ts.columns:
                # Multiplas usinas - grafico com facetas
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
                # Usina unica ou agregado
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=df_ts['Periodo'], y=df_ts['Estimada'],
                    name='Estimada', line=dict(color='#2E86AB', width=2)
                ))
                fig.add_trace(go.Scatter(
                    x=df_ts['Periodo'], y=df_ts['Verificada'],
                    name='Verificada', line=dict(color='#A23B72', width=2)
                ))
                fig.update_layout(
                    xaxis_title=label,
                    yaxis_title="MW",
                    hovermode='x unified',
                    height=500
                )

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Nenhum dado encontrado para os filtros selecionados.")

    # Tab 2: Correlacao
    with tab2:
        st.subheader("Correlacao: Estimada vs Verificada")

        df_corr = load_correlation_data(where_clause)

        if not df_corr.empty and len(df_corr) > 10:
            # Calcular R²
            correlation = df_corr['Estimada'].corr(df_corr['Verificada'])
            r_squared = correlation ** 2

            fig = px.scatter(
                df_corr, x='Estimada', y='Verificada',
                opacity=0.5,
                color_discrete_sequence=['#2E86AB']
            )

            # Linha de 45°
            max_val = max(df_corr['Estimada'].max(), df_corr['Verificada'].max())
            fig.add_trace(go.Scatter(
                x=[0, max_val], y=[0, max_val],
                mode='lines',
                name='Referencia (45°)',
                line=dict(color='red', dash='dash', width=2)
            ))

            fig.update_layout(
                xaxis_title="Geracao Estimada (MW)",
                yaxis_title="Geracao Verificada (MW)",
                height=500
            )

            col1, col2 = st.columns([3, 1])
            with col1:
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                st.markdown("### Estatisticas")
                st.metric("R²", f"{r_squared:.3f}")
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

    # Tab 3: Qualidade do Vento
    with tab3:
        st.subheader("Qualidade dos Dados de Vento por Mes")

        df_wind = load_wind_quality(where_clause)

        if not df_wind.empty:
            # Converter para formato long
            df_wind_long = df_wind.melt(
                id_vars=['Mes'],
                value_vars=['Validos', 'Invalidos'],
                var_name='Status',
                value_name='Quantidade'
            )

            fig = px.bar(
                df_wind_long,
                x='Mes', y='Quantidade',
                color='Status',
                barmode='group',
                color_discrete_map={'Validos': '#28a745', 'Invalidos': '#dc3545'}
            )

            fig.update_layout(
                xaxis_title="Mes",
                yaxis_title="Quantidade de Registros",
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

            # Resumo
            total_validos = df_wind['Validos'].sum()
            total_invalidos = df_wind['Invalidos'].sum()
            pct_validos = total_validos / (total_validos + total_invalidos) * 100 if (total_validos + total_invalidos) > 0 else 0

            col1, col2, col3 = st.columns(3)
            col1.metric("Total Validos", f"{total_validos:,}")
            col2.metric("Total Invalidos", f"{total_invalidos:,}")
            col3.metric("% Validos", f"{pct_validos:.1f}%")
        else:
            st.info("Nenhum dado encontrado para os filtros selecionados.")

    # ========================================================================
    # TABELA DE DADOS
    # ========================================================================

    st.markdown("---")

    with st.expander("📋 Ver Dados Brutos", expanded=False):
        n_registros = st.slider("Numero de registros", 100, 5000, 1000, 100)

        df_raw = load_raw_data(where_clause, n_registros)

        if not df_raw.empty:
            st.dataframe(df_raw, use_container_width=True, hide_index=True)

            # Download CSV
            csv = df_raw.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"curtailment_usina_{data_inicio}_{data_fim}.csv",
                mime="text/csv"
            )
        else:
            st.info("Nenhum dado encontrado para os filtros selecionados.")

except Exception as e:
    st.error(f"Erro ao conectar: {e}")
    st.info("Verifique as credenciais em Settings > Secrets no Streamlit Cloud")
    st.exception(e)

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.caption(
    "**BrazilGrid** | Dados: ONS | "
    "Tabela: curtailment_eolico_usina | "
    "Campos: val_geracaoestimada, val_geracaoverificada, flg_dadoventoinvalido"
)
