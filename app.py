"""
Shopping Relacionamento - Dashboard de Relacionamento por Shopping
Dashboard leve focado em ações de relacionamento do app AJFANS.
Cada shopping tem login próprio e vê apenas seus top 150 consumidores.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import bcrypt
import openpyxl
import io
import os
import base64

# ==============================================================================
# 1. CONFIG DA PÁGINA
# ==============================================================================

st.set_page_config(
    page_title="Shopping Relacionamento",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded",
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTADOS_DIR = os.path.join(BASE_DIR, "Resultados")

# ==============================================================================
# 2. PLOTLY TEMPLATE + RENDER_CHART
# ==============================================================================

pio.templates["dashboard"] = go.layout.Template(layout=go.Layout(
    font=dict(size=12),
    margin=dict(t=40, b=30, l=40, r=20),
    legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5, font=dict(size=10)),
    xaxis=dict(tickfont=dict(size=10), title=dict(font=dict(size=11)), automargin=True),
    yaxis=dict(tickfont=dict(size=10), title=dict(font=dict(size=11)), automargin=True),
    autosize=True,
))
pio.templates.default = "plotly+dashboard"


def render_chart(fig, key=None):
    """Wrapper para st.plotly_chart com config responsivo."""
    st.plotly_chart(fig, use_container_width=True, config={
        'responsive': True, 'displayModeBar': False,
    }, key=key)


# ==============================================================================
# 3. CSS
# ==============================================================================

st.markdown("""
<style>
/* Sidebar escuro */
[data-testid="stSidebar"] {
    background-color: #1E3A5F;
}
[data-testid="stSidebar"] * {
    color: #FFFFFF !important;
}
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stMultiSelect label {
    color: #B0C4DE !important;
}
[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div,
[data-testid="stSidebar"] .stMultiSelect [data-baseweb="select"] > div {
    background-color: #16253d;
    border-color: #2C3E50;
}
[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div * {
    color: #FFFFFF !important;
}

/* Header */
.main-header {
    font-size: 2rem;
    font-weight: 700;
    color: #1E3A5F;
    margin-bottom: 0.5rem;
}
.sub-header {
    font-size: 1.1rem;
    color: #5D6D7E;
    margin-bottom: 1.5rem;
}

/* KPI cards */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
    border-radius: 10px;
    padding: 15px;
    border-left: 4px solid #3498DB;
}
[data-testid="stMetric"] label {
    color: #5D6D7E !important;
    font-size: 0.85rem !important;
}
[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #1E3A5F !important;
    font-size: 1.4rem !important;
    font-weight: 700 !important;
}

/* Botão logout */
.logout-btn button {
    background-color: #E74C3C !important;
    color: white !important;
    border: none !important;
    width: 100%;
}
.logout-btn button:hover {
    background-color: #C0392B !important;
}

/* Cards de ação */
.action-card {
    border-radius: 10px;
    padding: 20px;
    margin-bottom: 15px;
    border-left: 5px solid;
}
.action-card.alerta {
    background-color: #FDEDEC;
    border-left-color: #E74C3C;
}
.action-card.atencao {
    background-color: #FEF9E7;
    border-left-color: #F39C12;
}
.action-card.oportunidade {
    background-color: #EAFAF1;
    border-left-color: #2ECC71;
}
.action-card h4 {
    margin: 0 0 8px 0;
    color: #1E3A5F;
}
.action-card p {
    margin: 0 0 5px 0;
    color: #2C3E50;
    font-size: 0.95rem;
}
.action-card .acao {
    font-weight: 600;
    color: #1E3A5F;
    font-size: 0.9rem;
    margin-top: 8px;
}

/* Contador de clientes */
.counter {
    background: #EBF5FB;
    border-radius: 8px;
    padding: 8px 16px;
    display: inline-block;
    color: #1E3A5F;
    font-weight: 600;
    margin-bottom: 10px;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
}
.stTabs [data-baseweb="tab"] {
    background-color: #2C3E50;
    color: white;
    border-radius: 6px;
    padding: 8px 16px;
}
.stTabs [aria-selected="true"] {
    background-color: #3498DB !important;
}

/* Responsividade */
@media (max-width: 768px) {
    .main-header { font-size: 1.5rem; }
    [data-testid="stMetric"] { padding: 10px; }
    [data-testid="stMetric"] [data-testid="stMetricValue"] { font-size: 1.1rem !important; }
}
</style>
""", unsafe_allow_html=True)


# ==============================================================================
# 4. AUTENTICAÇÃO
# ==============================================================================

def converter_para_dict(obj):
    """Converte AttrDict do Streamlit Cloud para dict Python."""
    if hasattr(obj, 'to_dict'):
        return obj.to_dict()
    if isinstance(obj, dict):
        return {k: converter_para_dict(v) for k, v in obj.items()}
    return obj


def carregar_config_auth():
    """Carrega configuração de autenticação dos secrets."""
    try:
        if "credentials" not in st.secrets:
            return None
        config = converter_para_dict(dict(st.secrets["credentials"]))
        if config and "usernames" in config:
            return {"credentials": config}
    except (FileNotFoundError, KeyError):
        pass
    return None


def validar_credenciais(username, password, config):
    """Valida credenciais do usuário usando bcrypt. Retorna (sucesso, shopping_nome)."""
    if not config or "credentials" not in config:
        return False, None

    usernames = config["credentials"].get("usernames", {})
    if username not in usernames:
        return False, None

    user_data = usernames[username]
    stored_hash = user_data.get("password", "")

    try:
        if bcrypt.checkpw(password.encode(), stored_hash.encode()):
            return True, user_data.get("shopping", "")
    except Exception:
        pass

    return False, None


def verificar_autenticacao():
    """Gerencia o fluxo de autenticação. Retorna True se autenticado."""
    if st.session_state.get("authentication_status"):
        return True

    config = carregar_config_auth()

    # Dev mode: sem secrets = acesso direto ao BS para testes
    if config is None:
        st.session_state["authentication_status"] = True
        st.session_state["shopping_nome"] = "Balneário Shopping"
        st.session_state["username"] = "bs"
        return True

    # Tela de login
    st.markdown('<p class="main-header">🛍️ Shopping Relacionamento</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Acesse com suas credenciais para ver os dados do seu shopping</p>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("login_form", clear_on_submit=False):
            username_input = st.text_input("Usuário", key="login_username_input")
            password_input = st.text_input("Senha", type="password", key="login_password_input")
            submit_button = st.form_submit_button("Entrar", use_container_width=True)

        if submit_button:
            if username_input and password_input:
                sucesso, shopping_nome = validar_credenciais(
                    username_input.strip().lower(), password_input, config
                )
                if sucesso:
                    st.session_state["authentication_status"] = True
                    st.session_state["shopping_nome"] = shopping_nome
                    st.session_state["username"] = username_input.strip().lower()
                    st.rerun()
                else:
                    st.error("Usuário ou senha incorretos.")
            else:
                st.warning("Preencha usuário e senha.")

    return False


# ==============================================================================
# 5. DATA LOADING
# ==============================================================================

def optimize_dtypes(df):
    """Otimiza tipos de dados para reduzir uso de memória."""
    for col in df.select_dtypes(include=["int64"]).columns:
        if df[col].min() >= 0 and df[col].max() <= 65535:
            df[col] = df[col].astype("uint16")
        elif df[col].min() >= -32768 and df[col].max() <= 32767:
            df[col] = df[col].astype("int16")
        else:
            df[col] = df[col].astype("int32")
    for col in df.select_dtypes(include=["float64"]).columns:
        df[col] = df[col].astype("float32")
    for col in df.select_dtypes(include=["object"]).columns:
        if df[col].nunique() / len(df) < 0.5:
            df[col] = df[col].astype("category")
    return df


def _resolver_periodo_pasta(periodo):
    """Retorna o caminho da pasta para o período selecionado."""
    if periodo == "Completo":
        return os.path.join(RESULTADOS_DIR, "Completo")
    elif periodo.startswith("20") and len(periodo) == 4:
        return os.path.join(RESULTADOS_DIR, "Por_Ano", periodo)
    else:
        return os.path.join(RESULTADOS_DIR, "Por_Trimestre", periodo)


@st.cache_data(ttl=3600)
def carregar_top_consumidores(periodo, shopping_nome):
    """Carrega top_consumidores_rfv.csv filtrado pelo shopping."""
    pasta = _resolver_periodo_pasta(periodo)
    caminho = os.path.join(pasta, "top_consumidores_rfv.csv")

    if not os.path.exists(caminho):
        return pd.DataFrame()

    df = pd.read_csv(caminho, sep=";", decimal=",", encoding="utf-8-sig")
    df = df[df["Shopping"] == shopping_nome].head(150).copy()
    df = optimize_dtypes(df)
    return df


@st.cache_data(ttl=3600)
def carregar_loja_info(periodo, shopping_nome):
    """Carrega loja_info.csv filtrado pelo shopping, com fallback para Completo."""
    pasta = _resolver_periodo_pasta(periodo)

    # Tentar em RFV/ primeiro, depois na raiz da pasta
    caminhos = [
        os.path.join(pasta, "RFV", "loja_info.csv"),
        os.path.join(pasta, "loja_info.csv"),
    ]
    # Fallback para Completo/RFV
    caminhos.append(os.path.join(RESULTADOS_DIR, "Completo", "RFV", "loja_info.csv"))

    for caminho in caminhos:
        if os.path.exists(caminho):
            df = pd.read_csv(caminho, encoding="utf-8-sig")
            df = df[df["shopping"] == shopping_nome].copy()
            return df

    return pd.DataFrame()


@st.cache_data(ttl=3600)
def carregar_cliente_loja(periodo):
    """Carrega cliente_loja.csv com fallback para Completo."""
    pasta = _resolver_periodo_pasta(periodo)

    caminhos = [
        os.path.join(pasta, "RFV", "cliente_loja.csv"),
        os.path.join(pasta, "cliente_loja.csv"),
    ]
    caminhos.append(os.path.join(RESULTADOS_DIR, "Completo", "RFV", "cliente_loja.csv"))

    for caminho in caminhos:
        if os.path.exists(caminho):
            df = pd.read_csv(caminho, encoding="utf-8-sig")
            return df

    return pd.DataFrame()


def descobrir_periodos():
    """Descobre períodos disponíveis com base nas pastas existentes."""
    periodos = []

    # Completo
    if os.path.exists(os.path.join(RESULTADOS_DIR, "Completo", "top_consumidores_rfv.csv")):
        periodos.append("Completo")

    # Anos
    pasta_anos = os.path.join(RESULTADOS_DIR, "Por_Ano")
    if os.path.exists(pasta_anos):
        anos = sorted([d for d in os.listdir(pasta_anos)
                       if os.path.isdir(os.path.join(pasta_anos, d))], reverse=True)
        periodos.extend(anos)

    # Trimestres
    pasta_tri = os.path.join(RESULTADOS_DIR, "Por_Trimestre")
    if os.path.exists(pasta_tri):
        trimestres = sorted([d for d in os.listdir(pasta_tri)
                            if os.path.isdir(os.path.join(pasta_tri, d))], reverse=True)
        periodos.extend(trimestres)

    return periodos if periodos else ["Completo"]


# ==============================================================================
# 6. MAIN APP
# ==============================================================================

def main():
    # Autenticação
    if not verificar_autenticacao():
        return

    shopping_nome = st.session_state.get("shopping_nome", "")

    # ------------------------------------------------------------------
    # SIDEBAR
    # ------------------------------------------------------------------
    with st.sidebar:
        # Logo
        logo_path = os.path.join(BASE_DIR, "AJ.jpg")
        if os.path.exists(logo_path):
            with open(logo_path, "rb") as f:
                logo_b64 = base64.b64encode(f.read()).decode()
            st.markdown(
                f'<div style="text-align:center;margin-bottom:15px;">'
                f'<img src="data:image/jpeg;base64,{logo_b64}" style="max-width:180px;border-radius:8px;">'
                f'</div>',
                unsafe_allow_html=True,
            )

        st.markdown("### 🛍️ Shopping Relacionamento")
        st.markdown(f"**{shopping_nome}**")
        st.divider()

        # Seletor de período
        periodos = descobrir_periodos()
        periodo = st.selectbox("📅 Período", periodos, index=0)

        st.divider()

        # Logout
        st.markdown('<div class="logout-btn">', unsafe_allow_html=True)
        if st.button("🚪 Sair", key="logout_btn", use_container_width=True):
            st.session_state["authentication_status"] = None
            st.session_state["shopping_nome"] = None
            st.session_state["username"] = None
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # CARREGAR DADOS
    # ------------------------------------------------------------------
    df = carregar_top_consumidores(periodo, shopping_nome)

    if df.empty:
        st.warning(f"Nenhum dado encontrado para **{shopping_nome}** no período **{periodo}**.")
        return

    df_loja_info = carregar_loja_info(periodo, shopping_nome)
    df_cliente_loja = carregar_cliente_loja(periodo)

    # ------------------------------------------------------------------
    # HEADER
    # ------------------------------------------------------------------
    st.markdown(f'<p class="main-header">📊 Top 150 Consumidores — {shopping_nome}</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-header">Período: {periodo} · Dados para ações de relacionamento</p>', unsafe_allow_html=True)

    # ------------------------------------------------------------------
    # KPIs
    # ------------------------------------------------------------------
    col1, col2, col3, col4 = st.columns(4)

    total_clientes = len(df)
    valor_total = df["Valor_Total"].sum()
    pct_vip = (df["Perfil_Cliente"].eq("VIP").sum() / total_clientes * 100) if total_clientes > 0 else 0
    ticket_medio = valor_total / total_clientes if total_clientes > 0 else 0

    col1.metric("Total Clientes", f"{total_clientes}")
    col2.metric("Valor Total", f"R$ {valor_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    col3.metric("% VIP", f"{pct_vip:.1f}%")
    col4.metric("Ticket Médio", f"R$ {ticket_medio:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

    st.markdown("---")

    # ------------------------------------------------------------------
    # FILTROS
    # ------------------------------------------------------------------
    st.markdown("#### 🔍 Filtros")
    fcol1, fcol2, fcol3 = st.columns(3)

    with fcol1:
        perfis_disponiveis = sorted(df["Perfil_Cliente"].unique().tolist())
        perfil_filtro = st.multiselect("Perfil", perfis_disponiveis, key="filtro_perfil")

    with fcol2:
        segmentos_disponiveis = sorted(df["Segmento_Principal"].dropna().unique().tolist())
        segmento_filtro = st.multiselect("Segmento", segmentos_disponiveis, key="filtro_segmento")

    with fcol3:
        lojas_disponiveis = []
        if not df_loja_info.empty:
            lojas_disponiveis = sorted(df_loja_info["loja_nome"].dropna().unique().tolist())
        loja_filtro = st.multiselect("Loja", lojas_disponiveis, key="filtro_loja")

    # Aplicar filtros
    df_filtrado = df.copy()

    if perfil_filtro:
        df_filtrado = df_filtrado[df_filtrado["Perfil_Cliente"].isin(perfil_filtro)]

    if segmento_filtro:
        df_filtrado = df_filtrado[df_filtrado["Segmento_Principal"].isin(segmento_filtro)]

    if loja_filtro and not df_cliente_loja.empty:
        clientes_loja = df_cliente_loja[df_cliente_loja["loja_nome"].isin(loja_filtro)]["cliente_id"].unique()
        df_filtrado = df_filtrado[df_filtrado["Cliente_ID"].isin(clientes_loja)]

    st.markdown(
        f'<div class="counter">Exibindo {len(df_filtrado)} de {len(df)} clientes</div>',
        unsafe_allow_html=True,
    )

    # ------------------------------------------------------------------
    # TABELA DE DADOS
    # ------------------------------------------------------------------
    colunas_exibir = [
        "Ranking", "Nome", "CPF", "Email", "Celular",
        "Bairro", "Cidade", "Genero",
        "Valor_Total", "Frequencia_Compras", "Recencia_Dias",
        "Data_Primeira_Compra", "Data_Ultima_Compra",
        "Segmento_Principal", "Loja_Favorita_Shopping", "Perfil_Cliente",
    ]
    colunas_existentes = [c for c in colunas_exibir if c in df_filtrado.columns]

    st.dataframe(
        df_filtrado[colunas_existentes],
        height=500,
        hide_index=True,
        use_container_width=True,
    )

    # ------------------------------------------------------------------
    # DOWNLOADS
    # ------------------------------------------------------------------
    st.markdown("#### 📥 Exportar Dados")
    dcol1, dcol2 = st.columns(2)

    df_export = df_filtrado[colunas_existentes].copy()
    # Converter categorias de volta para string para export
    for col in df_export.select_dtypes(include=["category"]).columns:
        df_export[col] = df_export[col].astype(str)

    with dcol1:
        csv_data = df_export.to_csv(sep=";", decimal=",", index=False, encoding="utf-8-sig")
        st.download_button(
            "⬇️ Baixar CSV",
            data=csv_data,
            file_name=f"top_consumidores_{shopping_nome.replace(' ', '_')}_{periodo}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with dcol2:
        buffer = io.BytesIO()
        df_export.to_excel(buffer, index=False, engine="openpyxl")
        st.download_button(
            "⬇️ Baixar Excel",
            data=buffer.getvalue(),
            file_name=f"top_consumidores_{shopping_nome.replace(' ', '_')}_{periodo}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    st.markdown("---")

    # ------------------------------------------------------------------
    # GRÁFICOS
    # ------------------------------------------------------------------
    st.markdown("#### 📈 Análises")

    cores_perfil = {"VIP": "#FFD700", "Premium": "#C0C0C0", "Potencial": "#CD7F32", "Pontual": "#808080"}

    tab1, tab2, tab3 = st.tabs(["Por Perfil", "Por Segmento", "Por Loja"])

    with tab1:
        gcol1, gcol2 = st.columns(2)

        with gcol1:
            # Pie chart: distribuição por perfil
            perfil_counts = df_filtrado["Perfil_Cliente"].value_counts().reset_index()
            perfil_counts.columns = ["Perfil", "Clientes"]
            fig_pie = px.pie(
                perfil_counts, names="Perfil", values="Clientes",
                title="Distribuição por Perfil",
                color="Perfil",
                color_discrete_map=cores_perfil,
            )
            fig_pie.update_traces(textposition="inside", textinfo="percent+label")
            render_chart(fig_pie, key="pie_perfil")

        with gcol2:
            # Bar chart: valor médio por perfil
            perfil_valor = df_filtrado.groupby("Perfil_Cliente")["Valor_Total"].mean().reset_index()
            perfil_valor.columns = ["Perfil", "Valor Médio"]
            perfil_valor = perfil_valor.sort_values("Valor Médio", ascending=True)
            fig_bar = px.bar(
                perfil_valor, x="Valor Médio", y="Perfil",
                orientation="h",
                title="Valor Médio por Perfil (R$)",
                color="Perfil",
                color_discrete_map=cores_perfil,
            )
            fig_bar.update_traces(texttemplate="R$ %{x:,.0f}", textposition="outside")
            fig_bar.update_layout(showlegend=False)
            render_chart(fig_bar, key="bar_perfil")

    with tab2:
        # Top 10 segmentos por valor total
        seg_valor = df_filtrado.groupby("Segmento_Principal")["Valor_Total"].sum().reset_index()
        seg_valor.columns = ["Segmento", "Valor Total"]
        seg_valor = seg_valor.sort_values("Valor Total", ascending=True).tail(10)
        fig_seg = px.bar(
            seg_valor, x="Valor Total", y="Segmento",
            orientation="h",
            title="Top 10 Segmentos por Valor Total (R$)",
            color_discrete_sequence=["#3498DB"],
        )
        fig_seg.update_traces(texttemplate="R$ %{x:,.0f}", textposition="outside")
        fig_seg.update_layout(showlegend=False)
        render_chart(fig_seg, key="bar_segmento")

    with tab3:
        if not df_loja_info.empty:
            loja_top = df_loja_info.sort_values("valor", ascending=True).tail(10)
            fig_loja = px.bar(
                loja_top, x="valor", y="loja_nome",
                orientation="h",
                title="Top 10 Lojas por Valor (R$)",
                color_discrete_sequence=["#2ECC71"],
            )
            fig_loja.update_traces(texttemplate="R$ %{x:,.0f}", textposition="outside")
            fig_loja.update_layout(showlegend=False, yaxis_title="Loja")
            render_chart(fig_loja, key="bar_loja")
        else:
            st.info("Dados de loja não disponíveis para este período.")

    st.markdown("---")

    # ------------------------------------------------------------------
    # AÇÕES RECOMENDADAS
    # ------------------------------------------------------------------
    st.markdown("#### 🎯 Ações Recomendadas")
    st.markdown('<p class="sub-header">Sugestões automáticas baseadas nos dados dos seus top clientes</p>', unsafe_allow_html=True)

    # 1. VIPs em Risco (Recência > 60 dias)
    vips = df_filtrado[df_filtrado["Perfil_Cliente"] == "VIP"]
    vips_risco = vips[vips["Recencia_Dias"] > 60] if not vips.empty else pd.DataFrame()

    if not vips_risco.empty:
        nomes_vip = ", ".join(vips_risco["Nome"].head(5).tolist())
        extra = f" e mais {len(vips_risco) - 5}" if len(vips_risco) > 5 else ""
        st.markdown(f"""
        <div class="action-card alerta">
            <h4>🚨 VIPs em Risco — {len(vips_risco)} cliente(s)</h4>
            <p>Estes clientes VIP não compram há mais de 60 dias: <strong>{nomes_vip}{extra}</strong></p>
            <p class="acao">💡 Ação: contato personalizado, oferta exclusiva, convite para evento VIP</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="action-card oportunidade">
            <h4>✅ VIPs Ativos</h4>
            <p>Todos os clientes VIP estão ativos (compraram nos últimos 60 dias). Ótimo sinal!</p>
            <p class="acao">💡 Ação: manter programa de fidelidade e benefícios exclusivos</p>
        </div>
        """, unsafe_allow_html=True)

    # 2. Premium para Upgrade
    premium = df_filtrado[df_filtrado["Perfil_Cliente"] == "Premium"]
    if not premium.empty:
        top_premium = premium.nlargest(5, "Valor_Total")
        nomes_premium = ", ".join(top_premium["Nome"].tolist())
        st.markdown(f"""
        <div class="action-card atencao">
            <h4>⬆️ Premium para Upgrade — {len(top_premium)} candidato(s)</h4>
            <p>Estes clientes Premium estão próximos de se tornarem VIP: <strong>{nomes_premium}</strong></p>
            <p class="acao">💡 Ação: programa de incentivo, convites para eventos, benefícios progressivos</p>
        </div>
        """, unsafe_allow_html=True)

    # 3. Segmento Dominante
    if not df_filtrado.empty:
        seg_totais = df_filtrado.groupby("Segmento_Principal")["Valor_Total"].sum()
        seg_top = seg_totais.idxmax()
        seg_pct = (seg_totais.max() / seg_totais.sum() * 100)
        st.markdown(f"""
        <div class="action-card oportunidade">
            <h4>🏆 Segmento Dominante — {seg_top}</h4>
            <p><strong>{seg_pct:.1f}%</strong> do faturamento dos top clientes vem do segmento <strong>{seg_top}</strong>.</p>
            <p class="acao">💡 Ação: reforçar parcerias com lojas deste segmento, criar promoções temáticas</p>
        </div>
        """, unsafe_allow_html=True)

    # 4. Loja Destaque
    if "Loja_Favorita_Shopping" in df_filtrado.columns:
        loja_counts = df_filtrado["Loja_Favorita_Shopping"].value_counts()
        if not loja_counts.empty:
            loja_top_nome = loja_counts.index[0]
            loja_top_count = loja_counts.iloc[0]
            st.markdown(f"""
            <div class="action-card oportunidade">
                <h4>⭐ Loja Destaque — {loja_top_nome}</h4>
                <p>A loja <strong>{loja_top_nome}</strong> é a favorita de <strong>{loja_top_count}</strong> dos top clientes.</p>
                <p class="acao">💡 Ação: ações conjuntas, eventos exclusivos, programa de fidelidade com a loja</p>
            </div>
            """, unsafe_allow_html=True)

    # 5. Clientes Inativos (Recência > 90 dias)
    inativos = df_filtrado[df_filtrado["Recencia_Dias"] > 90]
    if not inativos.empty:
        st.markdown(f"""
        <div class="action-card alerta">
            <h4>😴 Clientes Inativos — {len(inativos)} cliente(s)</h4>
            <p><strong>{len(inativos)}</strong> clientes não compram há mais de 3 meses. Perfis: {', '.join(inativos['Perfil_Cliente'].value_counts().index.tolist())}</p>
            <p class="acao">💡 Ação: campanha de reativação com benefícios, cupons de desconto, contato direto</p>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="action-card oportunidade">
            <h4>✅ Base Ativa</h4>
            <p>Todos os top clientes compraram nos últimos 90 dias. Excelente engajamento!</p>
            <p class="acao">💡 Ação: aproveitar o momento para ampliar ticket médio e cross-sell</p>
        </div>
        """, unsafe_allow_html=True)


# ==============================================================================
# EXECUÇÃO
# ==============================================================================

if __name__ == "__main__":
    main()
