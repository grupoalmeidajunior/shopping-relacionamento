"""
Shopping Relacionamento - Dashboard de Relacionamento por Shopping
Dashboard focado em ações de relacionamento do app AJFANS.
Cada shopping tem login próprio e vê todos os consumidores do seu shopping.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import io
import os
import base64
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

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

# Segurança
MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_DURATION_MINUTES = 15
ATTEMPT_RESET_MINUTES = 30

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
    st.plotly_chart(fig, use_container_width=True, config={
        'responsive': True, 'displayModeBar': False,
    }, key=key)


# ==============================================================================
# 3. CSS
# ==============================================================================

st.markdown("""
<style>
[data-testid="stSidebar"] { background-color: #1E3A5F; }
[data-testid="stSidebar"] * { color: #FFFFFF !important; }
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] .stMultiSelect label { color: #B0C4DE !important; }
[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div,
[data-testid="stSidebar"] .stMultiSelect [data-baseweb="select"] > div {
    background-color: #16253d; border-color: #2C3E50;
}
[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div * { color: #FFFFFF !important; }
.main-header { font-size: 2rem; font-weight: 700; color: #1E3A5F; margin-bottom: 0.5rem; }
.sub-header { font-size: 1.1rem; color: #5D6D7E; margin-bottom: 1.5rem; }
[data-testid="stMetric"] {
    background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
    border-radius: 10px; padding: 15px; border-left: 4px solid #3498DB;
}
[data-testid="stMetric"] label { color: #5D6D7E !important; font-size: 0.85rem !important; }
[data-testid="stMetric"] [data-testid="stMetricValue"] { color: #1E3A5F !important; font-size: 1.4rem !important; font-weight: 700 !important; }
[data-testid="stSidebar"] button[kind="secondary"] {
    background-color: #E74C3C !important; color: white !important;
    border: none !important; font-weight: 600 !important;
}
[data-testid="stSidebar"] button[kind="secondary"]:hover {
    background-color: #C0392B !important; color: white !important;
}
.action-card { border-radius: 10px; padding: 20px; margin-bottom: 15px; border-left: 5px solid; }
.action-card.alerta { background-color: #FDEDEC; border-left-color: #E74C3C; }
.action-card.atencao { background-color: #FEF9E7; border-left-color: #F39C12; }
.action-card.oportunidade { background-color: #EAFAF1; border-left-color: #2ECC71; }
.action-card h4 { margin: 0 0 8px 0; color: #1E3A5F; }
.action-card p { margin: 0 0 5px 0; color: #2C3E50; font-size: 0.95rem; }
.action-card .acao { font-weight: 600; color: #1E3A5F; font-size: 0.9rem; margin-top: 8px; }
.counter { background: #EBF5FB; border-radius: 8px; padding: 8px 16px; display: inline-block; color: #1E3A5F; font-weight: 600; margin-bottom: 10px; }
.stTabs [data-baseweb="tab-list"] { gap: 8px; }
.stTabs [data-baseweb="tab"] { background-color: #2C3E50; color: white; border-radius: 6px; padding: 8px 16px; }
.stTabs [aria-selected="true"] { background-color: #3498DB !important; }
@media (max-width: 768px) {
    .main-header { font-size: 1.5rem; }
    [data-testid="stMetric"] { padding: 10px; }
    [data-testid="stMetric"] [data-testid="stMetricValue"] { font-size: 1.1rem !important; }
}
</style>
""", unsafe_allow_html=True)


# ==============================================================================
# 4. TIMEZONE
# ==============================================================================

def get_timestamp_brasilia():
    try:
        return datetime.now(ZoneInfo('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return (datetime.utcnow() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')


def get_datetime_brasilia():
    try:
        return datetime.now(ZoneInfo('America/Sao_Paulo')).replace(tzinfo=None)
    except Exception:
        return datetime.utcnow() - timedelta(hours=3)


# ==============================================================================
# 5. GOOGLE SHEETS
# ==============================================================================

@st.cache_resource(ttl=300)
def get_gsheets_connection():
    try:
        if "gsheets" not in st.secrets:
            return None
        import gspread
        from google.oauth2.service_account import Credentials

        credentials_dict = {
            "type": st.secrets["gsheets"]["type"],
            "project_id": st.secrets["gsheets"]["project_id"],
            "private_key_id": st.secrets["gsheets"]["private_key_id"],
            "private_key": st.secrets["gsheets"]["private_key"],
            "client_email": st.secrets["gsheets"]["client_email"],
            "client_id": st.secrets["gsheets"]["client_id"],
            "auth_uri": st.secrets["gsheets"]["auth_uri"],
            "token_uri": st.secrets["gsheets"]["token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gsheets"]["auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gsheets"]["client_x509_cert_url"],
        }
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        credentials = Credentials.from_service_account_info(credentials_dict, scopes=scopes)
        client = gspread.authorize(credentials)
        return client.open_by_key(st.secrets["gsheets"]["spreadsheet_id"])
    except Exception as e:
        print(f"[GSHEETS] Erro conexão: {e}")
        return None


def registrar_login(usuario, nome, shopping):
    try:
        spreadsheet = get_gsheets_connection()
        if spreadsheet is None:
            return
        ws = spreadsheet.worksheet('logins')
        ip = get_client_ip()
        ws.append_row([get_timestamp_brasilia(), usuario, nome, shopping, ip])
    except Exception:
        pass


def registrar_filtro(usuario, shopping, filtro, valor):
    try:
        spreadsheet = get_gsheets_connection()
        if spreadsheet is None:
            return
        try:
            ws = spreadsheet.worksheet('filtros')
        except Exception:
            ws = spreadsheet.add_worksheet(title='filtros', rows=1000, cols=5)
            ws.append_row(['timestamp', 'usuario', 'shopping', 'filtro', 'valor'])
        ws.append_row([get_timestamp_brasilia(), usuario, shopping, filtro, str(valor)])
    except Exception:
        pass


def registrar_download(usuario, shopping, arquivo, registros):
    try:
        spreadsheet = get_gsheets_connection()
        if spreadsheet is None:
            return
        try:
            ws = spreadsheet.worksheet('downloads')
        except Exception:
            ws = spreadsheet.add_worksheet(title='downloads', rows=1000, cols=5)
            ws.append_row(['timestamp', 'usuario', 'shopping', 'arquivo', 'registros'])
        ws.append_row([get_timestamp_brasilia(), usuario, shopping, arquivo, str(registros)])
    except Exception:
        pass


def registrar_evento_seguranca(tipo_evento, username, client_id, detalhes=None):
    try:
        spreadsheet = get_gsheets_connection()
        if spreadsheet is None:
            return
        try:
            ws = spreadsheet.worksheet('seguranca')
        except Exception:
            ws = spreadsheet.add_worksheet(title='seguranca', rows=1000, cols=6)
            ws.append_row(['timestamp', 'tipo', 'username', 'client_id', 'detalhes', 'ip'])
        ip = client_id.replace('ip_', '') if client_id.startswith('ip_') else 'N/A'
        ws.append_row([get_timestamp_brasilia(), tipo_evento, username or 'N/A', client_id, str(detalhes) if detalhes else '', ip])
    except Exception:
        pass


# ==============================================================================
# 6. EMAIL ALERTS
# ==============================================================================

def enviar_email(destinatario, assunto, corpo):
    try:
        if "SMTP_EMAIL" not in st.secrets or "SMTP_PASSWORD" not in st.secrets:
            return False
        smtp_email = st.secrets["SMTP_EMAIL"]
        smtp_password = st.secrets["SMTP_PASSWORD"]
        msg = MIMEMultipart()
        msg['From'] = f"Shopping Relacionamento <{smtp_email}>"
        msg['To'] = destinatario
        msg['Subject'] = assunto
        corpo_html = f"""<html><body style="font-family:Arial,sans-serif;line-height:1.6;">
        <div style="max-width:600px;margin:0 auto;padding:20px;">
        <h2 style="color:#1E3A5F;border-bottom:2px solid #1E3A5F;padding-bottom:10px;">🛍️ Shopping Relacionamento</h2>
        <div style="background:#f8f9fa;padding:15px;border-radius:5px;margin:15px 0;">{corpo.replace(chr(10), '<br>')}</div>
        <hr style="border:none;border-top:1px solid #ddd;margin:20px 0;">
        <p style="color:#666;font-size:12px;">Alerta automático - Shopping Relacionamento</p>
        </div></body></html>"""
        msg.attach(MIMEText(corpo_html, 'html', 'utf-8'))
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(smtp_email, smtp_password)
            server.send_message(msg)
        return True
    except Exception:
        return False


def enviar_alerta_seguranca(tipo_alerta, username, client_id, detalhes=None):
    email_seguranca = "squadaj@almeidajunior.com.br"
    agora = get_timestamp_brasilia()
    if tipo_alerta == 'bloqueio_brute_force':
        assunto = f"🚨 [ALERTA] Brute Force - Shopping Relacionamento - {agora}"
        corpo = f"""🚨 ALERTA DE SEGURANÇA - SHOPPING RELACIONAMENTO
==========================================
Tipo: TENTATIVA DE BRUTE FORCE DETECTADA
Data/Hora: {agora}
Usuário alvo: {username or 'N/A'}
Identificador: {client_id}
Tentativas: {detalhes or MAX_LOGIN_ATTEMPTS}
Ação: Bloqueado por {LOCKOUT_DURATION_MINUTES} minutos"""
    elif tipo_alerta == 'multiplas_falhas':
        assunto = f"⚠️ [AVISO] Múltiplas Falhas Login - Shopping Relacionamento - {agora}"
        corpo = f"""⚠️ AVISO - SHOPPING RELACIONAMENTO
=========================================
Tipo: MÚLTIPLAS FALHAS DE LOGIN
Data/Hora: {agora}
Usuário: {username or 'N/A'}
Identificador: {client_id}
Tentativas falhas: {detalhes}"""
    else:
        return
    enviar_email(email_seguranca, assunto, corpo)


# ==============================================================================
# 7. RATE LIMITING / BRUTE FORCE
# ==============================================================================

def get_client_ip():
    try:
        from streamlit.web.server.websocket_headers import _get_websocket_headers
        headers = _get_websocket_headers()
        if headers:
            for header in ['X-Forwarded-For', 'X-Real-IP', 'CF-Connecting-IP']:
                if header in headers:
                    return headers[header].split(',')[0].strip()
    except Exception:
        pass
    return 'N/A'


def get_client_identifier():
    try:
        from streamlit.web.server.websocket_headers import _get_websocket_headers
        headers = _get_websocket_headers()
        if headers:
            for header in ['X-Forwarded-For', 'X-Real-IP', 'CF-Connecting-IP']:
                if header in headers:
                    return f"ip_{headers[header].split(',')[0].strip()}"
    except Exception:
        pass
    if 'client_id' not in st.session_state:
        import uuid
        st.session_state['client_id'] = str(uuid.uuid4())[:8]
    return f"session_{st.session_state['client_id']}"


def obter_ou_criar_aba_rate_limit(spreadsheet):
    try:
        return spreadsheet.worksheet('rate_limit')
    except Exception:
        try:
            ws = spreadsheet.add_worksheet(title='rate_limit', rows=100, cols=4)
            ws.append_row(['client_id', 'tentativas', 'ultima_tentativa', 'bloqueado_ate'])
            return ws
        except Exception:
            return None


def obter_tentativas_gsheets(client_id):
    if not client_id:
        return 0, None
    try:
        spreadsheet = get_gsheets_connection()
        if spreadsheet is None:
            return 0, None
        ws = obter_ou_criar_aba_rate_limit(spreadsheet)
        if ws is None:
            return 0, None
        agora = get_datetime_brasilia()
        registros = ws.get_all_records()
        for i, reg in enumerate(registros):
            if reg.get('client_id') == client_id:
                tentativas = int(reg.get('tentativas', 0))
                bloqueado_ate_str = reg.get('bloqueado_ate', '')
                ultima_str = reg.get('ultima_tentativa', '')
                bloqueado_ate = None
                if bloqueado_ate_str:
                    try:
                        bloqueado_ate = datetime.strptime(bloqueado_ate_str, '%Y-%m-%d %H:%M:%S')
                        if agora > bloqueado_ate:
                            ws.update_cell(i + 2, 2, '0')
                            ws.update_cell(i + 2, 4, '')
                            return 0, None
                    except Exception:
                        pass
                if ultima_str and not bloqueado_ate:
                    try:
                        ultima = datetime.strptime(ultima_str, '%Y-%m-%d %H:%M:%S')
                        if (agora - ultima).total_seconds() > ATTEMPT_RESET_MINUTES * 60:
                            ws.update_cell(i + 2, 2, '0')
                            return 0, None
                    except Exception:
                        pass
                return tentativas, bloqueado_ate
        return 0, None
    except Exception:
        return 0, None


def atualizar_tentativas_gsheets(client_id, tentativas, bloqueado_ate=None):
    if not client_id:
        return
    try:
        spreadsheet = get_gsheets_connection()
        if spreadsheet is None:
            return
        ws = obter_ou_criar_aba_rate_limit(spreadsheet)
        if ws is None:
            return
        agora = get_timestamp_brasilia()
        bloqueado_str = bloqueado_ate.strftime('%Y-%m-%d %H:%M:%S') if bloqueado_ate else ''
        registros = ws.get_all_records()
        for i, reg in enumerate(registros):
            if reg.get('client_id') == client_id:
                ws.update_cell(i + 2, 2, str(tentativas))
                ws.update_cell(i + 2, 3, agora)
                ws.update_cell(i + 2, 4, bloqueado_str)
                return
        ws.append_row([client_id, str(tentativas), agora, bloqueado_str])
    except Exception:
        pass


def registrar_tentativa_login(client_id, sucesso=False, username=None):
    agora = get_datetime_brasilia()
    tentativas, bloqueado_ate = obter_tentativas_gsheets(client_id)
    if bloqueado_ate and agora < bloqueado_ate:
        return True, round((bloqueado_ate - agora).total_seconds() / 60, 1)
    if sucesso:
        atualizar_tentativas_gsheets(client_id, 0, None)
        return False, 0
    tentativas += 1
    if tentativas == 3:
        try:
            enviar_alerta_seguranca('multiplas_falhas', username, client_id, tentativas)
        except Exception:
            pass
    if tentativas >= MAX_LOGIN_ATTEMPTS:
        tempo_bloqueio = agora + timedelta(minutes=LOCKOUT_DURATION_MINUTES)
        atualizar_tentativas_gsheets(client_id, tentativas, tempo_bloqueio)
        try:
            registrar_evento_seguranca('bloqueio_brute_force', username, client_id, tentativas)
            enviar_alerta_seguranca('bloqueio_brute_force', username, client_id, tentativas)
        except Exception:
            pass
        return True, LOCKOUT_DURATION_MINUTES
    atualizar_tentativas_gsheets(client_id, tentativas, None)
    return False, 0


def verificar_bloqueio_login(client_id):
    agora = get_datetime_brasilia()
    tentativas, bloqueado_ate = obter_tentativas_gsheets(client_id)
    if bloqueado_ate and agora < bloqueado_ate:
        return True, round((bloqueado_ate - agora).total_seconds() / 60, 1), 0
    return False, 0, MAX_LOGIN_ATTEMPTS - tentativas


# ==============================================================================
# 8. AUTENTICAÇÃO
# ==============================================================================

def converter_para_dict(obj):
    if hasattr(obj, 'to_dict'):
        return converter_para_dict(obj.to_dict())
    elif hasattr(obj, 'items'):
        return {k: converter_para_dict(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [converter_para_dict(item) for item in obj]
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes)):
        return [converter_para_dict(item) for item in obj]
    return obj


def carregar_config_auth():
    try:
        if "credentials" not in st.secrets:
            return None
        credentials = converter_para_dict(st.secrets['credentials'])
        return {'credentials': credentials}
    except (FileNotFoundError, KeyError):
        return None


def validar_credenciais(username, password, config):
    """Retorna (sucesso, nome, role, shopping)."""
    if not config or "credentials" not in config:
        return False, None, None, None
    usernames = config["credentials"].get("usernames", {})
    if username not in usernames:
        return False, None, None, None
    user_data = usernames[username]
    stored_hash = user_data.get("password", "")
    try:
        import bcrypt
        if bcrypt.checkpw(password.encode(), stored_hash.encode()):
            return (True,
                    user_data.get("name", username),
                    user_data.get("role", "viewer"),
                    user_data.get("shopping", ""))
    except Exception:
        pass
    return False, None, None, None


def verificar_autenticacao():
    """Retorna True se autenticado."""
    if st.session_state.get("authentication_status"):
        return True

    config = carregar_config_auth()

    # Dev mode
    if config is None:
        st.session_state["authentication_status"] = True
        st.session_state["shopping_nome"] = "Balneário Shopping"
        st.session_state["username"] = "bs"
        st.session_state["name"] = "Balneário Shopping"
        st.session_state["role"] = "viewer"
        return True

    client_id = get_client_identifier()

    # Verificar bloqueio
    bloqueado, tempo_restante, tentativas_restantes = verificar_bloqueio_login(client_id)
    if bloqueado:
        st.error(f'🔒 **Acesso temporariamente bloqueado**')
        st.warning(f'Muitas tentativas incorretas. Aguarde **{tempo_restante:.0f} minutos**.')
        st.info('💡 Contato: squadaj@almeidajunior.com.br')
        return False

    # Tela de login
    st.markdown('<p class="main-header">🛍️ Shopping Relacionamento</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Acesse com suas credenciais para ver os dados do seu shopping</p>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("login_form", clear_on_submit=False):
            username_input = st.text_input("Usuário", key="login_username_input")
            password_input = st.text_input("Senha", type="password", key="login_password_input")
            submit_button = st.form_submit_button("Entrar", use_container_width=True)

        if tentativas_restantes < MAX_LOGIN_ATTEMPTS:
            st.caption(f"⚠️ {tentativas_restantes} tentativa(s) restante(s)")

        if submit_button:
            if username_input and password_input:
                attempted = username_input.strip().lower()
                sucesso, nome, role, shopping = validar_credenciais(attempted, password_input, config)
                if sucesso:
                    registrar_tentativa_login(client_id, sucesso=True, username=attempted)
                    st.session_state["authentication_status"] = True
                    st.session_state["shopping_nome"] = shopping
                    st.session_state["username"] = attempted
                    st.session_state["name"] = nome
                    st.session_state["role"] = role
                    registrar_login(attempted, nome, shopping)
                    st.rerun()
                else:
                    bloqueado, tempo = registrar_tentativa_login(client_id, sucesso=False, username=attempted)
                    registrar_evento_seguranca('login_falha', attempted, client_id)
                    if bloqueado:
                        st.error(f'🔒 Bloqueado por {tempo:.0f} minutos.')
                    else:
                        st.error("Usuário ou senha incorretos.")
            else:
                st.warning("Preencha usuário e senha.")

    return False


# ==============================================================================
# 9. DATA LOADING
# ==============================================================================

def optimize_dtypes(df):
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


MESES_PT = {
    "01": "Janeiro", "02": "Fevereiro", "03": "Março", "04": "Abril",
    "05": "Maio", "06": "Junho", "07": "Julho", "08": "Agosto",
    "09": "Setembro", "10": "Outubro", "11": "Novembro", "12": "Dezembro",
}


def _resolver_periodo_pasta(periodo):
    if periodo == "Completo":
        return os.path.join(RESULTADOS_DIR, "Completo")
    elif periodo.startswith("20") and len(periodo) == 4:
        return os.path.join(RESULTADOS_DIR, "Por_Ano", periodo)
    elif "_Q" in periodo:
        return os.path.join(RESULTADOS_DIR, "Por_Trimestre", periodo)
    else:
        # Mês: formato 2025_01
        return os.path.join(RESULTADOS_DIR, "Por_Mes", periodo)


def _label_periodo(codigo):
    """Gera label legível: 2025_01 -> '2025 - Janeiro', 2025_Q1 -> '2025 - Q1'."""
    if codigo == "Completo":
        return "Completo"
    if "_Q" in codigo:
        return codigo.replace("_", " - ")
    if len(codigo) == 4:
        return codigo
    # Mês: 2025_01
    partes = codigo.split("_")
    if len(partes) == 2 and partes[1] in MESES_PT:
        return f"{partes[0]} - {MESES_PT[partes[1]]}"
    return codigo


@st.cache_data(ttl=3600, max_entries=5)
def _carregar_top_consumidores_unico(periodo, shopping_nome):
    pasta = _resolver_periodo_pasta(periodo)
    caminho = os.path.join(pasta, "top_consumidores_rfv.csv")
    if not os.path.exists(caminho):
        return pd.DataFrame()
    df = pd.read_csv(caminho, sep=";", decimal=",", encoding="utf-8-sig")
    if shopping_nome:
        df = df[df["Shopping"] == shopping_nome].copy()
    return df


def carregar_top_consumidores(periodos_selecionados, shopping_nome):
    """Carrega e agrega dados de um ou mais períodos."""
    if len(periodos_selecionados) == 1:
        df = _carregar_top_consumidores_unico(periodos_selecionados[0], shopping_nome)
        return optimize_dtypes(df) if not df.empty else df

    # Multi-período: carregar cada um e agregar
    dfs = []
    for p in periodos_selecionados:
        df_p = _carregar_top_consumidores_unico(p, shopping_nome)
        if not df_p.empty:
            dfs.append(df_p)

    if not dfs:
        return pd.DataFrame()

    df_all = pd.concat(dfs, ignore_index=True)

    # Colunas de dados pessoais (pegar first)
    cols_pessoais = ["Shopping", "Nome", "CPF", "Email", "Celular",
                     "Logradouro", "Numero", "Complemento", "Bairro", "Cidade", "Estado", "CEP",
                     "Genero"]
    cols_pessoais = [c for c in cols_pessoais if c in df_all.columns]

    # Agregar por Cliente_ID
    agg_dict = {}
    for c in cols_pessoais:
        agg_dict[c] = "first"
    if "Valor_Total" in df_all.columns:
        agg_dict["Valor_Total"] = "sum"
    if "Frequencia_Compras" in df_all.columns:
        agg_dict["Frequencia_Compras"] = "sum"
    if "Recencia_Dias" in df_all.columns:
        agg_dict["Recencia_Dias"] = "min"
    if "Data_Primeira_Compra" in df_all.columns:
        agg_dict["Data_Primeira_Compra"] = "min"
    if "Data_Ultima_Compra" in df_all.columns:
        agg_dict["Data_Ultima_Compra"] = "max"
    # Para categorias, pegar moda (valor mais frequente)
    def _moda(x):
        m = x.mode()
        return m.iloc[0] if len(m) > 0 else x.iloc[0]

    for c in ["Segmento_Principal", "Loja_Favorita_Shopping", "Loja_Favorita_Geral", "Perfil_Cliente"]:
        if c in df_all.columns:
            agg_dict[c] = _moda
    # Scores e valores de segmento: pegar do período mais recente (last)
    for c in ["Score_Recencia", "Score_Frequencia", "Score_Valor", "Score_Total_RFV",
              "Valor_Segmento_Principal", "Valor_Loja_Favorita_Geral", "Valor_Loja_Favorita_Shopping"]:
        if c in df_all.columns:
            agg_dict[c] = "last"

    df_agg = df_all.groupby("Cliente_ID", as_index=False).agg(agg_dict)

    # Re-ranquear por Valor_Total
    df_agg = df_agg.sort_values("Valor_Total", ascending=False).reset_index(drop=True)
    df_agg["Ranking"] = range(1, len(df_agg) + 1)

    return optimize_dtypes(df_agg)


@st.cache_data(ttl=3600)
def _carregar_loja_info_unico(periodo, shopping_nome):
    pasta = _resolver_periodo_pasta(periodo)
    caminhos = [
        os.path.join(pasta, "RFV", "loja_info.csv"),
        os.path.join(pasta, "loja_info.csv"),
        os.path.join(RESULTADOS_DIR, "Completo", "RFV", "loja_info.csv"),
    ]
    for caminho in caminhos:
        if os.path.exists(caminho):
            df = pd.read_csv(caminho, encoding="utf-8-sig")
            if shopping_nome:
                df = df[df["shopping"] == shopping_nome].copy()
            return df
    return pd.DataFrame()


def carregar_loja_info(periodos_selecionados, shopping_nome):
    if len(periodos_selecionados) == 1:
        return _carregar_loja_info_unico(periodos_selecionados[0], shopping_nome)
    dfs = []
    for p in periodos_selecionados:
        df_p = _carregar_loja_info_unico(p, shopping_nome)
        if not df_p.empty:
            dfs.append(df_p)
    if not dfs:
        return pd.DataFrame()
    df_all = pd.concat(dfs, ignore_index=True)
    # Agregar: mesma loja em vários períodos -> somar valor e cupons
    group_cols = [c for c in ["loja_nome", "segmento", "shopping"] if c in df_all.columns]
    if group_cols:
        agg = {}
        if "valor" in df_all.columns:
            agg["valor"] = "sum"
        if "cupons" in df_all.columns:
            agg["cupons"] = "sum"
        df_all = df_all.groupby(group_cols, as_index=False).agg(agg)
    return df_all


@st.cache_data(ttl=3600)
def _carregar_cliente_loja_unico(periodo):
    pasta = _resolver_periodo_pasta(periodo)
    caminhos = [
        os.path.join(pasta, "RFV", "cliente_loja.csv"),
        os.path.join(pasta, "cliente_loja.csv"),
        os.path.join(RESULTADOS_DIR, "Completo", "RFV", "cliente_loja.csv"),
    ]
    for caminho in caminhos:
        if os.path.exists(caminho):
            return pd.read_csv(caminho, encoding="utf-8-sig")
    return pd.DataFrame()


def carregar_cliente_loja(periodos_selecionados):
    if len(periodos_selecionados) == 1:
        return _carregar_cliente_loja_unico(periodos_selecionados[0])
    dfs = []
    for p in periodos_selecionados:
        df_p = _carregar_cliente_loja_unico(p)
        if not df_p.empty:
            dfs.append(df_p)
    if not dfs:
        return pd.DataFrame()
    df_all = pd.concat(dfs, ignore_index=True)
    group_cols = [c for c in ["cliente_id", "loja_nome"] if c in df_all.columns]
    if group_cols:
        agg = {}
        if "n" in df_all.columns:
            agg["n"] = "sum"
        if "valor" in df_all.columns:
            agg["valor"] = "sum"
        df_all = df_all.groupby(group_cols, as_index=False).agg(agg)
    return df_all


def descobrir_periodos():
    """Retorna dict {codigo: label} ordenado: Completo, anos, trimestres, meses."""
    periodos = {}

    if os.path.exists(os.path.join(RESULTADOS_DIR, "Completo", "top_consumidores_rfv.csv")):
        periodos["Completo"] = "Completo"

    # Anos (somente 2025/2026)
    pasta_anos = os.path.join(RESULTADOS_DIR, "Por_Ano")
    if os.path.exists(pasta_anos):
        for d in sorted(os.listdir(pasta_anos), reverse=True):
            if os.path.isdir(os.path.join(pasta_anos, d)) and d in ("2025", "2026"):
                periodos[d] = d

    # Trimestres (somente 2025/2026)
    pasta_tri = os.path.join(RESULTADOS_DIR, "Por_Trimestre")
    if os.path.exists(pasta_tri):
        for d in sorted(os.listdir(pasta_tri), reverse=True):
            if os.path.isdir(os.path.join(pasta_tri, d)) and d.startswith(("2025_", "2026_")):
                periodos[d] = _label_periodo(d)

    # Meses (somente 2025/2026)
    pasta_mes = os.path.join(RESULTADOS_DIR, "Por_Mes")
    if os.path.exists(pasta_mes):
        for d in sorted(os.listdir(pasta_mes), reverse=True):
            if os.path.isdir(os.path.join(pasta_mes, d)) and d.startswith(("2025_", "2026_")):
                periodos[d] = _label_periodo(d)

    return periodos if periodos else {"Completo": "Completo"}


# ==============================================================================
# 10. PAINEL ADMINISTRATIVO
# ==============================================================================

def pagina_admin():
    st.markdown('<p class="main-header">⚙️ Administração</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Logs de acesso e segurança do dashboard</p>', unsafe_allow_html=True)

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["Logs de Login", "Filtros", "Downloads", "Segurança", "Rate Limit"])

    spreadsheet = get_gsheets_connection()

    with tab1:
        if spreadsheet:
            try:
                ws = spreadsheet.worksheet('logins')
                dados = ws.get_all_records()
                if dados:
                    df_logs = pd.DataFrame(dados)
                    st.dataframe(df_logs.sort_values('timestamp', ascending=False).head(100), hide_index=True, use_container_width=True)
                    st.caption(f"Total: {len(dados)} registros")
                else:
                    st.info("Nenhum login registrado ainda.")
            except Exception as e:
                st.error(f"Erro ao carregar logs: {e}")
        else:
            st.warning("Google Sheets não configurado.")

    with tab2:
        if spreadsheet:
            try:
                ws = spreadsheet.worksheet('filtros')
                dados = ws.get_all_records()
                if dados:
                    df_filtros = pd.DataFrame(dados)
                    st.dataframe(df_filtros.sort_values('timestamp', ascending=False).head(200), hide_index=True, use_container_width=True)
                    st.caption(f"Total: {len(dados)} registros")
                else:
                    st.info("Nenhum filtro registrado ainda.")
            except Exception as e:
                st.error(f"Erro ao carregar filtros: {e}")
        else:
            st.warning("Google Sheets não configurado.")

    with tab3:
        if spreadsheet:
            try:
                ws = spreadsheet.worksheet('downloads')
                dados = ws.get_all_records()
                if dados:
                    df_dl = pd.DataFrame(dados)
                    st.dataframe(df_dl.sort_values('timestamp', ascending=False).head(200), hide_index=True, use_container_width=True)
                    st.caption(f"Total: {len(dados)} registros")
                else:
                    st.info("Nenhum download registrado ainda.")
            except Exception as e:
                st.error(f"Erro ao carregar downloads: {e}")
        else:
            st.warning("Google Sheets não configurado.")

    with tab4:
        if spreadsheet:
            try:
                ws = spreadsheet.worksheet('seguranca')
                dados = ws.get_all_records()
                if dados:
                    df_seg = pd.DataFrame(dados)
                    st.dataframe(df_seg.sort_values('timestamp', ascending=False).head(100), hide_index=True, use_container_width=True)

                    # Resumo
                    st.markdown("##### Resumo")
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Eventos", len(dados))
                    bloqueios = len([d for d in dados if d.get('tipo') == 'bloqueio_brute_force'])
                    col2.metric("Bloqueios", bloqueios)
                    falhas = len([d for d in dados if d.get('tipo') == 'login_falha'])
                    col3.metric("Falhas Login", falhas)
                else:
                    st.info("Nenhum evento de segurança registrado.")
            except Exception as e:
                st.error(f"Erro ao carregar segurança: {e}")
        else:
            st.warning("Google Sheets não configurado.")

    with tab5:
        if spreadsheet:
            try:
                ws = spreadsheet.worksheet('rate_limit')
                dados = ws.get_all_records()
                if dados:
                    df_rl = pd.DataFrame(dados)
                    st.dataframe(df_rl, hide_index=True, use_container_width=True)

                    # Botão para limpar bloqueios
                    if st.button("🔓 Limpar todos os bloqueios", type="primary"):
                        for i in range(len(dados)):
                            ws.update_cell(i + 2, 2, '0')
                            ws.update_cell(i + 2, 4, '')
                        st.success("Bloqueios limpos!")
                        st.rerun()
                else:
                    st.info("Nenhum registro de rate limit.")
            except Exception as e:
                st.error(f"Erro ao carregar rate limit: {e}")
        else:
            st.warning("Google Sheets não configurado.")


# ==============================================================================
# 11. DASHBOARD PRINCIPAL
# ==============================================================================

def pagina_dashboard():
    shopping_nome = st.session_state.get("shopping_nome", "")
    role = st.session_state.get("role", "viewer")
    username = st.session_state.get("username", "")

    # SIDEBAR
    with st.sidebar:
        logo_path = os.path.join(BASE_DIR, "AJ.jpg")
        if os.path.exists(logo_path):
            with open(logo_path, "rb") as f:
                logo_b64 = base64.b64encode(f.read()).decode()
            st.markdown(
                f'<div style="text-align:center;margin-bottom:15px;">'
                f'<img src="data:image/jpeg;base64,{logo_b64}" style="max-width:180px;border-radius:8px;">'
                f'</div>', unsafe_allow_html=True)

        st.markdown("### 🛍️ Shopping Relacionamento")
        st.markdown(f"**{st.session_state.get('name', '')}**")

        st.divider()
        if role == "admin":
            pagina_selecionada = st.radio(
                "Navegação", ["📊 Dashboard", "🎖️ AJFANS", "⚙️ Administração"],
                key="nav_admin", label_visibility="collapsed")
        else:
            pagina_selecionada = st.radio(
                "Navegação", ["📊 Dashboard", "🎖️ AJFANS"],
                key="nav_viewer", label_visibility="collapsed")

        st.divider()

        # Admin: seletor de shopping
        if role == "admin":
            shoppings_todos = [
                "Balneário Shopping", "Continente Shopping", "Garten Shopping",
                "Nações Shopping", "Neumarkt Shopping", "Norte Shopping"
            ]
            shopping_sel = st.selectbox("🏢 Shopping", shoppings_todos,
                                        index=shoppings_todos.index(shopping_nome) if shopping_nome in shoppings_todos else 0)
            st.session_state["shopping_nome"] = shopping_sel
            shopping_nome = shopping_sel

        periodos_dict = descobrir_periodos()
        codigos = list(periodos_dict.keys())
        labels = list(periodos_dict.values())
        periodos_selecionados = st.multiselect(
            "📅 Período(s)", codigos, default=[codigos[0]] if codigos else [],
            format_func=lambda x: periodos_dict.get(x, x),
            key="filtro_periodo",
            help="Selecione um ou mais períodos. Ao combinar períodos, os dados são somados (ex: 2025 + 2026 mostra o total dos dois anos)"
        )
        if not periodos_selecionados:
            periodos_selecionados = [codigos[0]] if codigos else ["Completo"]

        st.divider()
        if st.button("🚪 Sair", key="logout_btn", use_container_width=True):
            for key in ["authentication_status", "shopping_nome", "username", "name", "role"]:
                st.session_state[key] = None
            st.rerun()

    # Roteamento
    if role == "admin" and pagina_selecionada == "⚙️ Administração":
        pagina_admin()
        return
    if pagina_selecionada == "🎖️ AJFANS":
        pagina_ajfans(shopping_nome, username)
        return

    # CARREGAR DADOS
    df = carregar_top_consumidores(periodos_selecionados, shopping_nome)
    if df.empty:
        periodo_label = ", ".join([periodos_dict.get(p, p) for p in periodos_selecionados])
        st.warning(f"Nenhum dado encontrado para **{shopping_nome}** no período **{periodo_label}**.")
        return
    df_loja_info = carregar_loja_info(periodos_selecionados, shopping_nome)
    df_cliente_loja = carregar_cliente_loja(periodos_selecionados)

    # HEADER
    periodo_label = ", ".join([periodos_dict.get(p, p) for p in periodos_selecionados])
    st.markdown(f'<p class="main-header">📊 Top Consumidores — {shopping_nome}</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-header">Período: {periodo_label} · Dados para ações de relacionamento</p>', unsafe_allow_html=True)

    st.info(
        "**O que é este painel?** Aqui você encontra a lista dos seus consumidores mais relevantes, "
        "organizados por valor de compras. Use os **filtros** abaixo para encontrar grupos específicos "
        "(ex: clientes VIP do segmento Moda) e as **Ações Recomendadas** ao final da página para planejar "
        "campanhas de relacionamento."
    )

    # FILTROS
    st.markdown("#### 🔍 Filtros")
    st.caption(
        "Use os filtros para segmentar a lista de clientes. Eles funcionam em conjunto: "
        "ao selecionar um segmento, o filtro de loja mostra apenas as lojas daquele segmento."
    )
    fcol1, fcol2, fcol3 = st.columns(3)
    with fcol1:
        perfis_disponiveis = sorted(df["Perfil_Cliente"].unique().tolist())
        perfil_filtro = st.multiselect("Perfil", perfis_disponiveis, key="filtro_perfil",
                                        help="Classificação do cliente por valor e frequência de compra: VIP (maior valor), Premium, Potencial e Pontual (menor valor)")
    with fcol2:
        segmentos_disponiveis = sorted(df["Segmento_Principal"].dropna().unique().tolist())
        segmento_filtro = st.multiselect("Segmento", segmentos_disponiveis, key="filtro_segmento",
                                          help="Categoria de loja onde o cliente mais compra (ex: Moda, Alimentação, Esportes)")
    with fcol3:
        # Lojas filtradas: só do shopping e, se segmento selecionado, só dos segmentos escolhidos
        if not df_loja_info.empty:
            df_lojas_filtradas = df_loja_info.copy()
            if segmento_filtro:
                df_lojas_filtradas = df_lojas_filtradas[df_lojas_filtradas["segmento"].isin(segmento_filtro)]
            lojas_disponiveis = sorted(df_lojas_filtradas["loja_nome"].dropna().unique().tolist())
        else:
            lojas_disponiveis = []
        loja_filtro = st.multiselect("Loja", lojas_disponiveis, key="filtro_loja",
                                      help="Filtra clientes que compraram nesta(s) loja(s) específica(s)")

    df_filtrado = df.copy()
    if perfil_filtro:
        df_filtrado = df_filtrado[df_filtrado["Perfil_Cliente"].isin(perfil_filtro)]
        registrar_filtro(username, shopping_nome, "Perfil", ", ".join(perfil_filtro))
    if segmento_filtro:
        df_filtrado = df_filtrado[df_filtrado["Segmento_Principal"].isin(segmento_filtro)]
        registrar_filtro(username, shopping_nome, "Segmento", ", ".join(segmento_filtro))
    if loja_filtro and not df_cliente_loja.empty:
        clientes_loja = df_cliente_loja[df_cliente_loja["loja_nome"].isin(loja_filtro)]["cliente_id"].unique()
        df_filtrado = df_filtrado[df_filtrado["Cliente_ID"].isin(clientes_loja)]
        registrar_filtro(username, shopping_nome, "Loja", ", ".join(loja_filtro))

    st.markdown(f'<div class="counter">Exibindo {len(df_filtrado)} de {len(df)} clientes</div>', unsafe_allow_html=True)

    # KPIs (baseados nos dados filtrados)
    col1, col2, col3, col4 = st.columns(4)
    total_clientes = len(df_filtrado)
    valor_total = df_filtrado["Valor_Total"].sum() if total_clientes > 0 else 0
    pct_vip = (df_filtrado["Perfil_Cliente"].eq("VIP").sum() / total_clientes * 100) if total_clientes > 0 else 0
    ticket_medio = valor_total / total_clientes if total_clientes > 0 else 0
    col1.metric("Total Clientes", f"{total_clientes:,}".replace(",", "."),
                help="Quantidade de clientes exibidos com os filtros atuais")
    col2.metric("Valor Total", f"R$ {valor_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                help="Soma de todas as compras destes clientes no período selecionado")
    col3.metric("% VIP", f"{pct_vip:.1f}%",
                help="Percentual de clientes VIP (os que mais compram) entre os filtrados")
    col4.metric("Ticket Médio", f"R$ {ticket_medio:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                help="Valor médio gasto por cliente (Valor Total ÷ Total Clientes)")
    st.markdown("---")

    # PREPARAR COLUNAS EXTRAS
    # Primeiro Nome (title case)
    if "Nome" in df_filtrado.columns:
        df_filtrado["Primeiro_Nome"] = df_filtrado["Nome"].fillna("").str.split().str[0].str.title()
        df_filtrado = df_filtrado.rename(columns={"Nome": "Nome_Completo"})

    # Valor_Total_Filtrado (valor nas lojas/segmentos selecionados)
    if (segmento_filtro or loja_filtro) and not df_cliente_loja.empty:
        df_cl_calc = df_cliente_loja[df_cliente_loja["cliente_id"].isin(df_filtrado["Cliente_ID"])]
        if segmento_filtro and not df_loja_info.empty:
            lojas_seg = df_loja_info[df_loja_info["segmento"].isin(segmento_filtro)]["loja_nome"].unique()
            df_cl_calc = df_cl_calc[df_cl_calc["loja_nome"].isin(lojas_seg)]
        if loja_filtro:
            df_cl_calc = df_cl_calc[df_cl_calc["loja_nome"].isin(loja_filtro)]
        valor_filtrado = df_cl_calc.groupby("cliente_id")["valor"].sum().reset_index()
        valor_filtrado.columns = ["Cliente_ID", "Valor_Total_Filtrado"]
        df_filtrado = df_filtrado.merge(valor_filtrado, on="Cliente_ID", how="left")
        df_filtrado["Valor_Total_Filtrado"] = df_filtrado["Valor_Total_Filtrado"].fillna(0).round(2)
    else:
        df_filtrado["Valor_Total_Filtrado"] = df_filtrado["Valor_Total"]

    # TABELA
    st.markdown("#### 📋 Lista de Clientes")
    st.caption(
        "**Valor_Total** = total gasto pelo cliente no período (todas as lojas do shopping). "
        "**Valor_Total_Filtrado** = total gasto apenas nas lojas/segmentos selecionados nos filtros. "
        "**Perfil_Cliente** = classificação automática: **VIP** (maior valor e frequência), "
        "**Premium** (alto), **Potencial** (médio) e **Pontual** (esporádico). "
        "Clique no cabeçalho de qualquer coluna para ordenar a tabela."
    )
    colunas_exibir = [
        "Ranking", "Cliente_ID", "Primeiro_Nome", "Nome_Completo", "Email", "Celular",
        "Bairro", "Cidade", "Genero",
        "Valor_Total", "Valor_Total_Filtrado",
        "Perfil_Cliente",
    ]
    colunas_existentes = [c for c in colunas_exibir if c in df_filtrado.columns]
    st.dataframe(df_filtrado[colunas_existentes], height=500, hide_index=True, use_container_width=True)

    # DOWNLOADS
    st.markdown("#### 📥 Exportar Dados")
    st.caption("Baixe a lista de clientes filtrada para usar em campanhas, mailings ou análises externas. O CSV abre no Excel com separador `;`.")
    dcol1, dcol2 = st.columns(2)
    df_export = df_filtrado[colunas_existentes].copy()
    for col in df_export.select_dtypes(include=["category"]).columns:
        df_export[col] = df_export[col].astype(str)
    periodo_sufixo = "_".join(periodos_selecionados)
    csv_filename = f"top_consumidores_{shopping_nome.replace(' ', '_')}_{periodo_sufixo}.csv"
    xlsx_filename = f"top_consumidores_{shopping_nome.replace(' ', '_')}_{periodo_sufixo}.xlsx"
    with dcol1:
        csv_data = df_export.to_csv(sep=";", decimal=",", index=False, encoding="utf-8-sig")
        if st.download_button("⬇️ Baixar CSV", data=csv_data,
                              file_name=csv_filename, mime="text/csv", use_container_width=True):
            registrar_download(username, shopping_nome, csv_filename, len(df_export))
    with dcol2:
        buffer = io.BytesIO()
        df_export.to_excel(buffer, index=False, engine="openpyxl")
        if st.download_button("⬇️ Baixar Excel", data=buffer.getvalue(),
                              file_name=xlsx_filename,
                              mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                              use_container_width=True):
            registrar_download(username, shopping_nome, xlsx_filename, len(df_export))
    st.markdown("---")

    # GRÁFICOS
    st.markdown("#### 📈 Análises Visuais")
    st.caption("Os gráficos abaixo respondem aos filtros aplicados. Explore as abas para diferentes visões dos dados.")
    cores_perfil = {"VIP": "#FFD700", "Premium": "#C0C0C0", "Potencial": "#CD7F32", "Pontual": "#808080"}
    tab1, tab2, tab3 = st.tabs(["Por Perfil", "Por Segmento", "Por Loja"])

    with tab1:
        st.caption(
            "**Pizza (esquerda):** mostra quanto do faturamento total vem de cada perfil. "
            "Se os VIPs concentram grande parte, significa que poucos clientes geram a maior receita. "
            "**Barras (direita):** mostra o gasto médio de cada perfil — quanto maior a barra, maior o valor médio por cliente."
        )
        gcol1, gcol2 = st.columns(2)
        with gcol1:
            perfil_valor_pie = df_filtrado.groupby("Perfil_Cliente")["Valor_Total"].sum().reset_index()
            perfil_valor_pie.columns = ["Perfil", "Valor"]
            fig_pie = px.pie(perfil_valor_pie, names="Perfil", values="Valor",
                             title="Concentração de Valor por Perfil",
                             color="Perfil", color_discrete_map=cores_perfil)
            fig_pie.update_traces(textposition="inside", textinfo="percent+label")
            render_chart(fig_pie, key="pie_perfil")
        with gcol2:
            perfil_resumo = df_filtrado.groupby("Perfil_Cliente").agg(
                Clientes=("Cliente_ID", "count"),
                Valor_Medio=("Valor_Total", "mean")
            ).reset_index()
            perfil_resumo.columns = ["Perfil", "Clientes", "Valor Médio"]
            perfil_resumo = perfil_resumo.sort_values("Valor Médio", ascending=True)
            fig_bar = px.bar(perfil_resumo, x="Valor Médio", y="Perfil", orientation="h",
                             title="Valor Médio por Perfil (R$)", color="Perfil", color_discrete_map=cores_perfil,
                             custom_data=["Clientes"])
            fig_bar.update_traces(
                texttemplate="R$ %{x:,.0f} (%{customdata[0]} clientes)",
                textposition="outside"
            )
            fig_bar.update_layout(showlegend=False)
            render_chart(fig_bar, key="bar_perfil")

    with tab2:
        st.caption(
            "Mostra os 10 segmentos de loja com maior faturamento entre os clientes filtrados. "
            "Use para identificar quais tipos de loja são mais relevantes para o seu público."
        )
        seg_valor = df_filtrado.groupby("Segmento_Principal")["Valor_Total"].sum().reset_index()
        seg_valor.columns = ["Segmento", "Valor Total"]
        seg_valor = seg_valor.sort_values("Valor Total", ascending=True).tail(10)
        fig_seg = px.bar(seg_valor, x="Valor Total", y="Segmento", orientation="h",
                         title="Top 10 Segmentos por Valor Total (R$)", color_discrete_sequence=["#3498DB"])
        fig_seg.update_traces(texttemplate="R$ %{x:,.0f}", textposition="outside")
        fig_seg.update_layout(showlegend=False)
        render_chart(fig_seg, key="bar_segmento")

    with tab3:
        st.caption(
            "Mostra as 10 lojas onde os clientes filtrados mais gastaram. "
            "Se você selecionou um segmento ou loja nos filtros, o gráfico mostra apenas as lojas correspondentes."
        )
        if not df_cliente_loja.empty and not df_filtrado.empty:
            # Filtrar cliente_loja pelos clientes do df_filtrado
            clientes_filtrados_ids = df_filtrado["Cliente_ID"].unique()
            df_cl_filtrado = df_cliente_loja[df_cliente_loja["cliente_id"].isin(clientes_filtrados_ids)]
            # Se tem filtro de segmento ou loja, mostrar apenas lojas correspondentes
            if segmento_filtro and not df_loja_info.empty:
                lojas_do_segmento = df_loja_info[df_loja_info["segmento"].isin(segmento_filtro)]["loja_nome"].unique()
                df_cl_filtrado = df_cl_filtrado[df_cl_filtrado["loja_nome"].isin(lojas_do_segmento)]
            if loja_filtro:
                df_cl_filtrado = df_cl_filtrado[df_cl_filtrado["loja_nome"].isin(loja_filtro)]
            if not df_cl_filtrado.empty:
                loja_valor = df_cl_filtrado.groupby("loja_nome")["valor"].sum().reset_index()
                loja_valor = loja_valor.sort_values("valor", ascending=True).tail(10)
                fig_loja = px.bar(loja_valor, x="valor", y="loja_nome", orientation="h",
                                  title="Top 10 Lojas por Valor (R$)", color_discrete_sequence=["#2ECC71"])
                fig_loja.update_traces(texttemplate="R$ %{x:,.0f}", textposition="outside")
                fig_loja.update_layout(showlegend=False, yaxis_title="Loja")
                render_chart(fig_loja, key="bar_loja")
            else:
                st.info("Nenhum dado de loja para os clientes filtrados.")
        else:
            st.info("Dados de loja não disponíveis para este período.")

    st.markdown("---")

    # AÇÕES RECOMENDADAS
    st.markdown("#### 🎯 Ações Recomendadas")
    st.info(
        "**Como usar esta seção?** Os cards abaixo são gerados automaticamente a partir dos dados dos seus clientes. "
        "Cada card mostra uma **situação** (o que está acontecendo), **quem são os clientes** envolvidos, "
        "e uma **sugestão de ação** concreta. Cards **vermelhos** precisam de atenção urgente, "
        "**amarelos** são oportunidades de melhoria e **verdes** são pontos positivos."
    )

    st.markdown("##### 🚨 Monitoramento de VIPs")
    st.caption("Clientes VIP são os mais valiosos. Quando param de comprar (mais de 60 dias sem compra), é um sinal de alerta.")
    vips = df_filtrado[df_filtrado["Perfil_Cliente"] == "VIP"]
    vips_risco = vips[vips["Recencia_Dias"] > 60] if not vips.empty else pd.DataFrame()
    if not vips_risco.empty:
        nomes_vip = ", ".join(vips_risco["Primeiro_Nome"].head(5).tolist())
        extra = f" e mais {len(vips_risco) - 5}" if len(vips_risco) > 5 else ""
        st.markdown(f"""<div class="action-card alerta">
            <h4>🚨 VIPs em Risco — {len(vips_risco)} cliente(s)</h4>
            <p>Estes clientes VIP não compram há mais de 60 dias: <strong>{nomes_vip}{extra}</strong></p>
            <p class="acao">💡 Ação: contato personalizado, oferta exclusiva, convite para evento VIP</p>
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown("""<div class="action-card oportunidade">
            <h4>✅ VIPs Ativos</h4>
            <p>Todos os clientes VIP estão ativos (compraram nos últimos 60 dias). Ótimo sinal!</p>
            <p class="acao">💡 Ação: manter programa de fidelidade e benefícios exclusivos</p>
        </div>""", unsafe_allow_html=True)

    # PRÓXIMOS DE UPGRADE
    st.markdown("##### ⬆️ Próximos de Upgrade")
    st.caption(
        "Clientes próximos de subir de perfil (ex: de Potencial para Premium). "
        "O **Score RFV** é uma pontuação de 3 a 15 que combina Recência (quão recente foi a última compra), "
        "Frequência (quantas vezes comprou) e Valor (quanto gastou). Quanto maior o score, mais valioso o cliente. "
        "Aqui mostramos quem está a poucos pontos de alcançar o próximo nível — esses clientes merecem atenção especial!"
    )
    perfis_ordem = ["Pontual", "Potencial", "Premium", "VIP"]
    acoes_upgrade = {
        "Pontual → Potencial": "campanha de ativação, cupons de primeira compra recorrente",
        "Potencial → Premium": "programa de incentivo progressivo, benefícios por frequência",
        "Premium → VIP": "convites para eventos exclusivos, atendimento personalizado",
    }
    score_col = "Score_Total_RFV" if "Score_Total_RFV" in df_filtrado.columns else None

    # Calcular threshold mínimo de Score_Total_RFV para cada perfil
    thresholds_score = {}
    thresholds_valor = {}
    for perfil in perfis_ordem:
        perfil_df = df_filtrado[df_filtrado["Perfil_Cliente"] == perfil]
        if not perfil_df.empty:
            if score_col:
                thresholds_score[perfil] = perfil_df[score_col].min()
            thresholds_valor[perfil] = perfil_df["Valor_Total"].median()

    colunas_download = ["Ranking", "Cliente_ID", "Primeiro_Nome", "Nome_Completo",
                        "Email", "Celular", "Bairro", "Cidade",
                        "Valor_Total", "Perfil_Cliente"]
    if score_col:
        colunas_download.append(score_col)
    colunas_download = [c for c in colunas_download if c in df_filtrado.columns]

    tem_upgrade = False
    for i in range(len(perfis_ordem) - 1):
        perfil_atual = perfis_ordem[i]
        perfil_proximo = perfis_ordem[i + 1]
        label = f"{perfil_atual} → {perfil_proximo}"

        clientes_perfil = df_filtrado[df_filtrado["Perfil_Cliente"] == perfil_atual].copy()
        if clientes_perfil.empty:
            continue

        if score_col and perfil_proximo in thresholds_score:
            # Usar Score_Total_RFV (define o perfil real)
            score_threshold = thresholds_score[perfil_proximo]
            # Candidatos: faltam até 2 pontos RFV para o próximo perfil
            clientes_perfil["_gap"] = score_threshold - clientes_perfil[score_col]
            candidatos = clientes_perfil[clientes_perfil["_gap"] <= 2].copy()
            candidatos = candidatos.sort_values("_gap")

            if candidatos.empty:
                continue

            tem_upgrade = True
            gap_medio = candidatos["_gap"].mean()
            valor_medio_atual = candidatos["Valor_Total"].median()
            valor_medio_proximo = thresholds_valor.get(perfil_proximo, 0)
            valor_fmt = f"R$ {valor_medio_atual:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
            valor_prox_fmt = f"R$ {valor_medio_proximo:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")

            st.markdown(f"""<div class="action-card atencao">
                <h4>⬆️ {label} — {len(candidatos)} cliente(s)</h4>
                <p><strong>{len(candidatos)}</strong> clientes {perfil_atual} estão próximos de se tornarem <strong>{perfil_proximo}</strong>.
                Faltam em média <strong>{gap_medio:.1f} pontos RFV</strong> (score atual vs mínimo {perfil_proximo}: {score_threshold:.0f}).
                Valor mediano atual: <strong>{valor_fmt}</strong> · Mediana {perfil_proximo}: <strong>{valor_prox_fmt}</strong></p>
                <p class="acao">💡 Ação: {acoes_upgrade[label]}</p>
            </div>""", unsafe_allow_html=True)

            # Download
            df_down = candidatos[colunas_download].copy()
            df_down["Pontos_Faltantes"] = candidatos["_gap"].round(1).values
            for col in df_down.select_dtypes(include=["category"]).columns:
                df_down[col] = df_down[col].astype(str)
        else:
            # Fallback sem score: usar top 20% por valor dentro do perfil
            candidatos = clientes_perfil.nlargest(max(1, len(clientes_perfil) // 5), "Valor_Total")
            if candidatos.empty:
                continue
            tem_upgrade = True
            st.markdown(f"""<div class="action-card atencao">
                <h4>⬆️ {label} — {len(candidatos)} cliente(s)</h4>
                <p>Os <strong>{len(candidatos)}</strong> clientes {perfil_atual} com maior valor estão mais próximos de {perfil_proximo}.</p>
                <p class="acao">💡 Ação: {acoes_upgrade[label]}</p>
            </div>""", unsafe_allow_html=True)
            df_down = candidatos[colunas_download].copy()
            for col in df_down.select_dtypes(include=["category"]).columns:
                df_down[col] = df_down[col].astype(str)

        nome_arquivo = f"upgrade_{perfil_atual}_para_{perfil_proximo}_{shopping_nome.replace(' ', '_')}.csv"
        csv_upgrade = df_down.to_csv(sep=";", decimal=",", index=False, encoding="utf-8-sig")
        st.download_button(
            f"⬇️ Baixar lista {label} ({len(df_down)})",
            data=csv_upgrade, file_name=nome_arquivo, mime="text/csv",
            key=f"download_upgrade_{i}", use_container_width=True
        )

    if not tem_upgrade:
        st.markdown("""<div class="action-card oportunidade">
            <h4>✅ Perfis Estáveis</h4>
            <p>Nenhum cliente próximo de mudar de perfil no momento.</p>
        </div>""", unsafe_allow_html=True)

    # SEGMENTO DOMINANTE
    st.markdown("##### 📊 Destaques do Período")
    st.caption("Visão geral dos segmentos e lojas mais relevantes para os clientes filtrados.")
    if not df_filtrado.empty and "Segmento_Principal" in df_filtrado.columns:
        seg_totais = df_filtrado.groupby("Segmento_Principal")["Valor_Total"].sum()
        if not seg_totais.empty:
            seg_top = seg_totais.idxmax()
            seg_pct = seg_totais.max() / seg_totais.sum() * 100
            st.markdown(f"""<div class="action-card oportunidade">
                <h4>🏆 Segmento Dominante — {seg_top}</h4>
                <p><strong>{seg_pct:.1f}%</strong> do faturamento vem do segmento <strong>{seg_top}</strong>.</p>
                <p class="acao">💡 Ação: reforçar parcerias com lojas deste segmento, criar promoções temáticas</p>
            </div>""", unsafe_allow_html=True)

    # LOJA DESTAQUE
    if "Loja_Favorita_Shopping" in df_filtrado.columns:
        loja_counts = df_filtrado["Loja_Favorita_Shopping"].value_counts()
        if not loja_counts.empty:
            st.markdown(f"""<div class="action-card oportunidade">
                <h4>⭐ Loja Destaque — {loja_counts.index[0]}</h4>
                <p>A loja <strong>{loja_counts.index[0]}</strong> é a favorita de <strong>{loja_counts.iloc[0]}</strong> dos clientes filtrados.</p>
                <p class="acao">💡 Ação: ações conjuntas, eventos exclusivos, programa de fidelidade com a loja</p>
            </div>""", unsafe_allow_html=True)

    # CLIENTES INATIVOS
    st.markdown("##### 😴 Reativação")
    st.caption("Clientes que não compram há mais de 90 dias (3 meses) são considerados inativos e podem precisar de uma campanha de reativação.")
    if "Recencia_Dias" in df_filtrado.columns:
        inativos = df_filtrado[df_filtrado["Recencia_Dias"] > 90]
        if not inativos.empty:
            st.markdown(f"""<div class="action-card alerta">
                <h4>😴 Clientes Inativos — {len(inativos)} cliente(s)</h4>
                <p><strong>{len(inativos)}</strong> clientes não compram há mais de 3 meses. Perfis: {', '.join(inativos['Perfil_Cliente'].value_counts().index.tolist())}</p>
                <p class="acao">💡 Ação: campanha de reativação com benefícios, cupons de desconto, contato direto</p>
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown("""<div class="action-card oportunidade">
                <h4>✅ Base Ativa</h4>
                <p>Todos os clientes compraram nos últimos 90 dias. Excelente engajamento!</p>
                <p class="acao">💡 Ação: aproveitar o momento para ampliar ticket médio e cross-sell</p>
            </div>""", unsafe_allow_html=True)


# ==============================================================================
# 12. PÁGINA AJFANS
# ==============================================================================

MAPA_SHOPPING_ID = {
    1: "Continente Shopping", 2: "Balneário Shopping", 3: "Neumarkt Shopping",
    4: "Norte Shopping", 5: "Garten Shopping", 6: "Nações Shopping",
}
MAPA_SHOPPING_NOME_ID = {v: k for k, v in MAPA_SHOPPING_ID.items()}
CORES_CATEGORIA = {"MegaFan": "#FFD700", "SuperFan": "#C0C0C0", "NewFan": "#CD7F32"}


@st.cache_data(ttl=3600, max_entries=3)
def carregar_categorias_ajfans(shopping_nome):
    caminho = os.path.join(BASE_DIR, "cliente_categoria.csv")
    if not os.path.exists(caminho):
        return pd.DataFrame()
    df = pd.read_csv(caminho, encoding="latin-1")
    df["shopping_id"] = df["shopping_id"].fillna(0).astype(int)
    df["shopping_nome"] = df["shopping_id"].map(MAPA_SHOPPING_ID)
    if shopping_nome:
        df = df[df["shopping_nome"] == shopping_nome].copy()
    return df


@st.cache_data(ttl=3600, max_entries=3)
def carregar_ranking_ajfans(shopping_nome):
    caminho = os.path.join(BASE_DIR, "ranking_ajfans.csv")
    if not os.path.exists(caminho):
        return pd.DataFrame()
    df = pd.read_csv(caminho, encoding="utf-8-sig", sep=";", low_memory=False)
    # Excluir linhas "Geral"
    df = df[df["shopping_sigla"].notna() & (df["shopping_sigla"] != "")].copy()
    if shopping_nome:
        df = df[df["shopping_nome"] == shopping_nome].copy()
    for c in df.select_dtypes(include=["float64"]).columns:
        df[c] = df[c].astype("float32")
    return df


def pagina_ajfans(shopping_nome, username):
    st.markdown('<p class="main-header">🎖️ AJFANS — Categorias de Clientes</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-header">{shopping_nome} · Categorias de fidelidade do app AJFANS</p>', unsafe_allow_html=True)

    st.info(
        "**O que é o AJFANS?** É o programa de fidelidade do app Almeida Junior. "
        "Os clientes são classificados em 3 categorias com base no valor total de compras e quantidade de cupons registrados no app:\n\n"
        "- 🥇 **MegaFan** — Clientes mais engajados, com maior valor de compras e mais cupons registrados\n"
        "- 🥈 **SuperFan** — Clientes com bom engajamento, valor e cupons intermediários\n"
        "- 🥉 **NewFan** — Clientes com menor engajamento, geralmente recentes ou com poucas compras\n\n"
        "Os critérios de cada categoria variam por shopping."
    )

    df_cat = carregar_categorias_ajfans(shopping_nome)
    df_rank = carregar_ranking_ajfans(shopping_nome)

    if df_cat.empty:
        st.warning("Dados de categorias AJFANS não disponíveis. Verifique se o arquivo `cliente_categoria.csv` existe.")
        return

    # KPIs
    total = len(df_cat)
    mega = (df_cat["categoria"] == "MegaFan").sum()
    super_ = (df_cat["categoria"] == "SuperFan").sum()
    new = (df_cat["categoria"] == "NewFan").sum()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Cadastros", f"{total:,}".replace(",", "."),
                help="Total de clientes cadastrados no AJFANS neste shopping")
    col2.metric("🥇 MegaFan", f"{mega:,}".replace(",", "."),
                help="Clientes mais engajados — maior valor de compras e mais cupons registrados")
    col3.metric("🥈 SuperFan", f"{super_:,}".replace(",", "."),
                help="Clientes com bom engajamento — valor e cupons intermediários")
    col4.metric("🥉 NewFan", f"{new:,}".replace(",", "."),
                help="Clientes com menor engajamento — geralmente mais recentes ou com poucas compras")
    st.markdown("---")

    # TABS
    tab1, tab2, tab3 = st.tabs(["Distribuição", "Ranking de Consumo", "Lista de Clientes"])

    with tab1:
        st.caption(
            "**Pizza (esquerda):** proporção de clientes em cada categoria. "
            "**Barras (direita):** quanto cada categoria gera em valor de compras. "
            "Compare: os MegaFans podem ser poucos, mas geram a maior receita."
        )
        gcol1, gcol2 = st.columns(2)
        with gcol1:
            cat_counts = df_cat["categoria"].value_counts().reset_index()
            cat_counts.columns = ["Categoria", "Clientes"]
            fig_pie = px.pie(cat_counts, names="Categoria", values="Clientes",
                             title="Distribuição por Categoria",
                             color="Categoria", color_discrete_map=CORES_CATEGORIA)
            fig_pie.update_traces(textposition="inside", textinfo="percent+label")
            render_chart(fig_pie, key="ajfans_pie")
        with gcol2:
            if not df_rank.empty:
                cat_valor = df_rank.groupby("categoria")["valor_total"].sum().reset_index()
                cat_valor.columns = ["Categoria", "Valor"]
                cat_valor = cat_valor.sort_values("Valor", ascending=True)
                fig_bar = px.bar(cat_valor, x="Valor", y="Categoria", orientation="h",
                                 title="Valor Total por Categoria (R$)",
                                 color="Categoria", color_discrete_map=CORES_CATEGORIA)
                fig_bar.update_traces(texttemplate="R$ %{x:,.0f}", textposition="outside")
                fig_bar.update_layout(showlegend=False)
                render_chart(fig_bar, key="ajfans_bar_valor")
            else:
                st.info("Dados de ranking não disponíveis.")

        # Engajamento: com cupons vs sem cupons
        if not df_rank.empty:
            st.markdown("##### 📊 Engajamento por Categoria")
            st.caption(
                "Mostra quantos clientes de cada categoria registraram pelo menos um cupom no app. "
                "Um percentual alto de engajamento significa que os clientes estão usando o app ativamente."
            )
            clientes_com_cupons = set(df_rank["cliente_id"].unique())
            df_cat_eng = df_cat.copy()
            df_cat_eng["tem_cupons"] = df_cat_eng["cliente_id"].isin(clientes_com_cupons)

            eng_resumo = df_cat_eng.groupby("categoria").agg(
                total=("cliente_id", "count"),
                com_cupons=("tem_cupons", "sum")
            ).reset_index()
            eng_resumo["pct_engajado"] = (eng_resumo["com_cupons"] / eng_resumo["total"] * 100).round(1)
            eng_resumo["sem_cupons"] = eng_resumo["total"] - eng_resumo["com_cupons"]

            ecol1, ecol2, ecol3 = st.columns(3)
            for col_st, (_, row) in zip([ecol1, ecol2, ecol3], eng_resumo.iterrows()):
                col_st.metric(
                    f"{row['categoria']}",
                    f"{row['pct_engajado']:.1f}% engajados",
                    f"{int(row['com_cupons']):,} de {int(row['total']):,}".replace(",", ".")
                )

    with tab2:
        if df_rank.empty:
            st.warning("Dados de ranking não disponíveis.")
        else:
            st.markdown("##### 🏆 Top Consumidores por Categoria")
            st.caption(
                "Ranking dos clientes AJFANS com maior valor de compras. "
                "Selecione a categoria e a quantidade de clientes para visualizar. "
                "Ideal para identificar os melhores clientes para ações de relacionamento personalizado."
            )
            rcol1, rcol2 = st.columns(2)
            with rcol1:
                cat_filtro = st.multiselect("Categoria", ["MegaFan", "SuperFan", "NewFan"],
                                            default=["MegaFan"], key="ajfans_cat_filtro")
            with rcol2:
                top_n = st.slider("Quantidade", 5, 50, 20, key="ajfans_top_n")

            df_rank_filtrado = df_rank.copy()
            if cat_filtro:
                df_rank_filtrado = df_rank_filtrado[df_rank_filtrado["categoria"].isin(cat_filtro)]

            df_top = df_rank_filtrado.nlargest(top_n, "valor_total")

            if not df_top.empty:
                df_top["primeiro_nome"] = df_top["nome"].fillna("").str.split().str[0].str.title()

                # Gráfico
                fig_rank = px.bar(
                    df_top.sort_values("valor_total", ascending=True).tail(20),
                    x="valor_total", y="primeiro_nome", orientation="h",
                    title=f"Top {min(top_n, len(df_top))} — Valor Total (R$)",
                    color="categoria", color_discrete_map=CORES_CATEGORIA,
                )
                fig_rank.update_traces(texttemplate="R$ %{x:,.0f}", textposition="outside")
                fig_rank.update_layout(showlegend=True, yaxis_title="")
                render_chart(fig_rank, key="ajfans_rank_bar")

                # Tabela
                colunas_rank = ["cliente_id", "primeiro_nome", "nome", "categoria",
                                "valor_total", "qtd_cupons", "ticket_medio",
                                "email", "celular", "cidade", "estado"]
                colunas_rank = [c for c in colunas_rank if c in df_top.columns]
                st.dataframe(df_top[colunas_rank], height=400, hide_index=True, use_container_width=True)

                # Download
                df_rank_export = df_top[colunas_rank].copy()
                for c in df_rank_export.select_dtypes(include=["category"]).columns:
                    df_rank_export[c] = df_rank_export[c].astype(str)
                csv_rank = df_rank_export.to_csv(sep=";", decimal=",", index=False, encoding="utf-8-sig")
                st.download_button(
                    f"⬇️ Baixar Top {len(df_top)} ({', '.join(cat_filtro) if cat_filtro else 'Todas'})",
                    data=csv_rank,
                    file_name=f"ranking_ajfans_{shopping_nome.replace(' ', '_')}.csv",
                    mime="text/csv", key="ajfans_download_rank", use_container_width=True
                )
            else:
                st.info("Nenhum cliente encontrado com os filtros selecionados.")

    with tab3:
        st.markdown("##### 📋 Lista Completa de Clientes")
        st.caption(
            "Lista completa de todos os clientes AJFANS do shopping. "
            "Filtre por categoria e baixe o arquivo para usar em campanhas de comunicação, "
            "mailings ou ações direcionadas."
        )
        cat_lista = st.multiselect("Filtrar por Categoria", ["MegaFan", "SuperFan", "NewFan"],
                                    key="ajfans_cat_lista")

        if not df_rank.empty:
            df_lista = df_rank.copy()
            if cat_lista:
                df_lista = df_lista[df_lista["categoria"].isin(cat_lista)]

            df_lista["primeiro_nome"] = df_lista["nome"].fillna("").str.split().str[0].str.title()
            colunas_lista = ["cliente_id", "primeiro_nome", "nome", "categoria",
                             "valor_total", "qtd_cupons", "email", "celular", "cidade"]
            colunas_lista = [c for c in colunas_lista if c in df_lista.columns]

            st.markdown(f'<div class="counter">{len(df_lista):,} clientes</div>'.replace(",", "."),
                        unsafe_allow_html=True)
            st.dataframe(df_lista[colunas_lista].head(5000), height=500, hide_index=True, use_container_width=True)

            # Download completo
            df_lista_export = df_lista[colunas_lista].copy()
            for c in df_lista_export.select_dtypes(include=["category"]).columns:
                df_lista_export[c] = df_lista_export[c].astype(str)
            csv_lista = df_lista_export.to_csv(sep=";", decimal=",", index=False, encoding="utf-8-sig")
            cat_label = "_".join(cat_lista) if cat_lista else "todas"
            st.download_button(
                f"⬇️ Baixar lista completa ({len(df_lista):,} clientes)".replace(",", "."),
                data=csv_lista,
                file_name=f"lista_ajfans_{cat_label}_{shopping_nome.replace(' ', '_')}.csv",
                mime="text/csv", key="ajfans_download_lista", use_container_width=True
            )
            registrar_filtro(username, shopping_nome, "AJFANS_Categoria",
                             ", ".join(cat_lista) if cat_lista else "Todas")
        else:
            # Fallback: usar cliente_categoria.csv (sem dados de contato)
            df_lista = df_cat.copy()
            if cat_lista:
                df_lista = df_lista[df_lista["categoria"].isin(cat_lista)]
            st.markdown(f'<div class="counter">{len(df_lista):,} clientes</div>'.replace(",", "."),
                        unsafe_allow_html=True)
            st.dataframe(df_lista[["cliente_id", "categoria"]], height=500, hide_index=True, use_container_width=True)


# ==============================================================================
# EXECUÇÃO
# ==============================================================================

try:
    if not verificar_autenticacao():
        st.stop()
    pagina_dashboard()
except Exception as e:
    import traceback
    st.error(f"Erro: {e}")
    st.code(traceback.format_exc())
