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
.logout-btn button { background-color: #E74C3C !important; color: white !important; border: none !important; width: 100%; }
.logout-btn button:hover { background-color: #C0392B !important; }
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


def _resolver_periodo_pasta(periodo):
    if periodo == "Completo":
        return os.path.join(RESULTADOS_DIR, "Completo")
    elif periodo.startswith("20") and len(periodo) == 4:
        return os.path.join(RESULTADOS_DIR, "Por_Ano", periodo)
    else:
        return os.path.join(RESULTADOS_DIR, "Por_Trimestre", periodo)


@st.cache_data(ttl=3600, max_entries=5)
def carregar_top_consumidores(periodo, shopping_nome):
    pasta = _resolver_periodo_pasta(periodo)
    caminho = os.path.join(pasta, "top_consumidores_rfv.csv")
    if not os.path.exists(caminho):
        return pd.DataFrame()
    # Ler apenas colunas do shopping para economizar memória
    df = pd.read_csv(caminho, sep=";", decimal=",", encoding="utf-8-sig")
    if shopping_nome:
        df = df[df["Shopping"] == shopping_nome].copy()
    df = optimize_dtypes(df)
    return df


@st.cache_data(ttl=3600)
def carregar_loja_info(periodo, shopping_nome):
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


@st.cache_data(ttl=3600)
def carregar_cliente_loja(periodo):
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


def descobrir_periodos():
    periodos = []
    if os.path.exists(os.path.join(RESULTADOS_DIR, "Completo", "top_consumidores_rfv.csv")):
        periodos.append("Completo")
    pasta_anos = os.path.join(RESULTADOS_DIR, "Por_Ano")
    if os.path.exists(pasta_anos):
        periodos.extend(sorted([d for d in os.listdir(pasta_anos) if os.path.isdir(os.path.join(pasta_anos, d))], reverse=True))
    pasta_tri = os.path.join(RESULTADOS_DIR, "Por_Trimestre")
    if os.path.exists(pasta_tri):
        periodos.extend(sorted([d for d in os.listdir(pasta_tri) if os.path.isdir(os.path.join(pasta_tri, d))], reverse=True))
    return periodos if periodos else ["Completo"]


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

        if role == "admin":
            st.divider()
            pagina_selecionada = st.radio(
                "Navegação", ["📊 Dashboard", "⚙️ Administração"],
                key="nav_admin", label_visibility="collapsed")
        else:
            pagina_selecionada = "📊 Dashboard"

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

        periodos = descobrir_periodos()
        periodo = st.selectbox("📅 Período", periodos, index=0)

        st.divider()
        st.markdown('<div class="logout-btn">', unsafe_allow_html=True)
        if st.button("🚪 Sair", key="logout_btn", use_container_width=True):
            for key in ["authentication_status", "shopping_nome", "username", "name", "role"]:
                st.session_state[key] = None
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    # Roteamento
    if role == "admin" and pagina_selecionada == "⚙️ Administração":
        pagina_admin()
        return

    # CARREGAR DADOS
    df = carregar_top_consumidores(periodo, shopping_nome)
    if df.empty:
        st.warning(f"Nenhum dado encontrado para **{shopping_nome}** no período **{periodo}**.")
        return
    df_loja_info = carregar_loja_info(periodo, shopping_nome)
    df_cliente_loja = carregar_cliente_loja(periodo)

    # HEADER
    st.markdown(f'<p class="main-header">📊 Top Consumidores — {shopping_nome}</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="sub-header">Período: {periodo} · Dados para ações de relacionamento</p>', unsafe_allow_html=True)

    # KPIs
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

    # FILTROS
    st.markdown("#### 🔍 Filtros")
    fcol1, fcol2, fcol3 = st.columns(3)
    with fcol1:
        perfis_disponiveis = sorted(df["Perfil_Cliente"].unique().tolist())
        perfil_filtro = st.multiselect("Perfil", perfis_disponiveis, key="filtro_perfil")
    with fcol2:
        segmentos_disponiveis = sorted(df["Segmento_Principal"].dropna().unique().tolist())
        segmento_filtro = st.multiselect("Segmento", segmentos_disponiveis, key="filtro_segmento")
    with fcol3:
        # Lojas filtradas: só do shopping e, se segmento selecionado, só dos segmentos escolhidos
        if not df_loja_info.empty:
            df_lojas_filtradas = df_loja_info.copy()
            if segmento_filtro:
                df_lojas_filtradas = df_lojas_filtradas[df_lojas_filtradas["segmento"].isin(segmento_filtro)]
            lojas_disponiveis = sorted(df_lojas_filtradas["loja_nome"].dropna().unique().tolist())
        else:
            lojas_disponiveis = []
        loja_filtro = st.multiselect("Loja", lojas_disponiveis, key="filtro_loja")

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

    # TABELA
    colunas_exibir = [
        "Ranking", "Nome", "CPF", "Email", "Celular", "Bairro", "Cidade", "Genero",
        "Valor_Total", "Frequencia_Compras", "Recencia_Dias",
        "Data_Primeira_Compra", "Data_Ultima_Compra",
        "Segmento_Principal", "Loja_Favorita_Shopping", "Perfil_Cliente",
    ]
    colunas_existentes = [c for c in colunas_exibir if c in df_filtrado.columns]
    st.dataframe(df_filtrado[colunas_existentes], height=500, hide_index=True, use_container_width=True)

    # DOWNLOADS
    st.markdown("#### 📥 Exportar Dados")
    dcol1, dcol2 = st.columns(2)
    df_export = df_filtrado[colunas_existentes].copy()
    for col in df_export.select_dtypes(include=["category"]).columns:
        df_export[col] = df_export[col].astype(str)
    csv_filename = f"top_consumidores_{shopping_nome.replace(' ', '_')}_{periodo}.csv"
    xlsx_filename = f"top_consumidores_{shopping_nome.replace(' ', '_')}_{periodo}.xlsx"
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
    st.markdown("#### 📈 Análises")
    cores_perfil = {"VIP": "#FFD700", "Premium": "#C0C0C0", "Potencial": "#CD7F32", "Pontual": "#808080"}
    tab1, tab2, tab3 = st.tabs(["Por Perfil", "Por Segmento", "Por Loja"])

    with tab1:
        gcol1, gcol2 = st.columns(2)
        with gcol1:
            perfil_counts = df_filtrado["Perfil_Cliente"].value_counts().reset_index()
            perfil_counts.columns = ["Perfil", "Clientes"]
            fig_pie = px.pie(perfil_counts, names="Perfil", values="Clientes", title="Distribuição por Perfil",
                             color="Perfil", color_discrete_map=cores_perfil)
            fig_pie.update_traces(textposition="inside", textinfo="percent+label")
            render_chart(fig_pie, key="pie_perfil")
        with gcol2:
            perfil_valor = df_filtrado.groupby("Perfil_Cliente")["Valor_Total"].mean().reset_index()
            perfil_valor.columns = ["Perfil", "Valor Médio"]
            perfil_valor = perfil_valor.sort_values("Valor Médio", ascending=True)
            fig_bar = px.bar(perfil_valor, x="Valor Médio", y="Perfil", orientation="h",
                             title="Valor Médio por Perfil (R$)", color="Perfil", color_discrete_map=cores_perfil)
            fig_bar.update_traces(texttemplate="R$ %{x:,.0f}", textposition="outside")
            fig_bar.update_layout(showlegend=False)
            render_chart(fig_bar, key="bar_perfil")

    with tab2:
        seg_valor = df_filtrado.groupby("Segmento_Principal")["Valor_Total"].sum().reset_index()
        seg_valor.columns = ["Segmento", "Valor Total"]
        seg_valor = seg_valor.sort_values("Valor Total", ascending=True).tail(10)
        fig_seg = px.bar(seg_valor, x="Valor Total", y="Segmento", orientation="h",
                         title="Top 10 Segmentos por Valor Total (R$)", color_discrete_sequence=["#3498DB"])
        fig_seg.update_traces(texttemplate="R$ %{x:,.0f}", textposition="outside")
        fig_seg.update_layout(showlegend=False)
        render_chart(fig_seg, key="bar_segmento")

    with tab3:
        if not df_loja_info.empty:
            loja_top = df_loja_info.sort_values("valor", ascending=True).tail(10)
            fig_loja = px.bar(loja_top, x="valor", y="loja_nome", orientation="h",
                              title="Top 10 Lojas por Valor (R$)", color_discrete_sequence=["#2ECC71"])
            fig_loja.update_traces(texttemplate="R$ %{x:,.0f}", textposition="outside")
            fig_loja.update_layout(showlegend=False, yaxis_title="Loja")
            render_chart(fig_loja, key="bar_loja")
        else:
            st.info("Dados de loja não disponíveis para este período.")

    st.markdown("---")

    # AÇÕES RECOMENDADAS
    st.markdown("#### 🎯 Ações Recomendadas")
    st.markdown('<p class="sub-header">Sugestões automáticas baseadas nos dados dos seus top clientes</p>', unsafe_allow_html=True)

    vips = df_filtrado[df_filtrado["Perfil_Cliente"] == "VIP"]
    vips_risco = vips[vips["Recencia_Dias"] > 60] if not vips.empty else pd.DataFrame()
    if not vips_risco.empty:
        nomes_vip = ", ".join(vips_risco["Nome"].head(5).tolist())
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

    premium = df_filtrado[df_filtrado["Perfil_Cliente"] == "Premium"]
    if not premium.empty:
        top_premium = premium.nlargest(5, "Valor_Total")
        nomes_premium = ", ".join(top_premium["Nome"].tolist())
        st.markdown(f"""<div class="action-card atencao">
            <h4>⬆️ Premium para Upgrade — {len(top_premium)} candidato(s)</h4>
            <p>Estes clientes Premium estão próximos de se tornarem VIP: <strong>{nomes_premium}</strong></p>
            <p class="acao">💡 Ação: programa de incentivo, convites para eventos, benefícios progressivos</p>
        </div>""", unsafe_allow_html=True)

    if not df_filtrado.empty:
        seg_totais = df_filtrado.groupby("Segmento_Principal")["Valor_Total"].sum()
        seg_top = seg_totais.idxmax()
        seg_pct = seg_totais.max() / seg_totais.sum() * 100
        st.markdown(f"""<div class="action-card oportunidade">
            <h4>🏆 Segmento Dominante — {seg_top}</h4>
            <p><strong>{seg_pct:.1f}%</strong> do faturamento dos top clientes vem do segmento <strong>{seg_top}</strong>.</p>
            <p class="acao">💡 Ação: reforçar parcerias com lojas deste segmento, criar promoções temáticas</p>
        </div>""", unsafe_allow_html=True)

    if "Loja_Favorita_Shopping" in df_filtrado.columns:
        loja_counts = df_filtrado["Loja_Favorita_Shopping"].value_counts()
        if not loja_counts.empty:
            st.markdown(f"""<div class="action-card oportunidade">
                <h4>⭐ Loja Destaque — {loja_counts.index[0]}</h4>
                <p>A loja <strong>{loja_counts.index[0]}</strong> é a favorita de <strong>{loja_counts.iloc[0]}</strong> dos top clientes.</p>
                <p class="acao">💡 Ação: ações conjuntas, eventos exclusivos, programa de fidelidade com a loja</p>
            </div>""", unsafe_allow_html=True)

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
            <p>Todos os top clientes compraram nos últimos 90 dias. Excelente engajamento!</p>
            <p class="acao">💡 Ação: aproveitar o momento para ampliar ticket médio e cross-sell</p>
        </div>""", unsafe_allow_html=True)


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
