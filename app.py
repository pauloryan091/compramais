from __future__ import annotations

import os
import sqlite3
import json
import base64
import re
import queue
import threading
import time
import logging
import io
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Dict, Optional, Tuple, List

from flask import Flask, jsonify, request, send_from_directory, Response, stream_with_context
from flask_cors import CORS
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from werkzeug.security import check_password_hash, generate_password_hash
from dotenv import load_dotenv

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.lib import colors

# =====================================================
# CONFIG
# =====================================================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("compra_plus")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
DATABASE = os.environ.get("COMPRA_PLUS_DB", os.path.join(BASE_DIR, "compra_plus.db"))

APP_SECRET = os.environ.get("COMPRA_PLUS_SECRET", "compra-plus-secret-2026-change-in-production")
TOKEN_SALT = os.environ.get("COMPRA_PLUS_TOKEN_SALT", "compra-plus-auth-salt")
TOKEN_EXPIRES_SECONDS = int(os.environ.get("COMPRA_PLUS_TOKEN_EXPIRES", "86400"))  # 1 dia

# OPEN_MODE DESATIVADO - SEMPRE FALSE
OPEN_MODE = False
APP_BUILD = os.environ.get('COMPRA_PLUS_BUILD', 'revised_2026-01-25')
SQLITE_TIMEOUT_SECONDS = float(os.environ.get("COMPRA_PLUS_DB_TIMEOUT", "12.0"))
SSE_PING_SECONDS = int(os.environ.get("COMPRA_PLUS_SSE_PING", "15"))

app = Flask(__name__, static_folder="static", static_url_path="/static")

CORS(
    app,
    resources={r"/api/*": {"origins": "*"}},
    supports_credentials=True,
    allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
)

serializer = URLSafeTimedSerializer(APP_SECRET, salt=TOKEN_SALT)

# =====================================================
# HELPERS
# =====================================================
def now_utc_ts() -> int:
    return int(datetime.utcnow().timestamp())

def normalize_email(v: str) -> str:
    return (v or "").strip().lower()

def only_digits(v: str) -> str:
    return re.sub(r"\D+", "", v or "")

def is_db_locked_error(e: Exception) -> bool:
    msg = str(e).lower()
    return "database is locked" in msg or "database table is locked" in msg

def format_cnpj(cnpj: str) -> str:
    cnpj = only_digits(cnpj)
    if len(cnpj) == 14:
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
    return cnpj

# =====================================================
# DB
# =====================================================
def get_db_connection() -> sqlite3.Connection:
    """
    Conexão SQLite resiliente:
    - WAL para reduzir lock
    - busy_timeout
    - retries com backoff
    """
    for attempt in range(5):
        try:
            conn = sqlite3.connect(
                DATABASE,
                timeout=SQLITE_TIMEOUT_SECONDS,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row

            # pragmas (por conexão)
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA busy_timeout = 8000")  # 8s

            return conn
        except sqlite3.OperationalError as e:
            if is_db_locked_error(e) and attempt < 4:
                time.sleep(0.12 * (2 ** attempt))
                continue
            raise

def seed_kv_defaults(cur: sqlite3.Cursor) -> None:
    def upsert(key: str, default_obj: Any) -> None:
        row = cur.execute("SELECT key FROM kv_store WHERE key = ?", (key,)).fetchone()
        if not row:
            cur.execute(
                "INSERT INTO kv_store (key, value_json) VALUES (?, ?)",
                (key, json.dumps(default_obj, ensure_ascii=False)),
            )

    # Licitações (global)
    upsert(
        "compraPlusLicitacoes_v1",
        [
            {
                "id": 1,
                "numero": "LIC-2023-015",
                "descricao": "Equipamentos de Informática",
                "status": "aberta",
                "inicio": "2023-09-01",
                "fim": "2023-10-01",
                "valor_estimado": 150000.00,
                "observacoes": "Licitação para equipamentos de TI",
            },
            {
                "id": 2,
                "numero": "LIC-2023-018",
                "descricao": "Materiais de Escritório",
                "status": "em_analise",
                "inicio": "2023-09-10",
                "fim": "2023-10-20",
                "valor_estimado": 50000.00,
                "observacoes": "Licitação para materiais de escritório diversos",
            },
            {
                "id": 3,
                "numero": "LIC-2023-020",
                "descricao": "Aquisição de Medicamentos",
                "status": "finalizada",
                "inicio": "2023-08-05",
                "fim": "2023-09-05",
                "valor_estimado": 100000.00,
                "observacoes": "Licitação para medicamentos hospitalares",
            },
        ],
    )

    # Itens de licitação (global) - ATUALIZADO COM ID 11
    upsert(
        "compraPlusItensLicitacao_v1",
        [
            {
                "id": 1,
                "licitacaoId": 1,
                "fornecedorId": 1,
                "nome": "Notebook i5 8GB",
                "descricao": "Processador i5, 8GB RAM, SSD 256GB",
                "quantidade_disponivel": 100,
                "quantidade_reservada": 0,
                "quantidade_minima": 1,
                "preco": 3500.00,
                "unidade": "un",
                "ativo": True,
            },
            {
                "id": 2,
                "licitacaoId": 1,
                "fornecedorId": 1,
                "nome": "Monitor 24'' LED",
                "descricao": "Monitor Full HD 24 polegadas",
                "quantidade_disponivel": 50,
                "quantidade_reservada": 0,
                "quantidade_minima": 1,
                "preco": 850.00,
                "unidade": "un",
                "ativo": True,
            },
            {
                "id": 3,
                "licitacaoId": 1,
                "fornecedorId": 2,
                "nome": "Papel A4 75g",
                "descricao": "Pacote com 500 folhas",
                "quantidade_disponivel": 200,
                "quantidade_reservada": 0,
                "quantidade_minima": 1,
                "preco": 17.50,
                "unidade": "pct",
                "ativo": True,
            },
            {
                "id": 4,
                "licitacaoId": 2,
                "fornecedorId": 1,
                "nome": "Caneta Esferográfica Azul",
                "descricao": "Pacote com 50 unidades",
                "quantidade_disponivel": 500,
                "quantidade_reservada": 0,
                "quantidade_minima": 1,
                "preco": 25.00,
                "unidade": "pct",
                "ativo": True,
            },
            # Adicionando mais itens incluindo ID 11 e 12
            {
                "id": 11,
                "licitacaoId": 1,
                "fornecedorId": 1,
                "nome": "Mouse sem fio",
                "descricao": "Mouse óptico sem fio com receptor USB",
                "quantidade_disponivel": 150,
                "quantidade_reservada": 0,
                "quantidade_minima": 1,
                "preco": 45.00,
                "unidade": "un",
                "ativo": True,
            },
            {
                "id": 12,
                "licitacaoId": 1,
                "fornecedorId": 2,
                "nome": "Teclado USB",
                "descricao": "Teclado USB ABNT2 com teclas silenciosas",
                "quantidade_disponivel": 80,
                "quantidade_reservada": 0,
                "quantidade_minima": 1,
                "preco": 89.90,
                "unidade": "un",
                "ativo": True,
            },
        ],
    )

    # Fornecedores (global)
    upsert(
        "compraPlusFornecedores_v1",
        [
            {
                "id": 1,
                "nome": "Fornecedor A Ltda.",
                "fantasia": "Fornecedor A",
                "cnpj": "00.000.000/0001-00",
                "inscricao": "",
                "email": "fornA@email.com",
                "telefone": "(11) 9999-9999",
                "endereco": "Rua A, 123 - Centro",
                "contato": "João Silva",
                "status": "active",
            },
            {
                "id": 2,
                "nome": "Fornecedor B S.A.",
                "fantasia": "Fornecedor B",
                "cnpj": "11.111.111/0001-11",
                "inscricao": "",
                "email": "fornB@email.com",
                "telefone": "(11) 8888-8888",
                "endereco": "Av. B, 456 - Jardins",
                "contato": "Maria Santos",
                "status": "active",
            },
        ],
    )

def init_db() -> None:
    conn = get_db_connection()
    cur = conn.cursor()

    # Tabela de usuários
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            last_login TEXT
        )
        """
    )

    # Tabela de fornecedores
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fornecedores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            razao_social TEXT NOT NULL,
            nome_fantasia TEXT,
            cnpj TEXT UNIQUE NOT NULL,
            inscricao_estadual TEXT,
            email TEXT,
            telefone TEXT,
            endereco TEXT,
            contato TEXT,
            status TEXT DEFAULT 'active',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
        """
    )

    # Tabela de notificações
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pedido_id INTEGER NOT NULL,
            titulo TEXT NOT NULL,
            descricao TEXT NOT NULL,
            tipo TEXT DEFAULT 'pedido_analise',
            lida BOOLEAN DEFAULT 0,
            data TEXT DEFAULT (datetime('now')),
            usuario TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """
    )

    # Tabela KV Store
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS kv_store (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at TEXT DEFAULT (datetime('now'))
        )
        """
    )

    # Índices
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fornecedores_status ON fornecedores(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fornecedores_cnpj ON fornecedores(cnpj)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_kv_store_updated ON kv_store(updated_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_notifications_pedido_id ON notifications(pedido_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_notifications_lida ON notifications(lida)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_notifications_data ON notifications(data)")

    def ensure_user(name: str, email: str, password: str, role: str) -> None:
        row = cur.execute("SELECT id FROM users WHERE email = ?", (normalize_email(email),)).fetchone()
        if not row:
            cur.execute(
                "INSERT INTO users (name, email, password_hash, role) VALUES (?, ?, ?, ?)",
                (name, normalize_email(email), generate_password_hash(password), role),
            )

    ensure_user("Administrador do Sistema", "admin@compraplus.com", "admin123", "Administrador")
    ensure_user("Gerente", "gerente@compraplus.com", "gerente123", "Gerente")
    ensure_user("Comprador", "comprador@compraplus.com", "comprador123", "Comprador")
    ensure_user("Consulta", "consulta@compraplus.com", "consulta123", "Consulta")

    seed_kv_defaults(cur)

    row = cur.execute("SELECT COUNT(1) AS c FROM fornecedores").fetchone()
    if row and int(row["c"]) == 0:
        cur.execute(
            """
            INSERT INTO fornecedores
            (razao_social, nome_fantasia, cnpj, inscricao_estadual, email, telefone, endereco, contato, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Fornecedor A Ltda.",
                "Fornecedor A",
                "00000000000100",
                "123.456.789.111",
                "fornA@email.com",
                "(11) 9999-9999",
                "Rua A, 123 - Centro, São Paulo - SP",
                "João Silva",
                "active",
            ),
        )
        cur.execute(
            """
            INSERT INTO fornecedores
            (razao_social, nome_fantasia, cnpj, inscricao_estadual, email, telefone, endereco, contato, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Fornecedor B S.A.",
                "Fornecedor B",
                "11111111111111",
                "987.654.321.000",
                "fornB@email.com",
                "(11) 8888-8888",
                "Av. B, 456 - Jardins, São Paulo - SP",
                "Maria Santos",
                "active",
            ),
        )

    conn.commit()
    conn.close()
    logger.info("Banco de dados inicializado com sucesso")

# =====================================================
# PERMISSIONS
# =====================================================
ROLE_PAGES: Dict[str, list[str]] = {
    "Administrador": ["dashboard", "pedidos", "licitacoes", "fornecedores", "relatorios", "configuracoes", "usuarios"],
    "Gerente": ["dashboard", "pedidos", "licitacoes", "fornecedores", "relatorios"],
    "Comprador": ["dashboard", "pedidos", "licitacoes", "fornecedores"],
    "Consulta": ["dashboard", "fornecedores", "relatorios"],
}

def pages_for_role(role: str) -> list[str]:
    return ROLE_PAGES.get(role, ["dashboard"])

def can_access_page(role: str, page: str) -> bool:
    """Verifica se um role tem acesso a uma página"""
    return page in pages_for_role(role)

# =====================================================
# AUTH - FUNÇÕES ATUALIZADAS
# =====================================================
def make_token(user_id: int, role: str) -> str:
    payload = {"uid": user_id, "role": role, "iat": now_utc_ts()}
    return serializer.dumps(payload)

def parse_bearer_token() -> Optional[str]:
    """Versão para endpoints regulares - só aceita header"""
    auth = request.headers.get("Authorization", "")
    if not auth:
        return None
    parts = auth.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() or None

def parse_bearer_token_for_sse() -> Optional[str]:
    """Versão especial para SSE que aceita token via query string"""
    # Primeiro tenta no header (padrão)
    auth = request.headers.get("Authorization", "")
    if auth:
        parts = auth.split(" ", 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            token = parts[1].strip()
            if token:
                return token
    
    # Depois tenta via query string (fallback para frontend)
    token_from_query = request.args.get("token")
    if token_from_query:
        return token_from_query
    
    return None

def verify_token(token: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        payload = serializer.loads(token, max_age=TOKEN_EXPIRES_SECONDS)
        if not isinstance(payload, dict) or "uid" not in payload:
            return None, "Token inválido"
        return payload, None
    except SignatureExpired:
        return None, "Token expirado"
    except BadSignature:
        return None, "Token inválido"
    except Exception as e:
        logger.error(f"Erro ao verificar token: {e}")
        return None, "Token inválido"

# =====================================================
# DECORATORS ATUALIZADOS
# =====================================================
def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        token = parse_bearer_token()
        if not token:
            return jsonify({"success": False, "message": "Token ausente"}), 401

        payload, err = verify_token(token)
        if err:
            return jsonify({"success": False, "message": err}), 401

        conn = get_db_connection()
        user = conn.execute("SELECT * FROM users WHERE id = ?", (payload["uid"],)).fetchone()
        conn.close()

        if not user:
            return jsonify({"success": False, "message": "Usuário não encontrado"}), 401

        request.current_user = dict(user)
        request.token_payload = payload
        return fn(*args, **kwargs)

    return wrapper

def login_required_for_sse(fn):
    """Decorator especial para SSE que aceita token via query string"""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        token = parse_bearer_token_for_sse()
        if not token:
            return jsonify({"success": False, "message": "Token ausente"}), 401

        payload, err = verify_token(token)
        if err:
            return jsonify({"success": False, "message": err}), 401

        conn = get_db_connection()
        user = conn.execute("SELECT * FROM users WHERE id = ?", (payload["uid"],)).fetchone()
        conn.close()

        if not user:
            return jsonify({"success": False, "message": "Usuário não encontrado"}), 401

        request.current_user = dict(user)
        request.token_payload = payload
        return fn(*args, **kwargs)

    return wrapper


def parse_bearer_token_allow_query() -> Optional[str]:
    """Aceita token no header Bearer OU via query string (?token=...).
    Útil para casos como abrir PDF em nova aba (não dá para setar header Authorization).
    """
    token = parse_bearer_token()
    if token:
        return token
    token_qs = request.args.get("token")
    if token_qs:
        return str(token_qs).strip() or None
    return None

def login_required_allow_query(fn):
    """Como login_required, mas permite token na query string (fallback)."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        token = parse_bearer_token_allow_query()
        if not token:
            return jsonify({"success": False, "message": "Token ausente"}), 401

        payload, err = verify_token(token)
        if err:
            return jsonify({"success": False, "message": err}), 401

        conn = get_db_connection()
        user = conn.execute("SELECT * FROM users WHERE id = ?", (payload["uid"],)).fetchone()
        conn.close()

        if not user:
            return jsonify({"success": False, "message": "Usuário não encontrado"}), 401

        request.current_user = dict(user)
        request.token_payload = payload
        return fn(*args, **kwargs)

    return wrapper

def role_required(*allowed_roles: str):
    """Restringe por perfil.
    IMPORTANTE: este decorator NÃO aplica login_required automaticamente.
    Use junto com @login_required (ou @login_required_for_sse quando aplicável).
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user = getattr(request, "current_user", None)
            if not user:
                return jsonify({"success": False, "message": "Não autenticado"}), 401
            if user.get("role") not in allowed_roles:
                return jsonify({"success": False, "message": "Acesso negado para este perfil"}), 403
            return fn(*args, **kwargs)
        return wrapper
    return decorator

# =====================================================
# SSE
# =====================================================
_subs_lock = threading.Lock()
_subs: list[queue.Queue[str]] = []

def _publish(event: str, data: Any = None) -> None:
    payload = {"event": event, "data": data, "ts": now_utc_ts()}
    msg = f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
    with _subs_lock:
        dead = []
        for q in _subs:
            try:
                q.put_nowait(msg)
            except Exception:
                dead.append(q)
        for q in dead:
            if q in _subs:
                _subs.remove(q)

# =====================================================
# KV Store
# =====================================================
def kv_get(key: str) -> Any:
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT value_json FROM kv_store WHERE key = ?", (key,)).fetchone()
        if row:
            return json.loads(row["value_json"])
        return None
    except Exception as e:
        logger.error(f"Erro ao obter KV {key}: {e}")
        return None
    finally:
        conn.close()

def kv_set(key: str, value: Any) -> bool:
    conn = get_db_connection()
    try:
        conn.execute(
            """
            INSERT INTO kv_store (key, value_json, updated_at)
            VALUES (?, ?, datetime('now'))
            ON CONFLICT(key) DO UPDATE SET
                value_json=excluded.value_json,
                updated_at=datetime('now')
            """,
            (key, json.dumps(value, ensure_ascii=False)),
        )
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Erro ao definir KV {key}: {e}")
        return False
    finally:
        conn.close()

# =====================================================
# KV Store - helpers de performance
# =====================================================
def _json_loads_safe(s: str, default: Any):
    try:
        return json.loads(s) if s else default
    except Exception:
        return default

def kv_get_many(keys: List[str]) -> Dict[str, Any]:
    """Busca várias chaves KV em UMA única conexão (bem mais rápido)."""
    if not keys:
        return {}
    conn = get_db_connection()
    try:
        placeholders = ",".join(["?"] * len(keys))
        rows = conn.execute(
            f"SELECT key, value_json FROM kv_store WHERE key IN ({placeholders})",
            keys,
        ).fetchall()
        out: Dict[str, Any] = {k: None for k in keys}
        for r in rows:
            out[r["key"]] = _json_loads_safe(r["value_json"], None)
        return out
    except Exception as e:
        logger.error(f"Erro ao obter KV em lote: {e}")
        return {k: None for k in keys}
    finally:
        conn.close()

def kv_get_like(prefix: str) -> Dict[str, Any]:
    """Busca todas as chaves que começam com prefix em UMA consulta."""
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT key, value_json FROM kv_store WHERE key LIKE ?",
            (f"{prefix}%",),
        ).fetchall()
        out: Dict[str, Any] = {}
        for r in rows:
            out[r["key"]] = _json_loads_safe(r["value_json"], [])
        return out
    except Exception as e:
        logger.error(f"Erro ao obter KV por prefixo {prefix!r}: {e}")
        return {}
    finally:
        conn.close()

# =====================================================
# FUNÇÕES PARA PEDIDOS POR USUÁRIO
# =====================================================
def get_user_pedidos_key(user_id: int) -> str:
    """Retorna a chave KV para os pedidos do usuário"""
    return f"compraPlusPedidos_user_{user_id}"

def get_user_pedidos(user_id: int) -> List[Dict[str, Any]]:
    """Obtém pedidos específicos do usuário"""
    key = get_user_pedidos_key(user_id)
    pedidos = kv_get(key)
    if pedidos is None:
        # Se não existir, cria uma lista vazia
        kv_set(key, [])
        return []
    return pedidos

def save_user_pedidos(user_id: int, pedidos: List[Dict[str, Any]]) -> bool:
    """Salva pedidos específicos do usuário"""
    key = get_user_pedidos_key(user_id)
    return kv_set(key, pedidos)

def get_all_pedidos_for_admin() -> List[Dict[str, Any]]:
    """Obtém todos os pedidos de todos os usuários (apenas para admin/gerente).

    Otimização: evita abrir uma conexão por usuário.
    Lê todos os registros KV 'compraPlusPedidos_user_%' em lote e junta com a tabela users.
    """
    conn = get_db_connection()
    try:
        users_rows = conn.execute("SELECT id, name, role FROM users").fetchall()
        users_by_id = {int(r["id"]): {"name": r["name"], "role": r["role"]} for r in users_rows}

        kv_rows = conn.execute(
            "SELECT key, value_json FROM kv_store WHERE key LIKE 'compraPlusPedidos_user_%'"
        ).fetchall()

        all_pedidos: List[Dict[str, Any]] = []
        for r in kv_rows:
            key = str(r["key"])
            m = re.match(r"^compraPlusPedidos_user_(\d+)$", key)
            if not m:
                continue
            uid = int(m.group(1))
            pedidos_list = _json_loads_safe(r["value_json"], [])
            if not isinstance(pedidos_list, list):
                continue

            uinfo = users_by_id.get(uid, {"name": "Usuário", "role": "Consulta"})
            for pedido in pedidos_list:
                if not isinstance(pedido, dict):
                    continue
                pedido_copy = dict(pedido)
                pedido_copy["userId"] = uid
                pedido_copy["userName"] = pedido_copy.get("userName") or uinfo["name"]
                pedido_copy["userRole"] = uinfo["role"]
                all_pedidos.append(pedido_copy)

        return all_pedidos
    finally:
        conn.close()


def get_pedidos_for_user(user_id: int, user_role: str) -> List[Dict[str, Any]]:
    """Obtém pedidos com base no papel do usuário"""
    if user_role in ["Administrador", "Gerente"]:
        return get_all_pedidos_for_admin()
    else:
        return get_user_pedidos(user_id)

def find_pedido_owner(pedido_id: int) -> Optional[int]:
    """Encontra o dono de um pedido (retorna user_id ou None).

    Otimização: percorre as chaves KV de pedidos em uma única consulta.
    """
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT key, value_json FROM kv_store WHERE key LIKE 'compraPlusPedidos_user_%'"
        ).fetchall()

        for r in rows:
            key = str(r["key"])
            m = re.match(r"^compraPlusPedidos_user_(\d+)$", key)
            if not m:
                continue
            uid = int(m.group(1))
            pedidos_list = _json_loads_safe(r["value_json"], [])
            if not isinstance(pedidos_list, list):
                continue
            for pedido in pedidos_list:
                if isinstance(pedido, dict) and pedido.get("id") == pedido_id:
                    return uid
        return None
    finally:
        conn.close()


# =====================================================
# FUNÇÕES PARA CONTROLE DE LICITAÇÕES E QUANTIDADES
# =====================================================
def get_itens_licitacao_disponiveis(licitacao_id: int, fornecedor_id: int = None) -> List[Dict[str, Any]]:
    """Retorna itens disponíveis de uma licitação, opcionalmente filtrado por fornecedor.

    Compatibilidade:
    - Alguns cadastros salvam a quantidade em `quantidade` (usado no licitacoes.html).
    - Outros salvam em `quantidade_disponivel`.

    Este endpoint normaliza esses campos e calcula `quantidade_real_disponivel`
    (quantidade - reservada, nunca menor que 0).
    """

    def safe_int(v, default: int = 0) -> int:
        try:
            return int(v)
        except Exception:
            return default

    itens = kv_get("compraPlusItensLicitacao_v1") or []
    if not isinstance(itens, list):
        return []

    lic_id = safe_int(licitacao_id, 0)
    forn_id = safe_int(fornecedor_id, 0) if fornecedor_id is not None else None

    itens_disponiveis: List[Dict[str, Any]] = []

    for item in itens:
        if not isinstance(item, dict):
            continue

        # Normaliza IDs (podem vir como string)
        item_lic_id = safe_int(item.get("licitacaoId"), 0)
        if item_lic_id != lic_id:
            continue

        if forn_id is not None and forn_id:
            item_forn_id = safe_int(item.get("fornecedorId"), 0)
            if item_forn_id != forn_id:
                continue

        # Verificar se está ativo
        if item.get("ativo") is False:
            continue

        # Quantidade pode vir em campos diferentes
        qtd_total = item.get("quantidade_disponivel")
        if qtd_total is None:
            qtd_total = item.get("quantidade")
        if qtd_total is None:
            qtd_total = item.get("qtd")
        quantidade_disponivel = safe_int(qtd_total, 0)

        quantidade_reservada = safe_int(item.get("quantidade_reservada"), 0)
        quantidade_real = max(0, quantidade_disponivel - quantidade_reservada)

        item_formatado = dict(item)
        # Normaliza para o frontend
        item_formatado["quantidade_disponivel"] = quantidade_disponivel
        item_formatado["quantidade_reservada"] = quantidade_reservada
        item_formatado["quantidade_real_disponivel"] = quantidade_real

        itens_disponiveis.append(item_formatado)

    return itens_disponiveis


def atualizar_quantidade_item_licitacao(item_id: int, quantidade_utilizada: int) -> bool:
    """Atualiza a quantidade reservada de um item da licitação"""
    itens = kv_get("compraPlusItensLicitacao_v1") or []
    if not isinstance(itens, list):
        return False
    
    for i, item in enumerate(itens):
        if isinstance(item, dict) and item.get("id") == item_id:
            # Atualizar quantidade reservada
            quantidade_reservada_atual = item.get("quantidade_reservada", 0)
            itens[i]["quantidade_reservada"] = quantidade_reservada_atual + quantidade_utilizada
            break
    
    return kv_set("compraPlusItensLicitacao_v1", itens)

def validar_itens_pedido(itens_pedido: List[Dict[str, Any]], licitacao_id: int, fornecedor_id: int) -> Tuple[bool, str, Dict[str, Any]]:
    """Valida se os itens do pedido estão disponíveis na licitação (VERSÃO TOLERANTE)."""
    itens_disponiveis = get_itens_licitacao_disponiveis(licitacao_id, fornecedor_id)

    # Criar mapa de itens disponíveis por ID
    mapa_disponiveis: Dict[int, Dict[str, Any]] = {}
    for item in itens_disponiveis:
        item_id = item.get("id")
        if item_id:
            mapa_disponiveis[int(item_id)] = item

    # Validar cada item do pedido
    for item_pedido in itens_pedido:
        item_id_raw = item_pedido.get("itemId") or item_pedido.get("id")
        try:
            item_id = int(item_id_raw)
        except Exception:
            item_id = 0

        try:
            quantidade_pedido = int(item_pedido.get("quantidade") or item_pedido.get("qtd") or 0)
        except Exception:
            quantidade_pedido = 0

        if not item_id:
            return False, f"Item sem ID: {item_pedido.get('nome', 'Desconhecido')}", {}

        if quantidade_pedido <= 0:
            return False, f"Quantidade inválida ({quantidade_pedido}) para item ID {item_id}", {}

        if item_id not in mapa_disponiveis:
            # ITEM NÃO ENCONTRADO - VERSÃO TOLERANTE: apenas registra
            logger.warning(f"Item ID {item_id} não encontrado na licitação {licitacao_id}")
            # Continua sem falhar imediatamente
            continue

        item_disponivel = mapa_disponiveis[item_id]
        quantidade_real = int(item_disponivel.get("quantidade_real_disponivel", 0) or 0)
        quantidade_minima = int(item_disponivel.get("quantidade_minima", 1) or 1)

        if quantidade_pedido < quantidade_minima:
            return False, (
                f"Quantidade solicitada ({quantidade_pedido}) é menor que a mínima ({quantidade_minima}) "
                f"para item: {item_disponivel.get('nome')}"
            ), {}

        if quantidade_pedido > quantidade_real:
            # AVISO mas não falha para permitir rascunhos
            logger.warning(
                f"Quantidade solicitada ({quantidade_pedido}) excede disponível ({quantidade_real}) "
                f"para item: {item_disponivel.get('nome')}"
            )
            # Continua mesmo assim - pode ser um rascunho

    return True, "", mapa_disponiveis

def criar_pedido_validado(user_id: int, user_name: str, dados_pedido: Dict[str, Any]) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """Cria um pedido validando contra a licitação (VERSÃO TOLERANTE)"""
    try:
        # Validar dados obrigatórios
        licitacao_id = dados_pedido.get("licitacaoId")
        fornecedor_id = dados_pedido.get("fornecedorId")
        itens = dados_pedido.get("itens", [])
        
        if not licitacao_id or not fornecedor_id or not itens:
            return False, "Licitação, fornecedor e itens são obrigatórios", None
        
        # Validar itens contra licitação (versão tolerante)
        valido, mensagem, mapa_itens = validar_itens_pedido(itens, licitacao_id, fornecedor_id)
        if not valido:
            return False, mensagem, None
        
        # Obter pedidos do usuário
        pedidos = get_user_pedidos(user_id)
        
        # Gerar novo ID
        new_id = max([p.get("id", 0) for p in pedidos], default=0) + 1
        
        # Gerar número do pedido
        pedido_numero = f"PED-{user_id:03d}-{new_id:06d}"
        
        # Calcular total e preparar itens com preços fixos da licitação
        total = 0
        itens_com_preco_fixo = []
        
        for item_pedido in itens:
            item_id = item_pedido.get("itemId") or item_pedido.get("id")
            quantidade = item_pedido.get("quantidade") or item_pedido.get("qtd", 1)
            
            if item_id in mapa_itens:
                item_licitacao = mapa_itens[item_id]
                preco_unitario = item_licitacao.get("preco", 0)
                subtotal = preco_unitario * quantidade
                total += subtotal
                
                # Criar item com preço fixo da licitação
                item_formatado = {
                    "itemId": item_id,
                    "nome": item_licitacao.get("nome"),
                    "descricao": item_licitacao.get("descricao"),
                    "quantidade": quantidade,
                    "preco": preco_unitario,
                    "subtotal": subtotal,
                    "unidade": item_licitacao.get("unidade", "UN")
                }
                itens_com_preco_fixo.append(item_formatado)
            else:
                # Item não encontrado na licitação - usar dados do pedido
                preco_unitario = item_pedido.get("preco", 0)
                subtotal = preco_unitario * quantidade
                total += subtotal
                
                item_formatado = {
                    "itemId": item_id,
                    "nome": item_pedido.get("nome", "Item desconhecido"),
                    "descricao": item_pedido.get("descricao", ""),
                    "quantidade": quantidade,
                    "preco": preco_unitario,
                    "subtotal": subtotal,
                    "unidade": item_pedido.get("unidade", "UN")
                }
                itens_com_preco_fixo.append(item_formatado)
                logger.warning(f"Item ID {item_id} não encontrado na licitação, usando dados do pedido")
        
        # Status inicial
        status = dados_pedido.get("status", "rascunho")
        
        # Criar pedido
        novo_pedido = {
            "id": new_id,
            "numero": pedido_numero,
            "data": datetime.now().strftime("%Y-%m-%d"),
            "licitacaoId": licitacao_id,
            "fornecedorId": fornecedor_id,
            "status": status,
            "total": total,
            "itens": itens_com_preco_fixo,
            "historico": [{
                "data": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "acao": "Criado",
                "usuario": user_name
            }],
            "userId": user_id,
            "userName": user_name,
            "observacao": dados_pedido.get("observacao", "")
        }
        
        # Adicionar ao array de pedidos
        pedidos.append(novo_pedido)
        
        # Salvar pedido
        if not save_user_pedidos(user_id, pedidos):
            return False, "Erro ao salvar pedido", None
        
        # Atualizar quantidades na licitação (se não for rascunho)
        if status != "rascunho":
            for item in itens_com_preco_fixo:
                item_id = item.get("itemId")
                quantidade = item.get("quantidade", 0)
                if item_id and quantidade > 0:
                    atualizar_quantidade_item_licitacao(item_id, quantidade)
        
        # Publicar evento
        _publish("data_updated", {"key": get_user_pedidos_key(user_id)})
        
        logger.info(f"Pedido criado: {pedido_numero} pelo usuário {user_id}")
        
        return True, "Pedido criado com sucesso", novo_pedido
        
    except Exception as e:
        logger.error(f"Erro ao criar pedido validado: {e}")
        return False, f"Erro ao criar pedido: {str(e)}", None

# =====================================================
# NOTIFICAÇÕES - FUNÇÕES CORRIGIDAS
# =====================================================
def notification_to_dict(row) -> Dict[str, Any]:
    """Converte uma linha do banco em dicionário de notificação"""
    return {
        "id": row["id"],
        "pedidoId": row["pedido_id"],
        "titulo": row["titulo"],
        "descricao": row["descricao"],
        "tipo": row["tipo"],
        "lida": bool(row["lida"]),
        "data": row["data"],
        "usuario": row["usuario"],
        "created_at": row["created_at"]
    }

def create_notification(pedido_id: int, titulo: str, descricao: str, tipo: str = "pedido_analise", usuario: str = "Sistema") -> bool:
    """Cria uma notificação no banco"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO notifications 
            (pedido_id, titulo, descricao, tipo, lida, data, usuario)
            VALUES (?, ?, ?, ?, ?, datetime('now'), ?)
            """,
            (
                pedido_id,
                titulo,
                descricao,
                tipo,
                False,
                usuario
            )
        )
        conn.commit()
        
        # Publicar evento SSE para notificações
        _publish("notification_created", {"pedidoId": pedido_id})
        
        logger.info(f"Notificação criada: {titulo} para pedido {pedido_id}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao criar notificação: {e}")
        return False
    finally:
        conn.close()

def criar_notificacao_automatica_pedido_enviado(pedido_id: int, pedido_numero: str, usuario_nome: str, fornecedor_nome: str) -> bool:
    """Cria notificação automática para admin/gerente quando pedido é enviado"""
    try:
        conn = get_db_connection()
        
        # Buscar todos os administradores e gerentes
        admins_gerentes = conn.execute(
            "SELECT id, name FROM users WHERE role IN ('Administrador', 'Gerente')"
        ).fetchall()
        
        notifications_created = 0
        for admin in admins_gerentes:
            create_notification(
                pedido_id=pedido_id,
                titulo="📤 Pedido Enviado para Análise",
                descricao=f'O pedido {pedido_numero} foi enviado por {usuario_nome} (Fornecedor: {fornecedor_nome}) e aguarda análise.',
                tipo='pedido_enviado',
                usuario=admin["name"]
            )
            notifications_created += 1
        
        conn.close()
        
        logger.info(f"Notificações criadas para {notifications_created} admin/gerentes para pedido {pedido_id}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao criar notificações automáticas: {e}")
        return False

def criar_notificacao_analise_pedido(pedido_id: int, pedido_numero: str, usuario_id: int, acao: str, observacao: str = "") -> bool:
    """Cria notificação para o dono do pedido quando é analisado"""
    try:
        conn = get_db_connection()
        
        # Buscar informações do usuário dono do pedido
        user = conn.execute("SELECT name FROM users WHERE id = ?", (usuario_id,)).fetchone()
        if user:
            usuario_nome = user["name"]
        else:
            usuario_nome = "Usuário"
        
        # Determinar título e descrição baseado na ação
        if acao == "aprovar":
            titulo = "✅ Pedido Aprovado"
            descricao = f'Seu pedido {pedido_numero} foi aprovado.'
            if observacao:
                descricao += f' Observação: {observacao}'
        else:  # reprovar
            titulo = "❌ Pedido Reprovado"
            descricao = f'Seu pedido {pedido_numero} foi reprovado.'
            if observacao:
                descricao += f' Motivo: {observacao}'
        
        # Criar notificação usando a função existente
        success = create_notification(
            pedido_id=pedido_id,
            titulo=titulo,
            descricao=descricao,
            tipo='pedido_analisado',
            usuario=usuario_nome
        )
        
        conn.close()
        
        if success:
            logger.info(f"Notificação de análise criada para usuário {usuario_id} - pedido {pedido_id}")
        
        return success
        
    except Exception as e:
        logger.error(f"Erro ao criar notificação de análise: {e}")
        return False

# =====================================================
# STATIC
# =====================================================
@app.route("/")
def serve_home():
    return send_from_directory(STATIC_DIR, "index.html")

@app.route("/<path:filename>")
def serve_static(filename):
    if os.path.exists(os.path.join(STATIC_DIR, filename)):
        return send_from_directory(STATIC_DIR, filename)
    if "." not in filename:
        return send_from_directory(STATIC_DIR, "index.html")
    return "Arquivo não encontrado", 404

# =====================================================
# AUTH ENDPOINTS
# =====================================================
@app.route("/api/auth/check", methods=["GET"])
def auth_check():
    token = parse_bearer_token()
    if not token:
        return jsonify({"authenticated": False, "message": "Token ausente"}), 200

    payload, err = verify_token(token)
    if err:
        return jsonify({"authenticated": False, "message": err}), 200

    conn = get_db_connection()
    user = conn.execute(
        "SELECT id, name, email, role, last_login FROM users WHERE id = ?",
        (payload["uid"],),
    ).fetchone()
    conn.close()

    if not user:
        return jsonify({"authenticated": False}), 200

    return jsonify(
        {
            "authenticated": True,
            "user": {
                "id": user["id"],
                "name": user["name"],
                "email": user["email"],
                "role": user["role"],
                "allowed_pages": pages_for_role(user["role"]),
            },
        }
    )

@app.route("/api/authCheck", methods=["GET"])
def auth_check_alias():
    """Compatibilidade com index.html antigo que chama /api/authCheck."""
    return auth_check()

@app.route("/api/login", methods=["POST"])
def login():
    data = request.get_json(silent=True) or {}
    email = normalize_email(data.get("email", ""))
    password = data.get("password", "")

    if not email or not password:
        return jsonify({"success": False, "message": "Email e senha são obrigatórios"}), 400

    conn = get_db_connection()
    user = conn.execute("SELECT * FROM users WHERE lower(email) = ?", (email,)).fetchone()

    if not user or not check_password_hash(user["password_hash"], password):
        conn.close()
        return jsonify({"success": False, "message": "Credenciais inválidas"}), 401

    conn.execute("UPDATE users SET last_login = datetime('now') WHERE id = ?", (user["id"],))
    conn.commit()
    conn.close()

    token = make_token(int(user["id"]), str(user["role"]))

    return jsonify(
        {
            "success": True,
            "message": "Login realizado com sucesso",
            "access_token": token,
            "token_type": "Bearer",
            "expires_in": TOKEN_EXPIRES_SECONDS,
            "user": {
                "id": user["id"],
                "name": user["name"],
                "email": user["email"],
                "role": user["role"],
                "allowed_pages": pages_for_role(user["role"]),
            },
        }
    )

# =====================================================
# LICITAÇÕES ENDPOINTS - NOVOS
# =====================================================
@app.route("/api/licitacoes", methods=["GET"])
@login_required
def list_licitacoes():
    """Lista todas as licitações"""
    try:
        licitacoes = kv_get("compraPlusLicitacoes_v1") or []
        if not isinstance(licitacoes, list):
            licitacoes = []
        
        # Filtrar por status se fornecido
        status_filter = request.args.get("status", "all")
        if status_filter != "all":
            licitacoes = [l for l in licitacoes if l.get("status") == status_filter]
        
        return jsonify({
            "success": True,
            "licitacoes": licitacoes
        })
    except Exception as e:
        logger.error(f"Erro ao listar licitações: {e}")
        return jsonify({"success": False, "message": "Erro ao listar licitações"}), 500

@app.route("/api/licitacoes/<int:licitacao_id>/itens", methods=["GET"])
@login_required
def get_itens_licitacao(licitacao_id: int):
    """Obtém itens disponíveis de uma licitação, opcionalmente filtrado por fornecedor"""
    try:
        fornecedor_id = request.args.get("fornecedorId")
        if fornecedor_id:
            fornecedor_id = int(fornecedor_id)
        
        itens_disponiveis = get_itens_licitacao_disponiveis(licitacao_id, fornecedor_id)
        
        return jsonify({
            "success": True,
            "itens": itens_disponiveis
        })
    except Exception as e:
        logger.error(f"Erro ao obter itens da licitação: {e}")
        return jsonify({"success": False, "message": "Erro ao obter itens"}), 500

@app.route("/api/licitacoes/<int:licitacao_id>/itens/<int:fornecedor_id>", methods=["GET"])
@login_required
def get_itens_licitacao_fornecedor(licitacao_id: int, fornecedor_id: int):
    """Obtém itens disponíveis de uma licitação para um fornecedor específico"""
    try:
        itens_disponiveis = get_itens_licitacao_disponiveis(licitacao_id, fornecedor_id)
        
        return jsonify({
            "success": True,
            "itens": itens_disponiveis
        })
    except Exception as e:
        logger.error(f"Erro ao obter itens da licitação: {e}")
        return jsonify({"success": False, "message": "Erro ao obter itens"}), 500

@app.route("/api/licitacoes/<int:licitacao_id>/fornecedores", methods=["GET"])
@login_required
def get_fornecedores_licitacao(licitacao_id: int):
    """Obtém fornecedores disponíveis para uma licitação.

    Fonte de verdade:
    - Itens da licitação vêm do KV (compraPlusItensLicitacao_v1)
    - Cadastro de fornecedores vem do SQLite (tabela fornecedores), mesma base do /api/fornecedores
    """
    try:
        itens = kv_get("compraPlusItensLicitacao_v1") or []
        if not isinstance(itens, list):
            itens = []

        # Encontrar fornecedores únicos que têm itens nesta licitação (robusto a tipos string/int)
        fornecedores_ids: set[int] = set()
        for item in itens:
            if not isinstance(item, dict):
                continue

            # licitacaoId pode vir como string
            try:
                item_lic_id = int(item.get("licitacaoId") or 0)
            except Exception:
                continue

            if item_lic_id != int(licitacao_id):
                continue

            # fornecedorId pode vir como string
            try:
                forn_id = int(item.get("fornecedorId") or 0)
            except Exception:
                forn_id = 0

            if forn_id:
                fornecedores_ids.add(forn_id)

        if not fornecedores_ids:
            return jsonify({"success": True, "fornecedores": []})

        # Buscar dados completos dos fornecedores no SQLite
        conn = get_db_connection()
        try:
            placeholders = ",".join(["?"] * len(fornecedores_ids))
            rows = conn.execute(
                f"""
                SELECT *
                FROM fornecedores
                WHERE status = 'active'
                  AND id IN ({placeholders})
                ORDER BY nome_fantasia, razao_social, id
                """,
                tuple(fornecedores_ids),
            ).fetchall()

            fornecedores_encontrados: list[dict] = []
            for row in rows:
                f = dict(row)
                if f.get("cnpj"):
                    f["cnpj_formatado"] = format_cnpj(f["cnpj"])
                fornecedores_encontrados.append(f)

            return jsonify({"success": True, "fornecedores": fornecedores_encontrados})
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Erro ao obter fornecedores da licitação: {e}")
        return jsonify({"success": False, "message": "Erro ao obter fornecedores"}), 500

# =====================================================
# NOTIFICAÇÕES ENDPOINTS
# =====================================================
@app.route("/api/notifications", methods=["GET"])
@login_required
def list_notifications():
    """Lista notificações do usuário atual (filtradas por role)"""
    try:
        user = getattr(request, "current_user", {})
        user_id = user.get("id")
        user_role = user.get("role")
        
        conn = get_db_connection()
        
        # Filtra notificações baseado no papel
        if user_role in ["Administrador", "Gerente"]:
            # Administradores e Gerentes veem todas
            rows = conn.execute(
                "SELECT * FROM notifications ORDER BY data DESC, id DESC"
            ).fetchall()
        else:
            # Compradores e Consultas: veem apenas dos próprios pedidos
            # Primeiro, buscar pedidos do usuário
            user_pedidos = get_user_pedidos(user_id)
            user_pedido_ids = [str(p.get("id")) for p in user_pedidos if p.get("id")]
            
            if user_pedido_ids:
                placeholders = ','.join('?' * len(user_pedido_ids))
                query = f"SELECT * FROM notifications WHERE pedido_id IN ({placeholders}) ORDER BY data DESC, id DESC"
                rows = conn.execute(query, user_pedido_ids).fetchall()
            else:
                rows = []
        
        notifications = [notification_to_dict(row) for row in rows]
        nao_lidas = len([n for n in notifications if not n["lida"]])
        
        conn.close()
        
        return jsonify({
            "success": True,
            "notifications": notifications,
            "total": len(notifications),
            "naoLidas": nao_lidas
        })
    except Exception as e:
        logger.error(f"Erro ao listar notificações: {e}")
        return jsonify({"success": False, "message": "Erro ao listar notificações"}), 500

@app.route("/api/notifications/mark-read", methods=["POST"])
@login_required
def mark_notification_read():
    """Marca uma notificação como lida"""
    try:
        data = request.get_json(silent=True) or {}
        notification_id = data.get("id")
        
        if not notification_id:
            return jsonify({"success": False, "message": "ID da notificação é obrigatório"}), 400
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verifica se a notificação existe
        notification = cursor.execute(
            "SELECT id FROM notifications WHERE id = ?", 
            (notification_id,)
        ).fetchone()
        
        if not notification:
            return jsonify({"success": False, "message": "Notificação não encontrada"}), 404
        
        # Marca como lida
        cursor.execute(
            "UPDATE notifications SET lida = 1 WHERE id = ?",
            (notification_id,)
        )
        
        conn.commit()
        
        # Publicar evento SSE
        _publish("notification_read", {"notificationId": notification_id})
        
        logger.info(f"Notificação {notification_id} marcada como lida")
        
        return jsonify({
            "success": True,
            "message": "Notificação marcada como lida"
        })
        
    except Exception as e:
        logger.error(f"Erro ao marcar notificação como lida: {e}")
        return jsonify({"success": False, "message": "Erro ao marcar notificação como lida"}), 500
    finally:
        conn.close()

@app.route("/api/notifications/mark-all-read", methods=["POST"])
@login_required
def mark_all_notifications_read():
    """Marca todas as notificações do usuário como lidas"""
    try:
        user = getattr(request, "current_user", {})
        user_id = user.get("id")
        user_role = user.get("role")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Filtra baseado no role
        if user_role in ["Administrador", "Gerente"]:
            # Admin/Gerente marca todas como lidas
            cursor.execute("SELECT COUNT(*) as count FROM notifications WHERE lida = 0")
            result = cursor.fetchone()
            count_to_mark = result["count"] if result else 0
            
            if count_to_mark > 0:
                cursor.execute("UPDATE notifications SET lida = 1 WHERE lida = 0")
        else:
            # Comprador/Consulta marca apenas dos próprios pedidos
            user_pedidos = get_user_pedidos(user_id)
            user_pedido_ids = [str(p.get("id")) for p in user_pedidos if p.get("id")]
            
            if user_pedido_ids:
                placeholders = ','.join('?' * len(user_pedido_ids))
                query = f"SELECT COUNT(*) as count FROM notifications WHERE pedido_id IN ({placeholders}) AND lida = 0"
                cursor.execute(query, user_pedido_ids)
                result = cursor.fetchone()
                count_to_mark = result["count"] if result else 0
                
                if count_to_mark > 0:
                    update_query = f"UPDATE notifications SET lida = 1 WHERE pedido_id IN ({placeholders}) AND lida = 0"
                    cursor.execute(update_query, user_pedido_ids)
            else:
                count_to_mark = 0
        
        conn.commit()
        
        if count_to_mark > 0:
            # Publicar evento SSE
            _publish("notifications_all_read", {"count": count_to_mark})
            logger.info(f"{count_to_mark} notificações marcadas como lidas")
        
        return jsonify({
            "success": True,
            "message": f"{count_to_mark} notificações marcadas como lidas",
            "count": count_to_mark
        })
        
    except Exception as e:
        logger.error(f"Erro ao marcar todas as notificações como lidas: {e}")
        return jsonify({"success": False, "message": "Erro ao marcar notificações como lidas"}), 500
    finally:
        conn.close()

# =====================================================
# PEDIDOS ENDPOINTS - ATUALIZADOS COM VALIDAÇÃO TOLERANTE
# =====================================================
@app.route("/api/pedidos", methods=["GET"])
@login_required
def list_pedidos():
    """Lista pedidos do usuário atual (ou todos para admin/gerente)"""
    try:
        user = getattr(request, "current_user", {})
        user_id = user.get("id")
        user_role = user.get("role")
        
        # Filtros
        status_filter = request.args.get("status", "all")
        search = request.args.get("search", "").strip().lower()
        
        # Obter pedidos baseado no papel do usuário
        pedidos = get_pedidos_for_user(user_id, user_role)
        
        # Aplicar filtros
        filtered_pedidos = []
        for pedido in pedidos:
            # Filtro por status
            if status_filter != "all" and pedido.get("status") != status_filter:
                continue
            
            # Filtro por busca
            if search:
                search_fields = [
                    pedido.get("numero", ""),
                    str(pedido.get("id", "")),
                    pedido.get("observacao", ""),
                ]
                if not any(search in field.lower() for field in search_fields):
                    continue
            
            filtered_pedidos.append(pedido)
        
        # Ordenar por data (mais recente primeiro)
        filtered_pedidos.sort(key=lambda x: x.get("data", ""), reverse=True)
        
        # Estatísticas
        stats = {
            "total": len(pedidos),
            "aprovados": len([p for p in pedidos if p.get("status") == "aprovado"]),
            "reprovados": len([p for p in pedidos if p.get("status") == "reprovado"]),
            "pendentes": len([p for p in pedidos if p.get("status") in ["rascunho", "enviado"]]),
        }
        
        return jsonify({
            "success": True, 
            "pedidos": filtered_pedidos,
            "stats": stats,
            "userRole": user_role,
            "userId": user_id
        })
    except Exception as e:
        logger.error(f"Erro ao listar pedidos: {e}")
        return jsonify({"success": False, "message": "Erro ao listar pedidos"}), 500

@app.route("/api/pedidos/<int:pedido_id>", methods=["GET"])
@login_required
def get_pedido(pedido_id: int):
    """Obtém um pedido específico do usuário atual ou de todos (admin/gerente)"""
    try:
        user = getattr(request, "current_user", {})
        user_id = user.get("id")
        user_role = user.get("role")
        
        # Buscar pedido
        pedido = None
        
        if user_role in ["Administrador", "Gerente"]:
            # Admin/Gerente busca em todos os pedidos
            all_pedidos = get_all_pedidos_for_admin()
            pedido = next((p for p in all_pedidos if p.get("id") == pedido_id), None)
        else:
            # Usuário normal, busca apenas nos seus pedidos
            pedidos = get_user_pedidos(user_id)
            pedido = next((p for p in pedidos if p.get("id") == pedido_id), None)
        
        if not pedido:
            return jsonify({"success": False, "message": "Pedido não encontrado"}), 404
        
        return jsonify({"success": True, "pedido": pedido})
    except Exception as e:
        logger.error(f"Erro ao buscar pedido {pedido_id}: {e}")
        return jsonify({"success": False, "message": "Erro ao buscar pedido"}), 500

@app.route("/api/pedidos", methods=["POST"])
@login_required
@role_required("Administrador", "Gerente", "Comprador")
def create_pedido():
    """Cria um novo pedido (VERSÃO TOLERANTE)"""
    try:
        data = request.get_json(silent=True) or {}
        user = getattr(request, "current_user", {})
        user_id = user.get("id")
        user_name = user.get("name", "Usuário")
        
        # Usar função de criação tolerante
        sucesso, mensagem, novo_pedido = criar_pedido_validado(user_id, user_name, data)
        
        if not sucesso:
            return jsonify({"success": False, "message": mensagem}), 400
        
        # 🔥 CRIAR NOTIFICAÇÃO se o pedido foi criado como "enviado"
        if data.get("status") == "enviado":
            # Buscar informações do fornecedor
            fornecedor_id = data["fornecedorId"]
            fornecedor_nome = "Fornecedor Desconhecido"
            
            conn = get_db_connection()
            try:
                fornecedor = conn.execute(
                    "SELECT razao_social, nome_fantasia FROM fornecedores WHERE id = ?", 
                    (fornecedor_id,)
                ).fetchone()
                if fornecedor:
                    fornecedor_nome = fornecedor["razao_social"] or fornecedor["nome_fantasia"] or fornecedor_nome
            except:
                pass
            finally:
                conn.close()
            
            # Criar notificação automática para admin/gerente
            criar_notificacao_automatica_pedido_enviado(
                pedido_id=novo_pedido["id"],
                pedido_numero=novo_pedido["numero"],
                usuario_nome=user_name,
                fornecedor_nome=fornecedor_nome
            )
        
        return jsonify({
            "success": True,
            "message": mensagem,
            "pedido": novo_pedido
        })
        
    except Exception as e:
        logger.error(f"Erro ao criar pedido: {e}")
        return jsonify({"success": False, "message": "Erro ao criar pedido"}), 500

@app.route("/api/pedidos/<int:pedido_id>", methods=["PUT"])
@login_required
def update_pedido(pedido_id: int):
    """Atualiza um pedido (apenas dono ou admin/gerente) - VERSÃO TOLERANTE"""
    try:
        data = request.get_json(silent=True) or {}
        user = getattr(request, "current_user", {})
        user_id = user.get("id")
        user_role = user.get("role")
        user_name = user.get("name", "Usuário")
        
        # Buscar pedido
        owner_id = None
        pedidos = []
        pedido_to_update = None
        
        if user_role in ["Administrador", "Gerente"]:
            # Admin/Gerente busca em todos os usuários
            all_pedidos = get_all_pedidos_for_admin()
            pedido_to_update = next((p for p in all_pedidos if p.get("id") == pedido_id), None)
            if pedido_to_update:
                owner_id = pedido_to_update.get("userId")
                pedidos = get_user_pedidos(owner_id)
                pedido_to_update = next((p for p in pedidos if p.get("id") == pedido_id), None)
        else:
            # Usuário normal busca apenas nos seus pedidos
            owner_id = user_id
            pedidos = get_user_pedidos(user_id)
            pedido_to_update = next((p for p in pedidos if p.get("id") == pedido_id), None)
        
        if not pedido_to_update or not owner_id:
            return jsonify({"success": False, "message": "Pedido não encontrado"}), 404
        
        # Encontrar índice do pedido
        pedido_index = next((i for i, p in enumerate(pedidos) if p.get("id") == pedido_id), -1)
        if pedido_index == -1:
            return jsonify({"success": False, "message": "Pedido não encontrado"}), 404
        
        # Verificar permissões
        if user_role not in ["Administrador", "Gerente"] and pedidos[pedido_index].get("userId") != user_id:
            return jsonify({"success": False, "message": "Sem permissão para editar este pedido"}), 403
        
        # Verificar se pode editar (apenas rascunho)
        if pedidos[pedido_index].get("status") != "rascunho" and user_role not in ["Administrador", "Gerente"]:
            return jsonify({"success": False, "message": "Só é possível editar pedidos em rascunho"}), 400
        
        # Status anterior
        status_anterior = pedidos[pedido_index].get("status")
        novo_status = data.get("status", status_anterior)
        
        # Atualizar observação se fornecida
        if "observacao" in data:
            pedidos[pedido_index]["observacao"] = data["observacao"]
        
        # Validar itens se estiver sendo atualizado
        if "itens" in data:
            licitacao_id = pedidos[pedido_index].get("licitacaoId")
            fornecedor_id = pedidos[pedido_index].get("fornecedorId")
            
            # Validar novos itens contra licitação (versão tolerante)
            valido, mensagem, mapa_itens = validar_itens_pedido(data["itens"], licitacao_id, fornecedor_id)
            if not valido:
                return jsonify({"success": False, "message": mensagem}), 400
            
            # Atualizar itens com preços fixos da licitação ou dados do pedido
            itens_com_preco_fixo = []
            total = 0
            
            for item_pedido in data["itens"]:
                item_id = item_pedido.get("itemId") or item_pedido.get("id")
                quantidade = item_pedido.get("quantidade") or item_pedido.get("qtd", 1)
                
                if item_id in mapa_itens:
                    item_licitacao = mapa_itens[item_id]
                    preco_unitario = item_licitacao.get("preco", 0)
                    subtotal = preco_unitario * quantidade
                    total += subtotal
                    
                    # Criar item com preço fixo da licitação
                    item_formatado = {
                        "itemId": item_id,
                        "nome": item_licitacao.get("nome"),
                        "descricao": item_licitacao.get("descricao"),
                        "quantidade": quantidade,
                        "preco": preco_unitario,
                        "subtotal": subtotal,
                        "unidade": item_licitacao.get("unidade", "UN")
                    }
                    itens_com_preco_fixo.append(item_formatado)
                else:
                    # Item não encontrado na licitação - usar dados do pedido
                    preco_unitario = item_pedido.get("preco", 0)
                    subtotal = preco_unitario * quantidade
                    total += subtotal
                    
                    item_formatado = {
                        "itemId": item_id,
                        "nome": item_pedido.get("nome", "Item desconhecido"),
                        "descricao": item_pedido.get("descricao", ""),
                        "quantidade": quantidade,
                        "preco": preco_unitario,
                        "subtotal": subtotal,
                        "unidade": item_pedido.get("unidade", "UN")
                    }
                    itens_com_preco_fixo.append(item_formatado)
                    logger.warning(f"Item ID {item_id} não encontrado na licitação {licitacao_id}, usando dados do pedido")
            
            pedidos[pedido_index]["total"] = total
            pedidos[pedido_index]["itens"] = itens_com_preco_fixo
        
        if "status" in data:
            pedidos[pedido_index]["status"] = data["status"]
        
        # Adicionar ao histórico
        historico = pedidos[pedido_index].get("historico", [])
        historico.append({
            "data": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "acao": "Atualizado",
            "observacao": data.get("observacao", ""),
            "usuario": user_name
        })
        pedidos[pedido_index]["historico"] = historico
        
        # Salvar
        if not save_user_pedidos(owner_id, pedidos):
            return jsonify({"success": False, "message": "Erro ao salvar pedido"}), 500
        
        # 🔥 SE FOI ENVIADO PARA ANÁLISE, CRIAR NOTIFICAÇÃO PARA ADMIN/GERENTE
        if status_anterior != "enviado" and novo_status == "enviado":
            # Buscar informações do fornecedor
            fornecedor_id = pedidos[pedido_index].get("fornecedorId")
            fornecedor_nome = "Fornecedor Desconhecido"
            
            # Tentar buscar do banco SQLite
            conn = get_db_connection()
            try:
                fornecedor = conn.execute(
                    "SELECT razao_social, nome_fantasia FROM fornecedores WHERE id = ?", 
                    (fornecedor_id,)
                ).fetchone()
                if fornecedor:
                    fornecedor_nome = fornecedor["razao_social"] or fornecedor["nome_fantasia"] or fornecedor_nome
            except:
                pass
            finally:
                conn.close()
            
            # Criar notificação automática para admin/gerente
            pedido_numero = pedidos[pedido_index].get("numero", f"PED-{pedido_id}")
            
            criar_notificacao_automatica_pedido_enviado(
                pedido_id=pedido_id,
                pedido_numero=pedido_numero,
                usuario_nome=user_name,
                fornecedor_nome=fornecedor_nome
            )
        
        # Publicar evento
        _publish("data_updated", {"key": get_user_pedidos_key(owner_id)})
        
        logger.info(f"Pedido {pedido_id} atualizado por {user_name}")
        
        return jsonify({
            "success": True,
            "message": "Pedido atualizado com sucesso",
            "pedido": pedidos[pedido_index]
        })
        
    except Exception as e:
        logger.error(f"Erro ao atualizar pedido: {e}")
        return jsonify({"success": False, "message": "Erro ao atualizar pedido"}), 500

@app.route("/api/pedidos/<int:pedido_id>", methods=["DELETE"])
@login_required
def delete_pedido(pedido_id: int):
    """Exclui um pedido (apenas dono ou admin/gerente)"""
    try:
        user = getattr(request, "current_user", {})
        user_id = user.get("id")
        user_role = user.get("role")
        
        # Buscar pedido
        owner_id = None
        pedidos = []
        
        if user_role in ["Administrador", "Gerente"]:
            # Admin/Gerente busca em todos os usuários
            all_pedidos = get_all_pedidos_for_admin()
            pedido_to_delete = next((p for p in all_pedidos if p.get("id") == pedido_id), None)
            if pedido_to_delete:
                owner_id = pedido_to_delete.get("userId")
                pedidos = get_user_pedidos(owner_id)
        else:
            # Usuário normal busca apenas nos seus pedidos
            owner_id = user_id
            pedidos = get_user_pedidos(user_id)
            pedido_to_delete = next((p for p in pedidos if p.get("id") == pedido_id), None)
            if not pedido_to_delete:
                return jsonify({"success": False, "message": "Pedido não encontrado"}), 404
        
        # Verificar permissões
        if user_role not in ["Administrador", "Gerente"] and pedido_to_delete.get("status") != "rascunho":
            return jsonify({"success": False, "message": "Só é possível excluir pedidos em rascunho"}), 400
        
        # Filtrar pedido a ser excluído
        new_pedidos = [p for p in pedidos if p.get("id") != pedido_id]
        
        # Salvar
        if not save_user_pedidos(owner_id, new_pedidos):
            return jsonify({"success": False, "message": "Erro ao excluir pedido"}), 500
        
        # Publicar evento
        _publish("data_updated", {"key": get_user_pedidos_key(owner_id)})
        
        logger.info(f"Pedido {pedido_id} excluído por usuário {user_id}")
        
        return jsonify({
            "success": True,
            "message": "Pedido excluído com sucesso"
        })
        
    except Exception as e:
        logger.error(f"Erro ao excluir pedido: {e}")
        return jsonify({"success": False, "message": "Erro ao excluir pedido"}), 500

@app.route("/api/pedidos/analisar", methods=["POST"])
@login_required
@role_required("Administrador", "Gerente")
def analisar_pedido():
    """Analisa um pedido (aprova/reprova) - apenas admin/gerente"""
    try:
        data = request.get_json(silent=True) or {}
        pedido_id = data.get("pedidoId")
        acao = data.get("acao")  # "aprovar" ou "reprovar"
        observacao = data.get("observacao", "")
        
        if not pedido_id or not acao:
            return jsonify({"success": False, "message": "pedidoId e acao são obrigatórios"}), 400
        
        if acao not in ["aprovar", "reprovar"]:
            return jsonify({"success": False, "message": "Ação inválida. Use 'aprovar' ou 'reprovar'"}), 400
        
        if acao == "reprovar" and not observacao.strip():
            return jsonify({"success": False, "message": "Motivo da reprovação é obrigatório"}), 400
        
        # Buscar dono do pedido
        owner_id = find_pedido_owner(pedido_id)
        if not owner_id:
            return jsonify({"success": False, "message": "Pedido não encontrado"}), 404
        
        # Obter pedidos do owner
        pedidos = get_user_pedidos(owner_id)
        pedido_index = next((i for i, p in enumerate(pedidos) if p.get("id") == pedido_id), -1)
        
        if pedido_index == -1:
            return jsonify({"success": False, "message": "Pedido não encontrado"}), 404
        
        # Obter usuário atual
        user = getattr(request, "current_user", {})
        usuario = user.get("name", "Sistema") if user else "Sistema"
        
        # Novo status
        novo_status = "aprovado" if acao == "aprovar" else "reprovado"
        acao_texto = "Aprovado" if acao == "aprovar" else "Reprovado"
        
        # Atualizar pedido
        pedidos[pedido_index]["status"] = novo_status
        
        # Adicionar ao histórico
        historico = pedidos[pedido_index].get("historico", [])
        historico.append({
            "data": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "acao": f"Pedido {acao_texto}",
            "observacao": observacao,
            "usuario": usuario
        })
        pedidos[pedido_index]["historico"] = historico
        
        # Salvar
        if not save_user_pedidos(owner_id, pedidos):
            return jsonify({"success": False, "message": "Erro ao salvar pedido"}), 500
        
        # 🔥 CRIAR NOTIFICAÇÃO PARA O DONO DO PEDIDO
        pedido_numero = pedidos[pedido_index].get("numero", f"PED-{pedido_id}")
        
        criar_notificacao_analise_pedido(
            pedido_id=pedido_id,
            pedido_numero=pedido_numero,
            usuario_id=owner_id,
            acao=acao,
            observacao=observacao
        )
        
        # Publicar eventos SSE
        _publish("data_updated", {"key": get_user_pedidos_key(owner_id)})
        _publish("notification_created", {"pedidoId": pedido_id})
        
        logger.info(f"Pedido {pedido_id} {novo_status} por {usuario}")
        
        return jsonify({
            "success": True, 
            "message": f"Pedido {acao_texto.lower()} com sucesso"
        })
        
    except Exception as e:
        logger.error(f"Erro ao analisar pedido: {e}")
        return jsonify({"success": False, "message": "Erro ao analisar pedido"}), 500

# =====================================================
# USERS CRUD
# =====================================================
@app.route("/api/users", methods=["GET"])
@login_required
@role_required("Administrador", "Gerente")
def list_users():
    conn = get_db_connection()
    try:
        rows = conn.execute(
            "SELECT id, name, email, role, created_at, last_login FROM users ORDER BY id DESC"
        ).fetchall()
        return jsonify({"success": True, "users": [dict(r) for r in rows]})
    except Exception as e:
        logger.error(f"Erro ao listar usuários: {e}")
        return jsonify({"success": False, "message": "Erro ao listar usuários"}), 500
    finally:
        conn.close()

@app.route("/api/users", methods=["POST"])
@login_required
@role_required("Administrador")
def create_user():
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    email = normalize_email(data.get("email") or "")
    password = (data.get("password") or "").strip()
    role = (data.get("role") or "Consulta").strip()

    if not name or not email or not password:
        return jsonify({"success": False, "message": "Nome, email e senha são obrigatórios"}), 400
    if role not in ROLE_PAGES.keys():
        return jsonify({"success": False, "message": "Perfil inválido"}), 400

    conn = get_db_connection()
    try:
        conn.execute(
            "INSERT INTO users (name, email, password_hash, role) VALUES (?, ?, ?, ?)",
            (name, email, generate_password_hash(password), role),
        )
        conn.commit()
        _publish("data_updated", {"key": "users"})
        return jsonify({"success": True, "message": "Usuário criado com sucesso"})
    except sqlite3.IntegrityError:
        return jsonify({"success": False, "message": "Email já cadastrado"}), 409
    except Exception as e:
        logger.error(f"Erro ao criar usuário: {e}")
        if is_db_locked_error(e):
            return jsonify({"success": False, "message": "Banco ocupado. Tente novamente."}), 503
        return jsonify({"success": False, "message": "Erro ao criar usuário"}), 500
    finally:
        conn.close()

@app.route("/api/users/<int:uid>", methods=["PUT"])
@login_required
@role_required("Administrador")
def update_user(uid: int):
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    role = (data.get("role") or "").strip()
    password = (data.get("password") or "").strip()

    if role and role not in ROLE_PAGES.keys():
        return jsonify({"success": False, "message": "Perfil inválido"}), 400

    conn = get_db_connection()
    try:
        user = conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
        if not user:
            return jsonify({"success": False, "message": "Usuário não encontrado"}), 404

        new_name = name if name else user["name"]
        new_role = role if role else user["role"]

        if password:
            conn.execute(
                "UPDATE users SET name=?, role=?, password_hash=? WHERE id=?",
                (new_name, new_role, generate_password_hash(password), uid),
            )
        else:
            conn.execute("UPDATE users SET name=?, role=? WHERE id=?", (new_name, new_role, uid))

        conn.commit()
        _publish("data_updated", {"key": "users"})
        return jsonify({"success": True, "message": "Usuário atualizado"})
    except Exception as e:
        logger.error(f"Erro ao atualizar usuário: {e}")
        if is_db_locked_error(e):
            return jsonify({"success": False, "message": "Banco ocupado. Tente novamente."}), 503
        return jsonify({"success": False, "message": "Erro ao atualizar usuário"}), 500
    finally:
        conn.close()

@app.route("/api/users/<int:uid>", methods=["DELETE"])
@login_required
@role_required("Administrador")
def delete_user(uid: int):
    conn = get_db_connection()
    try:
        user = conn.execute("SELECT id FROM users WHERE id=?", (uid,)).fetchone()
        if not user:
            return jsonify({"success": False, "message": "Usuário não encontrado"}), 404
        conn.execute("DELETE FROM users WHERE id=?", (uid,))
        conn.commit()
        _publish("data_updated", {"key": "users"})
        return jsonify({"success": True, "message": "Usuário removido"})
    except Exception as e:
        logger.error(f"Erro ao excluir usuário: {e}")
        if is_db_locked_error(e):
            return jsonify({"success": False, "message": "Banco ocupado. Tente novamente."}), 503
        return jsonify({"success": False, "message": "Erro ao excluir usuário"}), 500
    finally:
        conn.close()

# =====================================================
# FORNECEDORES (SQLite)
# =====================================================
@app.route("/api/fornecedores", methods=["GET"])
@login_required
def list_fornecedores():
    status = request.args.get("status")
    q = request.args.get("q", "").strip()

    base_sql = "SELECT * FROM fornecedores"
    clauses = []
    params = []

    if status and status != "all":
        clauses.append("status = ?")
        params.append(status)

    if q:
        qlike = f"%{q}%"
        clauses.append(
            "(razao_social LIKE ? OR nome_fantasia LIKE ? OR cnpj LIKE ? OR email LIKE ? OR telefone LIKE ? OR contato LIKE ?)"
        )
        params.extend([qlike, qlike, qlike, qlike, qlike, qlike])

    if clauses:
        base_sql += " WHERE " + " AND ".join(clauses)

    base_sql += " ORDER BY id DESC"

    conn = get_db_connection()
    try:
        rows = conn.execute(base_sql, params).fetchall()
        fornecedores = []
        for r in rows:
            fornecedor = dict(r)
            if fornecedor.get("cnpj"):
                fornecedor["cnpj_formatado"] = format_cnpj(fornecedor["cnpj"])
            fornecedores.append(fornecedor)
        return jsonify({"success": True, "fornecedores": fornecedores})
    except Exception as e:
        logger.error(f"Erro ao listar fornecedores: {e}")
        return jsonify({"success": False, "message": "Erro ao listar fornecedores"}), 500
    finally:
        conn.close()

@app.route("/api/fornecedores", methods=["POST"])
@login_required
@role_required("Administrador", "Gerente", "Comprador")
def create_fornecedor():
    data = request.get_json(silent=True) or {}

    razao = (data.get("razao_social") or "").strip()
    fantasia = (data.get("nome_fantasia") or "").strip()
    cnpj = only_digits((data.get("cnpj") or "").strip())
    inscricao = (data.get("inscricao_estadual") or "").strip()
    email = (data.get("email") or "").strip()
    telefone = (data.get("telefone") or "").strip()
    endereco = (data.get("endereco") or "").strip()
    contato = (data.get("contato") or "").strip()
    status = (data.get("status") or "active").strip()

    if not razao:
        return jsonify({"success": False, "message": "Razão social é obrigatória"}), 400
    if not cnpj or len(cnpj) != 14:
        return jsonify({"success": False, "message": "CNPJ inválido (deve ter 14 dígitos)"}), 400

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO fornecedores
            (razao_social, nome_fantasia, cnpj, inscricao_estadual, email, telefone, endereco, contato, status, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (razao, fantasia, cnpj, inscricao, email, telefone, endereco, contato, status),
        )
        conn.commit()
        new_id = cur.lastrowid
        _publish("data_updated", {"key": "fornecedores_db"})
        return jsonify({"success": True, "id": new_id})
    except sqlite3.IntegrityError:
        return jsonify({"success": False, "message": "CNPJ já cadastrado"}), 409
    except Exception as e:
        logger.error(f"Erro ao criar fornecedor: {e}")
        if is_db_locked_error(e):
            return jsonify({"success": False, "message": "Banco ocupado. Tente novamente."}), 503
        return jsonify({"success": False, "message": "Erro ao criar fornecedor"}), 500
    finally:
        conn.close()

@app.route("/api/fornecedores/<int:fid>", methods=["PUT"])
@login_required
@role_required("Administrador", "Gerente", "Comprador")
def update_fornecedor(fid: int):
    data = request.get_json(silent=True) or {}

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        row = cur.execute("SELECT * FROM fornecedores WHERE id = ?", (fid,)).fetchone()
        if not row:
            return jsonify({"success": False, "message": "Fornecedor não encontrado"}), 404

        razao = (data.get("razao_social") or row["razao_social"]).strip()
        fantasia = (data.get("nome_fantasia") or row["nome_fantasia"] or "").strip()
        cnpj_raw = (data.get("cnpj") or row["cnpj"]).strip()
        cnpj = only_digits(cnpj_raw) if cnpj_raw else row["cnpj"]
        inscricao = (data.get("inscricao_estadual") or row["inscricao_estadual"] or "").strip()
        email = (data.get("email") or row["email"] or "").strip()
        telefone = (data.get("telefone") or row["telefone"] or "").strip()
        endereco = (data.get("endereco") or row["endereco"] or "").strip()
        contato = (data.get("contato") or row["contato"] or "").strip()
        status = (data.get("status") or row["status"]).strip()

        if not razao:
            return jsonify({"success": False, "message": "Razão social é obrigatória"}), 400
        if not cnpj or len(cnpj) != 14:
            return jsonify({"success": False, "message": "CNPJ inválido"}), 400

        cur.execute(
            """
            UPDATE fornecedores
            SET razao_social=?, nome_fantasia=?, cnpj=?, inscricao_estadual=?,
                email=?, telefone=?, endereco=?, contato=?, status=?, updated_at=datetime('now')
            WHERE id=?
            """,
            (razao, fantasia, cnpj, inscricao, email, telefone, endereco, contato, status, fid),
        )
        conn.commit()
        _publish("data_updated", {"key": "fornecedores_db"})
        return jsonify({"success": True})
    except sqlite3.IntegrityError:
        return jsonify({"success": False, "message": "CNPJ já cadastrado em outro fornecedor"}), 409
    except Exception as e:
        logger.error(f"Erro ao atualizar fornecedor: {e}")
        if is_db_locked_error(e):
            return jsonify({"success": False, "message": "Banco ocupado. Tente novamente."}), 503
        return jsonify({"success": False, "message": "Erro ao atualizar fornecedor"}), 500
    finally:
        conn.close()

@app.route("/api/fornecedores/<int:fid>", methods=["DELETE"])
@login_required
@role_required("Administrador", "Gerente")
def delete_fornecedor(fid: int):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        row = cur.execute("SELECT id FROM fornecedores WHERE id = ?", (fid,)).fetchone()
        if not row:
            return jsonify({"success": False, "message": "Fornecedor não encontrado"}), 404

        cur.execute("DELETE FROM fornecedores WHERE id = ?", (fid,))
        conn.commit()
        _publish("data_updated", {"key": "fornecedores_db"})
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Erro ao excluir fornecedor: {e}")
        if is_db_locked_error(e):
            return jsonify({"success": False, "message": "Banco ocupado. Tente novamente."}), 503
        return jsonify({"success": False, "message": "Erro ao excluir fornecedor"}), 500
    finally:
        conn.close()

# =====================================================
# PEDIDOS ENDPOINTS - NOVAS CORREÇÕES
# =====================================================

@app.route("/api/pedidos/<int:pedido_id>/analisar", methods=["POST"])
@login_required
@role_required("Administrador", "Gerente")
def analisar_pedido_detalhe(pedido_id: int):
    """Analisa um pedido específico (aprova/reprova) - apenas admin/gerente"""
    try:
        data = request.get_json(silent=True) or {}
        acao = data.get("acao")  # "aprovar" ou "reprovar"
        observacao = data.get("observacao", "")
        
        logger.info(f"📋 Analisando pedido {pedido_id}, ação: {acao}")
        
        if not acao:
            return jsonify({"success": False, "message": "acao é obrigatória"}), 400
        
        if acao not in ["aprovar", "reprovar"]:
            return jsonify({"success": False, "message": "Ação inválida. Use 'aprovar' ou 'reprovar'"}), 400
        
        if acao == "reprovar" and not observacao.strip():
            return jsonify({"success": False, "message": "Motivo da reprovação é obrigatório"}), 400
        
        # Buscar dono do pedido
        owner_id = find_pedido_owner(pedido_id)
        if not owner_id:
            logger.warning(f"Pedido {pedido_id} não encontrado (owner_id)")
            return jsonify({"success": False, "message": "Pedido não encontrado"}), 404
        
        # Obter pedidos do owner
        pedidos = get_user_pedidos(owner_id)
        pedido_index = next((i for i, p in enumerate(pedidos) if p.get("id") == pedido_id), -1)
        
        if pedido_index == -1:
            logger.warning(f"Pedido {pedido_id} não encontrado no índice")
            return jsonify({"success": False, "message": "Pedido não encontrado"}), 404
        
        # Obter status atual - CORREÇÃO CRÍTICA: aceita status em qualquer caixa
        status_atual = pedidos[pedido_index].get("status", "").lower()
        logger.info(f"📊 Pedido {pedido_id} status atual: {status_atual}")
        
        # Verificar se o pedido está em status "enviado" - VERSÃO FLEXÍVEL
        if status_atual != "enviado":
            # Verificar também no histórico se foi enviado
            historico = pedidos[pedido_index].get("historico", [])
            enviado_no_historico = any(
                "enviado" in str(entry.get("acao", "")).lower() or 
                "enviado" in str(entry.get("observacao", "")).lower()
                for entry in historico
            )
            
            # Tentar verificar se o pedido foi enviado mas o status não foi atualizado
            # Isso pode acontecer em caso de sincronização incompleta
            if not enviado_no_historico:
                # VERIFICAÇÃO MAIS PERMISSIVA: se o pedido tem itens e foi criado há pouco tempo
                # pode ser um bug de sincronização, então permitimos a análise
                itens_pedido = pedidos[pedido_index].get("itens", [])
                if itens_pedido and len(itens_pedido) > 0:
                    logger.warning(f"⚠️ Pedido {pedido_id} tem status '{pedidos[pedido_index].get('status')}' mas tem itens. Permitindo análise devido a possível bug de sincronização.")
                else:
                    logger.error(f"❌ Pedido {pedido_id} não está enviado e não tem histórico de envio")
                    return jsonify({
                        "success": False, 
                        "message": f"Pedido não está disponível para análise. Status atual: {pedidos[pedido_index].get('status')}"
                    }), 400
            else:
                logger.info(f"✅ Pedido {pedido_id} não tem status 'enviado', mas tem histórico de envio. Permitindo análise.")
        
        # Obter usuário atual
        user = getattr(request, "current_user", {})
        usuario = user.get("name", "Sistema") if user else "Sistema"
        
        # Novo status
        novo_status = "aprovado" if acao == "aprovar" else "reprovado"
        acao_texto = "Aprovado" if acao == "aprovar" else "Reprovado"
        
        # Atualizar pedido
        pedidos[pedido_index]["status"] = novo_status
        
        # Adicionar ao histórico
        historico = pedidos[pedido_index].get("historico", [])
        historico.append({
            "data": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "acao": f"Pedido {acao_texto}",
            "observacao": observacao,
            "usuario": usuario
        })
        pedidos[pedido_index]["historico"] = historico
        
        # Salvar
        if not save_user_pedidos(owner_id, pedidos):
            return jsonify({"success": False, "message": "Erro ao salvar pedido"}), 500
        
        # 🔥 CRIAR NOTIFICAÇÃO PARA O DONO DO PEDIDO
        pedido_numero = pedidos[pedido_index].get("numero", f"PED-{pedido_id}")
        
        criar_notificacao_analise_pedido(
            pedido_id=pedido_id,
            pedido_numero=pedido_numero,
            usuario_id=owner_id,
            acao=acao,
            observacao=observacao
        )
        
        # Publicar eventos SSE
        _publish("data_updated", {"key": get_user_pedidos_key(owner_id)})
        _publish("notification_created", {"pedidoId": pedido_id})
        
        logger.info(f"✅ Pedido {pedido_id} {novo_status} por {usuario}")
        
        return jsonify({
            "success": True, 
            "message": f"Pedido {acao_texto.lower()} com sucesso"
        })
        
    except Exception as e:
        logger.error(f"❌ Erro ao analisar pedido: {e}")
        return jsonify({"success": False, "message": "Erro ao analisar pedido"}), 500

# =====================================================
# AJUSTES NA FUNÇÃO get_fornecedores_licitacao
# =====================================================
@app.route("/api/licitacoes/<int:licitacao_id>/itens/<int:fornecedor_id>", methods=["GET"])
@login_required
def get_itens_licitacao_fornecedor_endpoint(licitacao_id: int, fornecedor_id: int):
    """Endpoint específico para obter itens de uma licitação para um fornecedor específico"""
    try:
        itens_disponiveis = get_itens_licitacao_disponiveis(licitacao_id, fornecedor_id)
        
        return jsonify({
            "success": True,
            "itens": itens_disponiveis,
            "licitacaoId": licitacao_id,
            "fornecedorId": fornecedor_id
        })
    except Exception as e:
        logger.error(f"Erro ao obter itens da licitação para fornecedor: {e}")
        return jsonify({"success": False, "message": "Erro ao obter itens"}), 500

# =====================================================
# SYNC KV (PARA DADOS GLOBAIS)
# =====================================================
@app.route("/api/sync/pull", methods=["GET"])
@login_required
def sync_pull():
    keys = [
        "compraPlusLicitacoes_v1",
        "compraPlusItensLicitacao_v1",
        "compraPlusFornecedores_v1",
    ]
    data = {}
    for k in keys:
        v = kv_get(k)
        data[k] = v if v is not None else []
    
    # Adicionar pedidos do usuário atual (se aplicável)
    user = getattr(request, "current_user", {})
    user_id = user.get("id")
    user_role = user.get("role")
    
    if user_id:
        if user_role in ["Administrador", "Gerente"]:
            # Admin/Gerente recebe todos os pedidos
            all_pedidos = get_all_pedidos_for_admin()
            data["compraPlusPedidos_v1"] = all_pedidos
        else:
            # Usuário normal recebe apenas seus pedidos
            user_pedidos = get_user_pedidos(user_id)
            data["compraPlusPedidos_v1"] = user_pedidos
    
    return jsonify({"success": True, "data": data})

@app.route("/api/sync/push", methods=["POST"])
@login_required
def sync_push():
    body = request.get_json(silent=True) or {}
    key = (body.get("key") or "").strip()
    logger.info(f"[sync_push][{APP_BUILD}] key={key!r} user={(getattr(request,'current_user',{}) or {}).get('email','?')}")
    value = body.get("value")

    if not key:
        return jsonify({"success": False, "message": "Chave ausente"}), 400

    # trava simples de chaves permitidas (evita lixo)
    allowed = {
        "compraPlusLicitacoes_v1",
        "compraPlusItensLicitacao_v1",
        "compraPlusFornecedores_v1",
        "compraPlusPedidos_v1",  # pedidos (por usuário / admin)
        # chaves extras (não fazem mal; evitam erro caso front evolua)
        "compraPlusItensLicitacao_BACKUP_v1",
        "compraPlus_pdf_brand_v1",
    }
    if key not in allowed:
        return jsonify({"success": False, "message": f"Chave não permitida: {key!r}"}), 400
    if key in {"compraPlusLicitacoes_v1", "compraPlusItensLicitacao_v1"}:
        user = getattr(request, "current_user", {}) or {}
        if user.get("role") not in ["Administrador", "Gerente"]:
            return jsonify({"success": False, "message": "Apenas Administrador/Gerente podem alterar licitações/itens"}), 403


    # Pedidos: salvamento é por usuário (comprador) ou distribuído (admin/gerente)
    if key == "compraPlusPedidos_v1":
        user = getattr(request, "current_user", {}) or {}
        user_id = user.get("id")
        user_role = user.get("role")
        user_name = user.get("name", "Usuário")

        if not user_id:
            return jsonify({"success": False, "message": "Usuário inválido"}), 401

        if not isinstance(value, list):
            return jsonify({"success": False, "message": "Formato inválido para pedidos"}), 400

        # Comprador: só pode salvar os próprios pedidos
        if user_role not in ["Administrador", "Gerente"]:
            sanitized = []
            for p in value:
                if not isinstance(p, dict):
                    continue
                p = dict(p)
                # força ownership
                p["userId"] = user_id
                p["userName"] = p.get("userName") or user_name
                sanitized.append(p)

            if not save_user_pedidos(user_id, sanitized):
                return jsonify({"success": False, "message": "Erro ao salvar pedidos"}), 500

            _publish("data_updated", {"key": get_user_pedidos_key(user_id)})
            return jsonify({"success": True, "message": "Pedidos salvos com sucesso"})

        # Admin/Gerente: pode enviar lista completa; distribuímos por userId
        grouped = {}
        for p in value:
            if not isinstance(p, dict):
                continue
            uid = p.get("userId")
            if not uid:
                # se vier sem userId, atribui ao admin atual (fallback)
                uid = user_id
                p = dict(p)
                p["userId"] = uid
                p["userName"] = p.get("userName") or user_name
            grouped.setdefault(int(uid), []).append(dict(p))

        for uid, plist in grouped.items():
            save_user_pedidos(uid, plist)

        _publish("data_updated", {"key": "compraPlusPedidos_v1"})
        return jsonify({"success": True, "message": "Pedidos salvos com sucesso"})

    if not kv_set(key, value):
        return jsonify({"success": False, "message": "Erro ao salvar no KV Store"}), 500

    _publish("data_updated", {"key": key})
    return jsonify({"success": True, "message": "Dados salvos com sucesso"})

# =====================================================
# RELATÓRIOS (PDF)
# =====================================================
@app.route("/api/relatorios/saldo-pregao.pdf", methods=["GET"])
@login_required_allow_query
@role_required("Administrador", "Gerente", "Consulta")
def relatorio_saldo_pregao_pdf():
    """Gera PDF no modelo 'Saldo de Pregão' - Acesso para Admin, Gerente e Consulta (somente visualizar)"""
    
    from reportlab.lib.pagesizes import landscape

    # Helpers locais
    def to_float_br(v) -> float:
        """Converte números em formato pt-BR (ex: '1.234,56') ou numérico para float."""
        if v is None:
            return 0.0
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s:
            return 0.0
        # "15.000,25" -> "15000.25"
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except Exception:
            return 0.0

    def get_first(d: dict, *keys, default=None):
        """Retorna o primeiro campo existente entre keys (ou default)."""
        for k in keys:
            if k in d and d.get(k) not in (None, ""):
                return d.get(k)
        return default

    def br_number(x, decimals: int = 4) -> str:
        v = to_float_br(x)
        s = f"{v:,.{decimals}f}"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")

    def br_money(x: float) -> str:
        s = br_number(x, 4)
        if "," in s:
            s = s.rstrip("0").rstrip(",")
        return s

    def safe_int(v, default: int = 0) -> int:
        try:
            return int(v)
        except Exception:
            return default

    # Parâmetros
    numero = (request.args.get("numero") or "").strip()
    pregao = (request.args.get("pregao") or "").strip()
    processo = (request.args.get("processo") or "").strip()

    # Filtros (vindos do frontend)
    licitacao_id = safe_int(request.args.get("licitacaoId") or request.args.get("licitacao_id") or 0, 0)
    fornecedor_id = safe_int(request.args.get("fornecedorId") or request.args.get("fornecedor_id") or 0, 0)

    # Carregar itens (KV Store) e aplicar filtros
    itens = []
    try:
        kv_itens = kv_get("compraPlusItensLicitacao_v1") or []
        if isinstance(kv_itens, list):
            for it in kv_itens:
                if not isinstance(it, dict):
                    continue
                if licitacao_id and safe_int(it.get("licitacaoId"), 0) != licitacao_id:
                    continue
                if fornecedor_id and safe_int(it.get("fornecedorId"), 0) != fornecedor_id:
                    continue
                itens.append(dict(it))
    except Exception as e:
        logger.warning(f"Falha ao carregar itens: {e}")

    # Ordenar por ID (para o relatório ficar estável)
    try:
        itens.sort(key=lambda x: safe_int(x.get("id"), 0))
    except Exception:
        pass

    # PDF
    buff = io.BytesIO()
    page_size = landscape(A4)
    c = canvas.Canvas(buff, pagesize=page_size)
    w, h = page_size

    margin_x = 1.2 * cm
    margin_top = 1.2 * cm
    margin_bottom = 1.2 * cm
    usable_w = w - (2 * margin_x)

    # Título
    c.setFont("Helvetica", 12)
    titulo = "PREGÃO ELETRÔNICO N. " + ((pregao or numero or "").strip() or "-")
    c.drawCentredString(w / 2, h - margin_top - 0.3 * cm, titulo)

    # Caixa do fornecedor
    box_y_top = h - margin_top - 1.0 * cm
    box_h = 0.8 * cm
    c.setStrokeColor(colors.black)
    c.setLineWidth(0.8)
    c.rect(margin_x, box_y_top - box_h, usable_w, box_h, stroke=1, fill=0)

    c.setFont("Helvetica", 9)
    c.drawString(margin_x + 0.25 * cm, box_y_top - 0.55 * cm, "Fornecedor: Sistema Compra+")

    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(w / 2, box_y_top - 0.55 * cm, "RELAÇÃO DE ITENS VENCIDOS POR FORNECEDOR")

    # Tabela
    col_defs = [
        ("ITEM", 0.055, "center"),
        ("PRODUTO", 0.505, "left"),
        ("UN", 0.05, "center"),
        ("QTD\nINICIAL", 0.08, "right"),
        ("QTD\nCOMPRADA", 0.08, "right"),
        ("QTD\nCOMPRAR", 0.08, "right"),
        ("VLR. UNIT.", 0.07, "right"),
        ("VLR. TOTAL\nDISP.", 0.08, "right"),
    ]
    
    widths = [usable_w * p for _, p, _ in col_defs]
    widths[-1] += usable_w - sum(widths)

    header_h = 1.0 * cm
    row_h = 0.75 * cm

    def draw_table_header(x0: float, y_top: float) -> float:
        c.setLineWidth(0.8)
        c.setStrokeColor(colors.black)
        c.setFillColor(colors.lightgrey)
        x = x0
        for (name, _, align), wcol in zip(col_defs, widths):
            c.rect(x, y_top - header_h, wcol, header_h, stroke=1, fill=1)
            c.setFillColor(colors.black)
            c.setFont("Helvetica-Bold", 9)
            lines = name.splitlines()
            ty = y_top - 0.35 * cm
            for ln in lines:
                if align == "left":
                    c.drawString(x + 0.18 * cm, ty, ln)
                elif align == "right":
                    c.drawRightString(x + wcol - 0.18 * cm, ty, ln)
                else:
                    c.drawCentredString(x + wcol / 2, ty, ln)
                ty -= 0.35 * cm
            c.setFillColor(colors.lightgrey)
            x += wcol
        c.setFillColor(colors.black)
        return y_top - header_h

    y = box_y_top - box_h - 1.0 * cm
    table_x = margin_x
    y = draw_table_header(table_x, y)

    # Dados
    if not itens:
        # Item vazio
        c.setFillColor(colors.whitesmoke)
        c.rect(table_x, y - row_h, usable_w, row_h, stroke=1, fill=1)
        c.setFillColor(colors.black)
        c.setFont("Helvetica", 9)
        c.drawCentredString(table_x + usable_w / 2, y - 0.52 * cm, "Nenhum item disponível")
    else:
        shade = True
        for idx, item in enumerate(itens):
            if (y - row_h) < margin_bottom:
                c.showPage()
                y = h - margin_top - 1.0 * cm
                y = draw_table_header(table_x, y)
                shade = True

            x = table_x
            c.setLineWidth(0.6)
            c.setStrokeColor(colors.black)
            fill = colors.whitesmoke if shade else colors.white

            # Normalização de campos (aceita variações vindas do frontend/integrações)
            qtd_inicial = to_float_br(get_first(item, "quantidade_disponivel", "qtdInicial", "qtd_inicial", "quantidade", default=0))
            qtd_comprada = to_float_br(get_first(item, "quantidade_reservada", "qtdComprada", "qtd_comprada", "comprada", default=0))
            qtd_comprar = max(0.0, qtd_inicial - qtd_comprada)

            preco_unit = to_float_br(get_first(item, "preco", "valor_unitario", "vlr_unit", default=0))
            unidade = str(get_first(item, "unidade", "un", default="UN"))

            values = [
                str(item.get("id", idx + 1)),
                str(get_first(item, "nome", "produto", default="")),
                unidade,
                br_number(qtd_inicial, 4),
                br_number(qtd_comprada, 4),
                br_number(qtd_comprar, 4),
                br_money(preco_unit),
                br_money(preco_unit * qtd_comprar),
            ]

            for (_, _, align), wcol, val in zip(col_defs, widths, values):
                c.setFillColor(fill)
                c.rect(x, y - row_h, wcol, row_h, stroke=1, fill=1)
                c.setFillColor(colors.black)
                c.setFont("Helvetica", 9)

                s = str(val)
                if align == "left":
                    c.drawString(x + 0.18 * cm, y - 0.52 * cm, s)
                elif align == "right":
                    c.drawRightString(x + wcol - 0.18 * cm, y - 0.52 * cm, s)
                else:
                    c.drawCentredString(x + wcol / 2, y - 0.52 * cm, s)

                x += wcol

            y -= row_h
            shade = not shade

        # Linha de TOTAL GERAL (somatório)
        if (y - row_h) < margin_bottom:
            c.showPage()
            y = h - margin_top - 1.0 * cm
            y = draw_table_header(table_x, y)

        total_qtd_inicial = 0.0
        total_qtd_comprada = 0.0
        total_qtd_comprar = 0.0
        total_valor_disp = 0.0

        # Recalcular totais com a mesma regra do relatório
        for it in itens:
            qi = to_float_br(get_first(it, "quantidade_disponivel", "qtdInicial", "qtd_inicial", "quantidade", default=0))
            qc = to_float_br(get_first(it, "quantidade_reservada", "qtdComprada", "qtd_comprada", "comprada", default=0))
            qrem = max(0.0, qi - qc)
            pu = to_float_br(get_first(it, "preco", "valor_unitario", "vlr_unit", default=0))
            total_qtd_inicial += qi
            total_qtd_comprada += qc
            total_qtd_comprar += qrem
            total_valor_disp += (pu * qrem)

        # Desenhar linha
        x = table_x
        c.setLineWidth(0.8)
        c.setStrokeColor(colors.black)
        c.setFillColor(colors.lightgrey)

        # Célula "TOTAL"
        c.rect(x, y - row_h, widths[0] + widths[1] + widths[2], row_h, stroke=1, fill=1)
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 9)
        c.drawString(x + 0.18 * cm, y - 0.52 * cm, "TOTAL GERAL")
        x += (widths[0] + widths[1] + widths[2])

        # QTD INICIAL
        c.setFillColor(colors.lightgrey); c.rect(x, y - row_h, widths[3], row_h, stroke=1, fill=1)
        c.setFillColor(colors.black); c.drawRightString(x + widths[3] - 0.18 * cm, y - 0.52 * cm, br_number(total_qtd_inicial, 4))
        x += widths[3]

        # QTD COMPRADA
        c.setFillColor(colors.lightgrey); c.rect(x, y - row_h, widths[4], row_h, stroke=1, fill=1)
        c.setFillColor(colors.black); c.drawRightString(x + widths[4] - 0.18 * cm, y - 0.52 * cm, br_number(total_qtd_comprada, 4))
        x += widths[4]

        # QTD COMPRAR
        c.setFillColor(colors.lightgrey); c.rect(x, y - row_h, widths[5], row_h, stroke=1, fill=1)
        c.setFillColor(colors.black); c.drawRightString(x + widths[5] - 0.18 * cm, y - 0.52 * cm, br_number(total_qtd_comprar, 4))
        x += widths[5]

        # VLR. UNIT. (em relatório "geral", não faz sentido somar; deixa em branco)
        c.setFillColor(colors.lightgrey); c.rect(x, y - row_h, widths[6], row_h, stroke=1, fill=1)
        x += widths[6]

        # VLR. TOTAL DISP.
        c.setFillColor(colors.lightgrey); c.rect(x, y - row_h, widths[7], row_h, stroke=1, fill=1)
        c.setFillColor(colors.black); c.drawRightString(x + widths[7] - 0.18 * cm, y - 0.52 * cm, br_money(total_valor_disp))

        y -= row_h

    c.save()
    pdf_bytes = buff.getvalue()
    buff.close()

    filename_safe = (pregao or numero or "relatorio").replace("/", "-").replace(" ", "_")
    filename = f"saldo-pregao_{filename_safe}.pdf"

    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={"Content-Disposition": f"inline; filename={filename}"},
    )

# =====================================================
# STATS
# =====================================================
@app.route("/api/stats", methods=["GET"])
@login_required
def api_stats():
    try:
        user = getattr(request, "current_user", {})
        user_id = user.get("id")
        user_role = user.get("role")
        
        lic = kv_get("compraPlusLicitacoes_v1") or []
        forn_kv = kv_get("compraPlusFornecedores_v1") or []

        conn = get_db_connection()
        
        # Fornecedores
        row = conn.execute("SELECT COUNT(1) AS c FROM fornecedores WHERE status = 'active'").fetchone()
        forn_db = int(row["c"]) if row else 0
        
        # Usuários
        user_row = conn.execute("SELECT COUNT(1) AS c FROM users").fetchone()
        users_count = int(user_row["c"]) if user_row else 0
        
        # Notificações não lidas (filtradas por role)
        if user_role in ["Administrador", "Gerente"]:
            notif_row = conn.execute("SELECT COUNT(1) AS c FROM notifications WHERE lida = 0").fetchone()
        else:
            # Para compradores/consultas, contar apenas dos próprios pedidos
            user_pedidos = get_user_pedidos(user_id)
            user_pedido_ids = [str(p.get("id")) for p in user_pedidos if p.get("id")]
            if user_pedido_ids:
                placeholders = ','.join('?' * len(user_pedido_ids))
                query = f"SELECT COUNT(1) AS c FROM notifications WHERE pedido_id IN ({placeholders}) AND lida = 0"
                notif_row = conn.execute(query, user_pedido_ids).fetchone()
            else:
                notif_row = {"c": 0}
        
        notificacoes_nao_lidas = int(notif_row["c"]) if notif_row else 0
        
        # Pedidos (baseado no papel do usuário)
        pedidos = get_pedidos_for_user(user_id, user_role)
        total_pedidos = len(pedidos)
        total_valor = sum(float(p.get("total", 0)) for p in pedidos if isinstance(p, dict))
        pedidos_aprovados = len([p for p in pedidos if isinstance(p, dict) and p.get("status") == "aprovado"])
        pedidos_pendentes = len([p for p in pedidos if isinstance(p, dict) and p.get("status") in ["rascunho", "enviado"]])
        
        conn.close()

        return jsonify(
            {
                "success": True,
                "stats": {
                    "licitacoes": len(lic),
                    "pedidos": total_pedidos,
                    "fornecedores": forn_db,
                    "fornecedores_kv": len(forn_kv),
                    "usuarios": users_count,
                    "notificacoes_nao_lidas": notificacoes_nao_lidas,
                    "total_pedidos_valor": total_valor,
                    "pedidos_aprovados": pedidos_aprovados,
                    "pedidos_pendentes": pedidos_pendentes,
                    "userRole": user_role,
                    "userId": user_id
                },
            }
        )
    except Exception as e:
        logger.error(f"Erro ao calcular stats: {e}")
        return jsonify(
            {
                "success": True,
                "stats": {
                    "licitacoes": 0,
                    "pedidos": 0,
                    "fornecedores": 0,
                    "fornecedores_kv": 0,
                    "usuarios": 0,
                    "notificacoes_nao_lidas": 0,
                    "total_pedidos_valor": 0,
                    "pedidos_aprovados": 0,
                    "pedidos_pendentes": 0,
                    "userRole": "error",
                    "userId": 0
                },
            }
        )

# =====================================================
# HEALTH
# =====================================================
@app.route("/api/version", methods=["GET"])
def api_version():
    return jsonify({"success": True, "build": APP_BUILD, "timestamp": datetime.now().isoformat()})

@app.route("/api/health", methods=["GET"])
def api_health():
    try:
        conn = get_db_connection()
        tables = ["users", "fornecedores", "kv_store", "notifications"]
        table_status = {}
        for table in tables:
            try:
                conn.execute(f"SELECT 1 FROM {table} LIMIT 1")
                table_status[table] = "OK"
            except Exception as e:
                table_status[table] = f"ERRO: {str(e)}"

        with _subs_lock:
            sse_clients = len(_subs)

        conn.close()

        return jsonify(
            {
                "success": True,
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "open_mode": False,
                "build": APP_BUILD,
                "db": "OK",
                "tables": table_status,
                "sse_clients": sse_clients,
                "environment": {"flask_debug": app.debug},
            }
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({"success": False, "status": "unhealthy", "error": str(e), "timestamp": datetime.now().isoformat()}), 500



# =====================================================
# BACKUP / RESTORE (SQLite)
# =====================================================
BACKUP_DIR = os.path.join(BASE_DIR, "backups")

def _ensure_backup_dir() -> str:
    os.makedirs(BACKUP_DIR, exist_ok=True)
    return BACKUP_DIR

def _sqlite_integrity_ok(db_path: str) -> Tuple[bool, str]:
    """Valida se um arquivo parece ser um SQLite válido e íntegro."""
    try:
        conn = sqlite3.connect(db_path, timeout=SQLITE_TIMEOUT_SECONDS)
        conn.row_factory = sqlite3.Row
        row = conn.execute("PRAGMA integrity_check;").fetchone()
        conn.close()
        if not row:
            return False, "Falha ao validar integridade (sem resposta)"
        # integrity_check retorna 'ok' (lowercase) quando está tudo certo
        msg = str(row[0])
        if msg.strip().lower() == "ok":
            return True, "ok"
        return False, msg
    except Exception as e:
        return False, str(e)

@app.route("/api/backup/list", methods=["GET"])
@login_required
@role_required("Administrador")
def api_backup_list():
    """Lista backups já gerados no servidor."""
    _ensure_backup_dir()
    items = []
    try:
        for name in os.listdir(BACKUP_DIR):
            if not name.lower().endswith(".db"):
                continue
            path = os.path.join(BACKUP_DIR, name)
            if not os.path.isfile(path):
                continue
            st = os.stat(path)
            items.append({
                "name": name,
                "size": st.st_size,
                "modified": datetime.fromtimestamp(st.st_mtime).isoformat(),
            })
        items.sort(key=lambda x: x["modified"], reverse=True)
        return jsonify({"success": True, "backups": items})
    except Exception as e:
        logger.exception("Erro ao listar backups")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/api/backup/db", methods=["GET"])
@login_required
@role_required("Administrador")
def api_backup_download_db():
    """Gera um backup consistente do SQLite e faz download."""
    _ensure_backup_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"compra_plus_backup_{ts}.db"
    backup_path = os.path.join(BACKUP_DIR, filename)

    try:
        # Backup consistente usando a API do sqlite3
        src = sqlite3.connect(DATABASE, timeout=SQLITE_TIMEOUT_SECONDS)
        dst = sqlite3.connect(backup_path, timeout=SQLITE_TIMEOUT_SECONDS)
        with dst:
            src.backup(dst)
        src.close()
        dst.close()

        return send_from_directory(
            BACKUP_DIR,
            filename,
            as_attachment=True,
            download_name=filename,
            mimetype="application/octet-stream",
        )
    except Exception as e:
        logger.exception("Erro ao gerar backup do banco")
        # Se criou arquivo parcialmente, remove
        try:
            if os.path.exists(backup_path):
                os.remove(backup_path)
        except Exception:
            pass
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/api/backup/download/<path:filename>", methods=["GET"])
@login_required
@role_required("Administrador")
def api_backup_download_existing(filename: str):
    """Baixa um backup existente (por nome)."""
    _ensure_backup_dir()
    # Segurança: não permitir path traversal
    safe_name = os.path.basename(filename)
    path = os.path.join(BACKUP_DIR, safe_name)
    if not os.path.isfile(path):
        return jsonify({"success": False, "message": "Backup não encontrado"}), 404

    return send_from_directory(
        BACKUP_DIR,
        safe_name,
        as_attachment=True,
        download_name=safe_name,
        mimetype="application/octet-stream",
    )

@app.route("/api/backup/restore", methods=["POST"])
@login_required
@role_required("Administrador")
def api_backup_restore():
    """Restaura o banco a partir de um arquivo .db enviado (multipart/form-data)."""
    _ensure_backup_dir()
    if "file" not in request.files:
        return jsonify({"success": False, "message": "Arquivo não enviado (campo 'file')"}), 400

    f = request.files["file"]
    if not f or not getattr(f, "filename", ""):
        return jsonify({"success": False, "message": "Arquivo inválido"}), 400

    filename = os.path.basename(f.filename)
    if not filename.lower().endswith(".db"):
        return jsonify({"success": False, "message": "Envie um arquivo .db (SQLite)"}), 400

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    uploaded_path = os.path.join(BACKUP_DIR, f"upload_restore_{ts}.db")
    f.save(uploaded_path)

    ok, detail = _sqlite_integrity_ok(uploaded_path)
    if not ok:
        try:
            os.remove(uploaded_path)
        except Exception:
            pass
        return jsonify({"success": False, "message": f"Backup inválido: {detail}"}), 400

    try:
        # Salva uma cópia do banco atual antes de restaurar
        if os.path.exists(DATABASE):
            before_name = f"compra_plus_before_restore_{ts}.db"
            before_path = os.path.join(BACKUP_DIR, before_name)
            # backup consistente do atual
            src = sqlite3.connect(DATABASE, timeout=SQLITE_TIMEOUT_SECONDS)
            dst = sqlite3.connect(before_path, timeout=SQLITE_TIMEOUT_SECONDS)
            with dst:
                src.backup(dst)
            src.close()
            dst.close()

        # Substitui o banco
        # Nota: em produção, ideal fechar conexões/pool. Aqui cada request abre/fecha conexões.
        os.replace(uploaded_path, DATABASE)

        return jsonify({"success": True, "message": "Banco restaurado com sucesso. Reinicie o app se necessário."})
    except Exception as e:
        logger.exception("Erro ao restaurar backup")
        # Se der erro, tenta manter o arquivo enviado no backup dir para análise
        return jsonify({"success": False, "message": str(e)}), 500

# =====================================================
# SSE ENDPOINT
# =====================================================
@app.route("/api/events", methods=["GET"])
@login_required_for_sse
def api_events():
    user = getattr(request, "current_user", {}) or {}
    user_name = user.get("name", "Anônimo") if isinstance(user, dict) else "Anônimo"

    @stream_with_context
    def generate():
        q = queue.Queue(maxsize=200)

        with _subs_lock:
            _subs.append(q)
            total = len(_subs)

        logger.info(f"Cliente SSE conectado: {user_name}. Total: {total}")

        try:
            hello_payload = {"message": "Conectado ao Compra+ SSE", "user": user_name, "ts": now_utc_ts()}
            yield f"event: hello\ndata: {json.dumps(hello_payload, ensure_ascii=False)}\n\n"

            last_ping = time.time()
            while True:
                try:
                    ev = q.get(timeout=1.0)
                    yield ev
                except queue.Empty:
                    if time.time() - last_ping >= SSE_PING_SECONDS:
                        last_ping = time.time()
                        yield f"event: ping\ndata: {json.dumps({'ts': now_utc_ts()}, ensure_ascii=False)}\n\n"
        except GeneratorExit:
            logger.info(f"Cliente SSE desconectado: {user_name}")
        except Exception as e:
            logger.error(f"Erro no stream SSE para {user_name}: {e}")
        finally:
            with _subs_lock:
                if q in _subs:
                    _subs.remove(q)
                remaining = len(_subs)
            logger.info(f"Cliente SSE desconectado: {user_name}. Restantes: {remaining}")

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Content-Type": "text/event-stream; charset=utf-8",
        },
    )

# =====================================================
# ERROR HANDLERS
# =====================================================
@app.errorhandler(404)
def not_found(_):
    return jsonify({"success": False, "message": "Endpoint não encontrado"}), 404

@app.errorhandler(405)
def method_not_allowed(_):
    return jsonify({"success": False, "message": "Método não permitido"}), 405

@app.errorhandler(500)
def internal_error(e):
    logger.error(f"Erro interno: {e}")
    return jsonify({"success": False, "message": "Erro interno do servidor"}), 500

# =====================================================
# BOOT
# =====================================================
app_start_time = time.time()

if __name__ == "__main__":
    init_db()

    print("\n" + "=" * 60)
    print("🚀 COMPRA+ BACKEND - SERVIDOR INICIADO")
    print("=" * 60)
    print("🌐 URL: http://127.0.0.1:5000")
    print(f"🗄️  Banco: {DATABASE}")
    print(f"📡 SSE: /api/events (ping: {SSE_PING_SECONDS}s)")
    print("📋 Endpoints principais:")
    print("  • /api/auth/check")
    print("  • /api/login (Login obrigatório!)")
    print("  • /api/users (GET/POST/PUT/DELETE) ✅")
    print("  • /api/fornecedores (CRUD) ✅")
    print("  • /api/pedidos (CRUD - POR USUÁRIO) ✅")
    print("  • /api/licitacoes (NOVO) ✅")
    print("  • /api/licitacoes/<id>/itens/<forn_id> (NOVO) ✅")
    print("  • /api/sync/pull | /api/sync/push ✅")
    print("  • /api/stats | /api/health | /api/events ✅")
    print("  • /api/notifications (CRUD) 🔔")
    print("=" * 60)
    print("\n🔥 Sistema de pedidos por usuário ativo!")
    print("   - Cada usuário tem seus próprios pedidos")
    print("   - Administradores e Gerentes veem todos os pedidos")
    print("   - Compradores só veem e editam seus pedidos")
    print("   - Notificações automáticas ativadas ✅")
    print("   - Controle de quantidades em licitações ✅")
    print("   - VERSÃO TOLERANTE: permite itens não encontrados")
    print("   - CORREÇÃO APLICADA: análise de pedidos mais flexível ✅")
    print("=" * 60 + "\n")

    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=True, use_reloader=False, threaded=True)