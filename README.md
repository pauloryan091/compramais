# CompraMais (Render-ready)

Estrutura pronta para deploy no Render com Flask + arquivos HTML em `/static` e SSE ajustado para Gunicorn.

## Estrutura
- `app.py` (Flask)
- `static/` (todas as páginas HTML)
- `data/compra_plus.db` (SQLite local para dev)
- `requirements.txt`
- `Procfile`
- `render.yaml` (opcional)

## Deploy no Render
1. Suba esta pasta para um repositório (GitHub).
2. No Render:
   - **Blueprint** (recomendado): New -> Blueprint, ele lê `render.yaml`
   - ou Web Service:
     - Build: `pip install -r requirements.txt`
     - Start: `gunicorn app:app --bind 0.0.0.0:$PORT --worker-class gevent --workers 2 --timeout 0 --keep-alive 75`

## Banco de dados
No Render, para não perder dados, use um **Disk** e configure a variável:
- `COMPRA_PLUS_DB=/var/data/compra_plus.db`

Localmente, se `COMPRA_PLUS_DB` não estiver setada, o app pode usar o SQLite padrão do projeto.

## Por que gevent + timeout 0?
O endpoint `/api/events` mantém conexão SSE aberta. Com worker sync padrão e timeout, o Gunicorn mata o worker (WORKER TIMEOUT).
