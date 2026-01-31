# Compra+ (Flask + HTML estático) — pronto para Render

Este repositório está organizado para publicar no Render como um serviço web Python.

## Estrutura

- `app.py` — API Flask + autenticação + SSE + geração de PDF + serve as páginas
- `static/` — páginas HTML (`index.html`, `dashboard.html`, `fornecedores.html`, etc.)
- `compra_plus.db` — banco SQLite de exemplo (seed)
- `requirements.txt` — dependências
- `Procfile` — comando padrão para rodar no Render
- `render.yaml` — blueprint opcional (Render cria o serviço automaticamente)

## Rodar local

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# opcional
export COMPRA_PLUS_SECRET='dev-secret'

python app.py
# abre: http://localhost:5000
```

## Deploy no Render (mais fácil)

### Opção A — usando `render.yaml` (Blueprint)
1. Suba este projeto para um repositório (GitHub/GitLab).
2. No Render: **New > Blueprint** e selecione o repo.
3. O Render vai usar o `render.yaml` para criar o serviço.

### Opção B — manual
1. **New > Web Service**
2. Build Command: `pip install -r requirements.txt`
3. Start Command: `gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 120`

## Variáveis de ambiente

- `COMPRA_PLUS_SECRET` (obrigatório em produção)
- `COMPRA_PLUS_TOKEN_SALT`
- `COMPRA_PLUS_TOKEN_EXPIRES` (padrão: 86400)
- `COMPRA_PLUS_DB` (caminho do SQLite; padrão: `./compra_plus.db`)

## Persistência do SQLite no Render

O filesystem do Render pode ser efêmero dependendo do plano. Para persistir o banco:

1. Crie/Anexe um **Disk** (ex.: `/var/data`).
2. Configure `COMPRA_PLUS_DB=/var/data/compra_plus.db`.

> Dica: você pode versionar um `.db` inicial (seed) no repo, mas em produção é melhor manter fora do Git.
