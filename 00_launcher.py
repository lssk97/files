# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Mini BI — launcher
# MAGIC Sobe o **Streamlit como subprocess** dentro deste notebook (serverless ou cluster clássico), sem usar Databricks Apps.
# MAGIC
# MAGIC **Como funciona**
# MAGIC 1. `bridge.py` sobe um servidor HTTP local (`127.0.0.1`) numa thread deste notebook, usando o `spark` daqui para executar SQL e o `WorkspaceClient` para Genie / Model Serving.
# MAGIC 2. O Streamlit (`app.py`) roda como subprocess e só conversa com essa ponte.
# MAGIC 3. O acesso pelo navegador é via **driver proxy** (clusters clássicos) ou **túnel** (opcional — leia o aviso abaixo).
# MAGIC
# MAGIC Coloque `00_launcher`, `app.py`, `bridge.py` e `dashboards.yaml` **na mesma pasta** do Workspace e execute as células em ordem.

# COMMAND ----------

# MAGIC %pip install -q "streamlit>=1.37" "plotly>=5.20" pyyaml "databricks-sdk>=0.40"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Parâmetros
dbutils.widgets.dropdown("acesso", "driver_proxy", ["driver_proxy", "tunel_cloudflare", "somente_local"], "Modo de acesso")
dbutils.widgets.text("porta", "8501", "Porta Streamlit")
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct", "Endpoint LLM (sem Genie)")
dbutils.widgets.text("senha", "", "Senha do app (opcional)")
dbutils.widgets.text("app_dir", "", "Pasta do app (vazio = pasta deste notebook)")

# COMMAND ----------

# DBTITLE 1,Sobe a ponte Spark + o Streamlit
import importlib, json, os, re, secrets, shutil, signal, subprocess, sys, time, urllib.request
import requests

ACESSO = dbutils.widgets.get("acesso")
PORTA = int(dbutils.widgets.get("porta"))
PORTA_BRIDGE = PORTA + 264  # 8765 por padrão
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint").strip()
SENHA = dbutils.widgets.get("senha").strip()

# --- contexto do notebook --------------------------------------------------------------
# Clusters em modo de acesso "Standard/Shared" (Unity Catalog) bloqueiam getContext().toJson()
# (Py4JSecurityException). Por isso lemos o contexto por fontes permitidas, com fallbacks.
def _primeiro(*fontes):
    for f in fontes:
        try:
            v = f()
            if v:
                return str(v)
        except Exception:
            pass
    return ""

def _repl_ctx():
    from dbruntime.databricks_repl_context import get_context
    return get_context()

def _conf(chave):
    return lambda: spark.conf.get(chave)

try:
    _rc = _repl_ctx()
except Exception:
    _rc = None
host = _primeiro(lambda: _rc.browserHostName,
                 _conf("spark.databricks.workspaceUrl"),
                 lambda: _rc.workspaceUrl).replace("https://", "").rstrip("/")
org_id = _primeiro(lambda: _rc.workspaceId,
                   _conf("spark.databricks.clusterUsageTags.clusterOwnerOrgId"),
                   _conf("spark.databricks.clusterUsageTags.orgId"))
cluster_id = _primeiro(lambda: _rc.clusterId,
                       _conf("spark.databricks.clusterUsageTags.clusterId"))
nb_path = _primeiro(lambda: _rc.notebookPath,
                    lambda: dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
print(f"🔎 host={host or '?'} org={org_id or '?'} cluster={cluster_id or '?'} notebook={nb_path or '?'}")

APP_DIR = dbutils.widgets.get("app_dir").strip()
if not APP_DIR:
    if nb_path:
        APP_DIR = "/Workspace" + os.path.dirname(nb_path) if not nb_path.startswith("/Workspace") else os.path.dirname(nb_path)
    else:
        APP_DIR = os.getcwd()  # DBR 14+: o diretório de trabalho é a pasta do notebook
assert os.path.exists(os.path.join(APP_DIR, "app.py")), f"app.py não encontrado em {APP_DIR} — preencha o widget 'app_dir'."
print("📁 Pasta do app:", APP_DIR)

# --- 1) ponte Spark ----------------------------------------------------------------------
sys.path.insert(0, APP_DIR)
import bridge
importlib.reload(bridge)
TOKEN = secrets.token_urlsafe(24)
bridge.iniciar_bridge(spark, PORTA_BRIDGE, TOKEN, LLM_ENDPOINT or None)
print(f"🔌 Ponte Spark ativa em 127.0.0.1:{PORTA_BRIDGE}")

# --- 2) encerra execuções anteriores -----------------------------------------------------
for nome in ("_MINIBI_ST", "_MINIBI_TUNEL"):
    p = globals().get(nome)
    if p and p.poll() is None:
        p.terminate()
subprocess.run(["pkill", "-f", f"streamlit run .*--server.port {PORTA}"], check=False)
subprocess.run(["pkill", "-f", "cloudflared tunnel"], check=False)
time.sleep(1)

# --- 3) monta URL / base path conforme o modo de acesso ----------------------------------
base_path, url_publica = "", None

if ACESSO == "driver_proxy":
    if not (cluster_id and org_id and host):
        print("⚠️  Não consegui descobrir host/org/cluster (veja a linha '🔎' acima). Se o compute for serverless,\n"
              "    o driver proxy não existe — use um cluster clássico ou o modo 'tunel_cloudflare'.")
    else:
        base_path = f"/driver-proxy/o/{org_id}/{cluster_id}/{PORTA}"
        url_publica = f"https://{host}{base_path}/"

if ACESSO == "tunel_cloudflare" and not SENHA:
    SENHA = secrets.token_urlsafe(8)
    print(f"🔐 Túnel público exige senha. Senha gerada: {SENHA}")

# --- 4) Streamlit como subprocess ---------------------------------------------------------
LOG_ST = "/tmp/minibi_streamlit.log"
env = {**os.environ,
       "MINIBI_BRIDGE_PORT": str(PORTA_BRIDGE), "MINIBI_BRIDGE_TOKEN": TOKEN,
       "MINIBI_SENHA": SENHA, "MINIBI_CONFIG": os.path.join(APP_DIR, "dashboards.yaml"),
       "STREAMLIT_BROWSER_GATHER_USAGE_STATS": "false"}
cmd = [sys.executable, "-m", "streamlit", "run", os.path.join(APP_DIR, "app.py"),
       "--server.port", str(PORTA), "--server.address", "0.0.0.0", "--server.headless", "true",
       "--server.enableCORS", "false", "--server.enableXsrfProtection", "false",
       "--server.fileWatcherType", "none", "--theme.base", "light"]
if base_path:
    cmd += ["--server.baseUrlPath", base_path]
_MINIBI_ST = subprocess.Popen(cmd, env=env, cwd="/tmp", stdout=open(LOG_ST, "w"), stderr=subprocess.STDOUT,
                              start_new_session=True)

saude = f"http://127.0.0.1:{PORTA}{base_path}/_stcore/health"
for _ in range(60):
    try:
        if requests.get(saude, timeout=2).ok:
            break
    except Exception:
        pass
    if _MINIBI_ST.poll() is not None:
        raise RuntimeError("Streamlit encerrou:\n" + open(LOG_ST).read()[-3000:])
    time.sleep(1)
else:
    raise RuntimeError("Streamlit não ficou saudável em 60 s:\n" + open(LOG_ST).read()[-3000:])
print(f"✅ Streamlit rodando (pid {_MINIBI_ST.pid}) — log em {LOG_ST}")

# --- 5) túnel opcional --------------------------------------------------------------------
if ACESSO == "tunel_cloudflare":
    binario = "/tmp/cloudflared"
    if not os.path.exists(binario):
        urllib.request.urlretrieve(
            "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64", binario)
        os.chmod(binario, 0o755)
    LOG_TUN = "/tmp/minibi_tunel.log"
    _MINIBI_TUNEL = subprocess.Popen([binario, "tunnel", "--no-autoupdate", "--url", f"http://127.0.0.1:{PORTA}"],
                                     stdout=open(LOG_TUN, "w"), stderr=subprocess.STDOUT, start_new_session=True)
    for _ in range(40):
        m = re.search(r"https://[-a-z0-9]+\.trycloudflare\.com", open(LOG_TUN).read())
        if m:
            url_publica = m.group(0)
            break
        time.sleep(1)
    else:
        raise RuntimeError("Túnel não subiu (egress bloqueado?):\n" + open(LOG_TUN).read()[-2000:])

# --- 6) link ------------------------------------------------------------------------------
if url_publica:
    displayHTML(f"""
      <div style="font-family:system-ui;padding:16px;border:1px solid #ddd;border-radius:12px">
        <div style="font-size:18px;font-weight:600">📊 Mini BI no ar</div>
        <p><a href="{url_publica}" target="_blank" style="font-size:16px">{url_publica}</a></p>
        {'<p>Senha: <code>' + SENHA + '</code></p>' if SENHA else ''}
        <p style="color:#777">Mantenha a célula de keep-alive rodando enquanto usa o app.</p>
      </div>""")
else:
    print(f"App local em http://127.0.0.1:{PORTA} (sem URL externa neste modo).")

# COMMAND ----------

# DBTITLE 1,Keep-alive (deixe rodando enquanto usa o app)
# O serverless encerra a sessão ociosa; esta célula mantém o notebook ativo e avisa se o Streamlit cair.
import time
try:
    while True:
        if _MINIBI_ST.poll() is not None:
            print("Streamlit caiu — veja /tmp/minibi_streamlit.log e rode a célula anterior de novo.")
            break
        time.sleep(30)
except KeyboardInterrupt:
    print("Keep-alive interrompido (o app continua até a sessão expirar).")

# COMMAND ----------

# DBTITLE 1,Parar tudo
for nome in ("_MINIBI_ST", "_MINIBI_TUNEL"):
    p = globals().get(nome)
    if p and p.poll() is None:
        p.terminate()
bridge.parar_bridge()
print("🛑 Mini BI encerrado.")

# COMMAND ----------

# DBTITLE 1,Diagnóstico
print(open("/tmp/minibi_streamlit.log").read()[-4000:])
