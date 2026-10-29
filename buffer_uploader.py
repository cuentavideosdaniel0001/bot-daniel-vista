"""
Sube un video ya procesado (master o render por cuenta) a Buffer, corriendo
DENTRO del mismo worker del VPS -- portado de la logica de Buffer en
subida_bot.py (el bot de escritorio que corria en una PC local), pero:

  - headless=True siempre (no hay pantalla en el VPS).
  - "Schedule Post" se clickea de VERDAD (subida_bot.py en su version de PC
    tambien lo hacia -- esto no es una prueba, es la subida real).
  - Sin GUI/tkinter -- logging con el modulo `logging` normal.
  - Screenshot automatico a disco + subido a B2 (logs/screenshots/) en
    cualquier error, para poder diagnosticar sin acceso a pantalla.
  - Usa directamente la ruta local del archivo ya renderizado (el worker lo
    tiene ahi mismo, recien subido a B2 -- no hace falta re-descargarlo).

Reusa las mismas tablas de Supabase que subida_bot.py (`cuentas`,
`buffer_logins`, `ultimos_videos`) para que ambos caminos (PC manual, VPS
automatico) sean intercambiables sin migrar datos.
"""
import os
import re
import time
import tempfile
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import boto3
import requests
import cv2
from skimage.metrics import structural_similarity as ssim
from supabase import create_client
from playwright.sync_api import sync_playwright

from comun import limitar_hashtags

log = logging.getLogger("buffer_uploader")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
B2_KEY_ID = os.environ["B2_KEY_ID"]
B2_APP_KEY = os.environ["B2_APP_KEY"]
B2_BUCKET = os.environ["B2_BUCKET"]
B2_ENDPOINT = os.environ["B2_ENDPOINT"]
B2_REGION = os.environ["B2_REGION"]

BUFFER_API_URL = "https://api.buffer.com"

# Carpeta persistente con los perfiles de Firefox (copiados desde las PCs
# donde ya estaban logueados -- ver PASOS.md/REGISTRO_DE_PROGRESO.md del
# 2026-07-25 para como se armo y valido esto). Configurable por si el path
# cambia en otra maquina.
CARPETA_FIREFOX_PROFILES = os.getenv("FIREFOX_PROFILES_DIR", "/root/firefox_profiles")
CARPETA_SCREENSHOTS = os.getenv("SCREENSHOTS_DIR", "/root/repo_integrado/screenshots")
os.makedirs(CARPETA_SCREENSHOTS, exist_ok=True)

# Mismo valor/env var que handler.py -- necesario acá para poder marcar de
# una una cuenta (y sus hermanas de perfil) como agotada cuando se confirma
# que el login no se pudo completar solo (ver _marcar_perfil_bloqueado_por_login).
MAX_INTENTOS_SUBIDA = int(os.getenv("MAX_INTENTOS_SUBIDA", "3"))

# Mismas env vars que maneja handler.notificar_error()/reportes._enviar_telegram_mensaje()
# -- se manda un tercer aviso directo, propio de este modulo (mismo criterio
# de "cada modulo manda su Telegram por su cuenta" que ya existia entre esos
# dos), porque este SI necesita ser inmediato (ver _notificar_telegram_inmediato).
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Mismo criterio que _get_timeout()/BASE_TIMEOUT en subida_bot.py: el
# timeout de cada paso escala segun cuantos PERFILES de Firefox estan
# corriendo en paralelo ahora mismo en este worker -- con varios Firefox
# headless a la vez (MAX_SUBIDAS_PARALELAS>1) cada paso individual tarda
# mas por contencion real de CPU/red en el VPS. Un timeout fijo causaba
# timeouts intermitentes reales bajo carga (confirmado en produccion
# 2026-07-26, ej. esperando el campo "Video title" o el click de
# "Schedule Post" con 4 perfiles activos a la vez). Nunca baja de
# BASE_TIMEOUT aunque haya 0 o 1 perfil activo.
BASE_TIMEOUT = int(os.getenv("BUFFER_TIMEOUT_MS", "10000"))

# Tiempo extra por caracter al escribir la descripcion con editor.type() en
# _subir_video_pagina: el timeout escala con el largo del texto. Medido
# 2026-10-22 (video_226): ~75 ms/caracter con 4 Firefox en paralelo; 150
# deja el doble de margen.
MS_POR_CARACTER = 150

_perfiles_activos = 0
_perfiles_activos_lock = threading.Lock()


def _get_timeout() -> int:
    with _perfiles_activos_lock:
        n = max(_perfiles_activos, 1)
    return BASE_TIMEOUT * n


def _incrementar_perfiles_activos():
    global _perfiles_activos
    with _perfiles_activos_lock:
        _perfiles_activos += 1


def _decrementar_perfiles_activos():
    global _perfiles_activos
    with _perfiles_activos_lock:
        _perfiles_activos = max(_perfiles_activos - 1, 0)

# Maximo de posts en cola por canal segun el plan de Buffer (10 en el
# gratuito) -- mismo valor que constantes.py (subida_bot.py, PC).
MAX_QUEUE_PER_CHANNEL = int(os.getenv("MAX_QUEUE_PER_CHANNEL", "10"))

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
b2_client = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    region_name=B2_REGION,
    aws_access_key_id=B2_KEY_ID,
    aws_secret_access_key=B2_APP_KEY,
)

_firefox_playwright_listo = False


def asegurar_firefox_playwright():
    """Mismo criterio de seguridad que subida_bot.py -- si por lo que sea el
    venv de este worker no tiene Firefox de Playwright instalado, lo instala
    solo en vez de fallar en silencio. En la practica ya se instala una vez
    al armar el venv (ver PASOS.md), esto es solo una red de seguridad."""
    global _firefox_playwright_listo
    if _firefox_playwright_listo:
        return True
    try:
        import playwright._impl._driver as _pw_driver
        import subprocess
        node_exe, cli_js = _pw_driver.compute_driver_executable()
        resultado = subprocess.run([node_exe, cli_js, "install", "firefox"], capture_output=True, text=True, timeout=300)
        if resultado.returncode != 0:
            log.error("No se pudo instalar Firefox de Playwright: %s", resultado.stderr[-500:])
            return False
        _firefox_playwright_listo = True
        return True
    except Exception as e:
        log.error("Error asegurando Firefox de Playwright: %s", e)
        return False


# ── Supabase: lookups (identico en criterio a subida_bot.py) ────────────

def obtener_canal(cuenta_key: str) -> Optional[dict]:
    filas = supabase.table("cuentas").select("*").eq("cuenta", cuenta_key).execute().data
    return filas[0] if filas else None


_cache_api_key_buffer: dict = {}


def obtener_api_key_buffer(login: str) -> Optional[str]:
    if login in _cache_api_key_buffer:
        return _cache_api_key_buffer[login]
    try:
        filas = supabase.table("buffer_logins").select("api_key").eq("login", login).execute().data
    except Exception as e:
        log.error("Error consultando buffer_logins para %s: %s", login, e)
        return None
    api_key = filas[0]["api_key"] if filas else None
    _cache_api_key_buffer[login] = api_key
    return api_key


_cache_password_buffer: dict = {}


def obtener_password_buffer(login: str) -> Optional[str]:
    """Password de la cuenta de Buffer (columna 'password' de buffer_logins,
    agregada 2026-08-27 para el auto-login -- ver _intentar_login_buffer).
    Es la contrasena del LOGIN WEB, distinta de 'api_key' (esa es para la
    API GraphQL, no sirve para el formulario de login.buffer.com). Mismo
    criterio de cache que obtener_api_key_buffer: no expira dentro del
    proceso, rotar la password requiere reiniciar el worker."""
    if login in _cache_password_buffer:
        return _cache_password_buffer[login]
    try:
        filas = supabase.table("buffer_logins").select("password").eq("login", login).execute().data
    except Exception as e:
        log.error("Error consultando buffer_logins (password) para %s: %s", login, e)
        return None
    password = filas[0]["password"] if filas else None
    _cache_password_buffer[login] = password
    return password


def obtener_ultimo_video(cuenta_key: str) -> Optional[str]:
    filas = supabase.table("ultimos_videos").select("post_id").eq("cuenta", cuenta_key).execute().data
    return filas[0]["post_id"] if filas else None


def guardar_ultimo_video(cuenta_key: str, post_id: str):
    supabase.table("ultimos_videos").upsert(
        {"cuenta": cuenta_key, "post_id": post_id, "updated_at": datetime.now(timezone.utc).isoformat()},
        on_conflict="cuenta",
    ).execute()


# ── Cliente GraphQL de Buffer (identico a subida_bot.py) ────────────────

@dataclass
class Channel:
    id: str
    name: str
    service: str
    channel_type: str
    is_disconnected: bool
    is_locked: bool
    organization_id: str


class BufferClient:
    def __init__(self, api_key: str, account_name: str = ""):
        self.api_key = api_key
        self.account_name = account_name
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        })

    def _query(self, query: str, variables: dict = None) -> dict:
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        try:
            resp = self.session.post(BUFFER_API_URL, json=payload, timeout=30)
        except requests.RequestException as e:
            raise ConnectionError(f"Error de red: {e}") from e
        if resp.status_code == 429:
            retry_after = resp.json().get("errors", [{}])[0].get("extensions", {}).get("retryAfter", 900)
            raise RuntimeError(f"Rate limit alcanzado. Reintentar en {retry_after}s.")
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        if "errors" in data:
            messages = [e.get("message", str(e)) for e in data["errors"]]
            raise RuntimeError(f"GraphQL error: {'; '.join(messages)}")
        return data.get("data", {})

    def get_channels(self, organization_id: str) -> list:
        query = """
        query GetChannels($input: ChannelsInput!) {
          channels(input: $input) { id name service type isDisconnected isLocked organizationId }
        }
        """
        data = self._query(query, {"input": {"organizationId": organization_id}})
        return [
            Channel(id=c["id"], name=c["name"], service=c["service"], channel_type=c["type"],
                    is_disconnected=c["isDisconnected"], is_locked=c["isLocked"], organization_id=c["organizationId"])
            for c in data.get("channels", [])
        ]

    def get_scheduled_posts(self, organization_id: str, channel_id: str) -> set:
        query = """
        query GetScheduledCount($input: PostsInput!) {
          posts(input: $input, first: 100) { edges { node { id } } pageInfo { hasNextPage } }
        }
        """
        variables = {"input": {"organizationId": organization_id, "filter": {"channelIds": [channel_id], "status": ["scheduled"]}}}
        data = self._query(query, variables)
        edges = data.get("posts", {}).get("edges", [])
        return {e["node"]["id"] for e in edges}

    def get_ultimo_post_cola(self, organization_id: str, channel_id: str) -> Optional[dict]:
        query = """
        query GetScheduledInfo($input: PostsInput!) {
            posts(input: $input, first: 100) { edges { node { id dueAt channelId assets { mimeType source } } } }
        }
        """
        variables = {"input": {"organizationId": organization_id, "filter": {"channelIds": [channel_id], "status": ["scheduled"]}}}
        data = self._query(query, variables)
        edges = data.get("posts", {}).get("edges", [])
        if not edges:
            return None
        return max(edges, key=lambda e: e["node"]["dueAt"])["node"]

    def get_ultimo_post_enviado(self, organization_id: str, channel_id: str) -> Optional[dict]:
        query = """
        query GetSentInfo($input: PostsInput!) {
            posts(input: $input, first: 1) { edges { node { id dueAt channelId assets { mimeType source } } } }
        }
        """
        variables = {"input": {"organizationId": organization_id, "filter": {"channelIds": [channel_id], "status": ["sent"]}}}
        data = self._query(query, variables)
        edges = data.get("posts", {}).get("edges", [])
        return edges[0]["node"] if edges else None


# ── Verificacion SSIM (identico a subida_bot.py) ────────────────────────

def descargar_video_temp(url, ruta_destino, timeout_conexion=10, timeout_lectura=30):
    resp = requests.get(url, stream=True, timeout=(timeout_conexion, timeout_lectura))
    resp.raise_for_status()
    with open(ruta_destino, "wb") as f:
        for chunk in resp.iter_content(8192):
            f.write(chunk)


def extraer_frames(ruta, n_frames=20):
    cap = cv2.VideoCapture(ruta)
    if not cap.isOpened():
        raise RuntimeError(f"No se pudo abrir el video: {ruta}")
    total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    frames = []
    for i in range(n_frames):
        pos = int(total * (i + 1) / (n_frames + 1))
        cap.set(cv2.CAP_PROP_POS_FRAMES, pos)
        ret, frame = cap.read()
        if ret:
            frame = cv2.resize(frame, (320, 180))
            frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            frames.append(frame)
    cap.release()
    return frames


def verificar_video(ruta_local, url_buffer, umbral=0.90):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
        ruta_tmp = tmp.name
    try:
        descargar_video_temp(url_buffer, ruta_tmp)
        frames_local = extraer_frames(ruta_local)
        frames_buffer = extraer_frames(ruta_tmp)
        if not frames_local or not frames_buffer:
            raise RuntimeError("no se pudieron extraer frames de uno de los dos videos")
        scores = [max(ssim(f_local, f_buf) for f_buf in frames_buffer) for f_local in frames_local]
        return (sum(scores) / len(scores)) >= umbral, sum(scores) / len(scores)
    finally:
        os.unlink(ruta_tmp)


def _confirmar_o_asumir_subido(post_node, ruta_local, cuenta_key, contexto):
    assets = post_node.get("assets") or []
    url_buffer = assets[0].get("source") if assets else None
    if not ruta_local or not os.path.exists(ruta_local) or not url_buffer:
        log.info("[%s] No se puede verificar por contenido (falta archivo o URL) -- se asume subido (%s).", cuenta_key, contexto)
        return True
    try:
        coincide, promedio = verificar_video(ruta_local, url_buffer)
        log.info("[%s] Verificacion SSIM (%s): promedio=%.3f -> %s", cuenta_key, contexto, promedio, "coincide" if coincide else "NO coincide")
        return coincide
    except Exception as e:
        log.warning("[%s] No se pudo verificar el video (%s): %s -- se asume subido.", cuenta_key, contexto, e)
        return True


def comprobacion_inicial_subido(cuenta_key: str, canal: dict, ruta_local: str) -> bool:
    api_key = obtener_api_key_buffer(canal["buffer_login"])
    if not api_key:
        raise RuntimeError(f"No se encontro el login de Buffer ({canal.get('buffer_login')}) de '{cuenta_key}' en buffer_logins.")
    client = BufferClient(api_key, canal["buffer_login"])
    ultimo_id = obtener_ultimo_video(cuenta_key)
    if not ultimo_id:
        return False

    # Igual a subida_bot.py: un id coincidente NO alcanza para asumir
    # "ya subido" -- solo cuando el id NO coincide, se confirma por
    # contenido (SSIM) antes de decidir. Si el id coincide, no hay evidencia
    # de que haya pasado nada nuevo desde la ultima vez -- se cae al `return
    # False` del final, que hace que se intente subir de verdad.
    ultimo_video_cola = client.get_ultimo_post_cola(canal["organization_id"], canal["buffer_channel_id"])
    if ultimo_video_cola:
        subido = ultimo_video_cola["id"] == ultimo_id
        if not subido:
            subido = _confirmar_o_asumir_subido(ultimo_video_cola, ruta_local, cuenta_key, "en cola")
            guardar_ultimo_video(cuenta_key, ultimo_video_cola["id"])
            if subido:
                return True
    else:
        ultimo_video_enviado = client.get_ultimo_post_enviado(canal["organization_id"], canal["buffer_channel_id"])
        if ultimo_video_enviado:
            subido = ultimo_video_enviado["id"] == ultimo_id
            if not subido:
                subido = _confirmar_o_asumir_subido(ultimo_video_enviado, ruta_local, cuenta_key, "enviado")
                guardar_ultimo_video(cuenta_key, ultimo_video_enviado["id"])
                if subido:
                    return True
    return False


def comprobacion_subido(cuenta_key: str, canal: dict, posts_antes: set) -> bool:
    api_key = obtener_api_key_buffer(canal["buffer_login"])
    client = BufferClient(api_key, canal["buffer_login"])
    post_actuales = client.get_scheduled_posts(canal["organization_id"], canal["buffer_channel_id"])
    nuevos = post_actuales - posts_antes
    if nuevos:
        guardar_ultimo_video(cuenta_key, nuevos.pop())
        return True
    return False


# ── Interaccion con la pagina (headless, click real de "Schedule Post") ─

def _shot(page, cuenta_key, sufijo) -> Optional[str]:
    """Screenshot a disco (para diagnostico sin pantalla) + subido a B2. No
    debe nunca hacer explotar el flujo principal -- si falla, solo se loguea."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    nombre = f"{ts}_{cuenta_key}_{sufijo}.png"
    ruta_local = os.path.join(CARPETA_SCREENSHOTS, nombre)
    try:
        page.screenshot(path=ruta_local, full_page=True)
    except Exception as e:
        log.warning("No se pudo tomar screenshot (%s): %s", sufijo, e)
        return None
    try:
        b2_client.upload_file(ruta_local, B2_BUCKET, f"logs/screenshots/{nombre}")
    except Exception as e:
        log.warning("No se pudo subir el screenshot a B2: %s", e)
    return ruta_local


def _cerrar_tooltip_promocional(page, cuenta_key: str):
    """Buffer muestra a veces, anclado ABAJO en la barra lateral de canales
    (justo debajo del ultimo canal listado), un tooltip promocional de
    'nueva funcion' (ej. 'New: Filter Instagram & Facebook Posts by Post
    Type' -- el texto cambia segun la funcion que este promocionando en
    cada momento, no hay que buscar por ese texto especifico). Confirmado
    en produccion 2026-09-03 (perfil8/perfil9/perfil11/perfil12: presente
    en TODOS los canales revisados, no es un caso raro) -- en perfiles con
    varios canales conectados, si la lista de canales es lo bastante larga
    este tooltip tapa el/los ultimos canales de la barra lateral
    (`a[data-sidebar="button"]`, el mismo selector que usa
    _cerrar_popup_racha para el popup de racha semanal), rompiendo con
    timeout el click a ese canal -- mismo sintoma que el popup de racha,
    pero un popup distinto, con su propio disparador (no es semanal, esta
    atado a que exista una novedad de producto para promocionar).

    A diferencia del popup de racha (que se cierra clickeando un punto
    neutro de la pagina), este tiene un boton de cerrar real en su propio
    DOM: un <button> con un texto interno 'Dismiss' (visualmente oculto,
    solo aparece al hacer hover con el mouse -- confirmado con screenshot
    real que el hover revela una X arriba a la derecha del tooltip -- pero
    Playwright lo puede clickear directo por su nombre accesible sin
    necesitar el hover real, confirmado en produccion). Se busca por rol
    accesible ('Dismiss') en vez de por clase CSS porque las clases de
    Buffer son hashes que cambian en cada deploy de su frontend
    (`publish_closeButton_t-m3Y` al momento de escribir esto)."""
    try:
        boton_cerrar = page.get_by_role("button", name="Dismiss")
        if boton_cerrar.count() == 0:
            return
        log.info("[%s] Tooltip promocional de Buffer detectado en la sidebar, cerrando...", cuenta_key)
        boton_cerrar.first.click(timeout=3000)
        boton_cerrar.first.wait_for(state="hidden", timeout=3000)
    except Exception as e:
        log.warning("[%s] No se pudo cerrar el tooltip promocional (se continua igual): %s", cuenta_key, e)


def _cerrar_aviso_canal_desconectado(page, cuenta_key: str):
    """Cuando un LOGIN de Buffer tiene mas de un canal conectado y alguno de
    ELLOS (no necesariamente el que se esta procesando ahora) perdio la
    conexion (ej. un canal de TikTok inactivo/no usado, `is_disconnected` en
    `canales_config`), Buffer muestra un dialog modal a nivel de
    ORGANIZACION -- 'We're having trouble connecting to your channel /
    Refresh the connection to keep your posts on track' -- que aparece
    tambien al entrar a un canal DISTINTO de esa misma organizacion que si
    esta conectado y funcionando bien. Confirmado en produccion 2026-09-03
    (organization_id 69933183f3bb6a4b57e3eb6a: 'ardilla.epics.clip'
    desconectado e inactivo en canales_config, pero el aviso tapaba tambien
    el boton 'New Post' de 'kou.recaps', el canal de esa misma cuenta que SI
    esta activo y recibe videos) -- coincide con el sintoma reportado por el
    usuario de que ese perfil fallaba siempre en los primeros intentos.

    El boton de cerrar tiene un texto interno 'Close' (nombre accesible,
    igual de criterio que 'Dismiss' en _cerrar_tooltip_promocional). Se
    busca primero el texto del aviso para no clickear un boton 'Close'
    ajeno a este dialog si Buffer reusa ese mismo texto en otro lado."""
    try:
        aviso = page.get_by_text(re.compile("trouble connecting to your channel", re.I))
        if aviso.count() == 0:
            return
        log.info("[%s] Aviso de canal desconectado (de OTRO canal de la misma organizacion) detectado, cerrando...", cuenta_key)
        boton_cerrar = page.get_by_role("button", name="Close")
        if boton_cerrar.count() > 0:
            boton_cerrar.first.click(timeout=3000)
            aviso.first.wait_for(state="hidden", timeout=3000)
    except Exception as e:
        log.warning("[%s] No se pudo cerrar el aviso de canal desconectado (se continua igual): %s", cuenta_key, e)


def _descartar_post_sin_terminar(page, cuenta_key: str):
    """Si un intento anterior se corto a mitad del composer (ej. timeout
    escribiendo la descripcion), Buffer guarda ese borrador y, al abrir el
    composer de nuevo, muestra un alertdialog 'Resume Unfinished Post? /
    You have an unfinished post from a previous session' con botones
    'Discard' / 'Resume Post'. Tapa todo el composer, asi que el click al
    editor fallaba con timeout en TODOS los reintentos (bug real 2026-10-22,
    video_226: 16 cuentas detenidas, capturas *_error.png de ese dia).

    Se llama justo despues de que aparece el composer (desde
    _subir_video_pagina). El dialog llega un instante despues que el editor, por
    eso se espera hasta 3 s a que aparezca en vez de un count() inmediato.
    Siempre 'Discard': la descripcion y el video se cargan de cero en este
    intento, restaurar el borrador duplicaria el texto."""
    try:
        aviso = page.get_by_text(re.compile("unfinished post from a previous session", re.I))
        try:
            aviso.first.wait_for(state="visible", timeout=3000)
        except Exception:
            return
        log.info("[%s] Dialog 'Resume Unfinished Post?' detectado, descartando borrador viejo...", cuenta_key)
        page.get_by_role("alertdialog").get_by_role("button", name="Discard").first.click(timeout=5000)
        aviso.first.wait_for(state="hidden", timeout=5000)
    except Exception as e:
        log.warning("[%s] No se pudo descartar el post sin terminar (se continua igual): %s", cuenta_key, e)


def _cerrar_popup_racha(page, cuenta_key: str):
    """Buffer muestra a veces (una vez por semana, cerca del domingo -- que
    es cuando se reinicia la 'racha' semanal de posts) un popover anclado al
    icono de flama arriba a la izquierda: 'N week streak - At risk -
    Schedule a post before Sunday to keep your streak alive!'. Se abre solo
    al cargar la pagina y NO se cierra solo -- tapa el selector de canal de
    la barra lateral (`a[data-sidebar="button"]`, usado mas abajo cuando
    num_channels>1), lo que hacia fallar con timeout el click a ese selector
    (confirmado 2026-08-16 con screenshot real de un fallo en produccion:
    20260815T230313Z_kougamingclips_error.png). Como varias cuentas del
    mismo perfil se procesan en secuencia sobre la MISMA pagina
    (subir_videos_perfil), un popup sin cerrar arrastraba el fallo a todas
    las cuentas de ese perfil en la misma corrida.

    Se cierra clickeando un punto neutro de la pagina (sin pisar ningun
    boton) -- igual que el workaround manual que ya se hacia a mano."""
    try:
        popup = page.get_by_text(re.compile("keep your streak alive", re.I))
        if popup.count() == 0:
            return
        log.info("[%s] Popup de racha semanal de Buffer detectado, cerrando...", cuenta_key)
        page.mouse.click(700, 400)
        popup.first.wait_for(state="hidden", timeout=3000)
    except Exception as e:
        log.warning("[%s] No se pudo cerrar el popup de racha semanal (se continua igual): %s", cuenta_key, e)


def _pantalla_de_login(page) -> bool:
    """Si la sesion persistida del perfil de Firefox ya caduco, Buffer
    redirige `publish.buffer.com/all-channels` a una pantalla de login en
    vez de mostrar la pagina pedida. Confirmado en produccion 2026-08-27
    (perfil9, sesion real ya caducada) que el dominio de esa redireccion NO
    es siempre el mismo: una sesion sin ningun cookie previo cae en
    `login.buffer.com/login`, pero una sesion que SI tenia cookies pero ya
    vencio cae en `auth.buffer.com/login/<token>` -- mismo formulario
    (`input#email`/`input#password`), dominio distinto. Por eso el chequeo
    es por el formulario en si, no por el dominio."""
    return page.locator("input#email").count() > 0 and page.locator("input#password").count() > 0


# Tiempo total que se espera a que Buffer termine de aceptar el login
# despues de que el chequeo rapido (ver abajo) ya determino que no paso de
# una -- pensado para dejarle al usuario tiempo real de entrar a su correo y
# confirmar el login (2026-08-27, a pedido explicito: "3 minutos" aprox).
ESPERA_VERIFICACION_LOGIN_MS = int(os.getenv("ESPERA_VERIFICACION_LOGIN_MS", str(3 * 60 * 1000)))
# Ventana corta para el caso comun (el dispositivo ya era de confianza, no
# hace falta ni avisar por Telegram ni esperar los 3 minutos completos).
_ESPERA_CHEQUEO_RAPIDO_LOGIN_MS = 8000


def _notificar_telegram_inmediato(texto: str):
    """Aviso directo a la API de Telegram, SIN pasar por el agrupamiento de
    reportes.reportar_resultado_subida() (ese espera a que termine de
    procesar TODAS las cuentas del video, y encima puede suprimir el aviso
    si ya hubo uno para el mismo video en las ultimas 12h -- ver
    ESTADOS_AGRUPABLES). Hace falta que este sea inmediato porque el usuario
    tiene una ventana real y acotada de tiempo (ESPERA_VERIFICACION_LOGIN_MS)
    para entrar a su correo y confirmar el login antes de que se de por
    vencido. Mismo criterio de "cada modulo manda su Telegram por su cuenta"
    que ya existia entre handler.notificar_error() y
    reportes._enviar_telegram_mensaje() -- no comparten codigo entre si."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": texto},
            timeout=15,
        )
    except Exception as e:
        log.warning("No se pudo enviar el aviso inmediato de Telegram: %s", e)


def _marcar_perfil_bloqueado_por_login(perfil: str, cuenta_key: str):
    """Se llama cuando _intentar_login_buffer() confirma (tras esperar
    ESPERA_VERIFICACION_LOGIN_MS completo) que el login de un perfil no se
    pudo completar solo. Marca como agotadas (intentos=MAX_INTENTOS_SUBIDA)
    las filas de `cola_subida` de TODAS las cuentas que comparten este mismo
    perfil de Firefox (`cuentas.buffer`), no solo la que se estaba probando
    -- todas comparten el mismo login de Buffer, asi que todas van a chocar
    con el mismo muro. Sin esto, cada cuenta del perfil gastaria sus propios
    3 intentos (cada uno con su propia espera de hasta 3 minutos) contra un
    problema que ya sabemos que no se va a resolver solo en esta corrida.
    Idempotente: si una fila ya estaba en MAX_INTENTOS_SUBIDA, no cambia
    nada en la practica."""
    try:
        cuentas_del_perfil = [
            fila["cuenta"] for fila in supabase.table("cuentas").select("cuenta").eq("buffer", perfil).execute().data
        ]
    except Exception as e:
        log.error("[%s] No se pudo listar las cuentas del perfil para marcarlas bloqueadas: %s", perfil, e)
        return
    if not cuentas_del_perfil:
        return
    try:
        supabase.table("cola_subida").update({"intentos": MAX_INTENTOS_SUBIDA}).in_("cuenta", cuentas_del_perfil).execute()
        log.warning(
            "[%s] Perfil bloqueado por login -- intentos puestos en %d para no perder tiempo reintentando: %s",
            perfil, MAX_INTENTOS_SUBIDA, ", ".join(cuentas_del_perfil),
        )
    except Exception as e:
        log.error("[%s] No se pudo marcar las cuentas del perfil como bloqueadas: %s", perfil, e)


def _intentar_login_buffer(page, login: str, cuenta_key: str, perfil: str) -> Optional[str]:
    """Reloguea con email+password guardados en buffer_logins (columna
    'password') cuando _pantalla_de_login() detecto que la sesion caduco.
    Reemplaza la sincronizacion de perfiles de Firefox via B2 que existia
    antes (sincronizar_perfiles_firefox(), removida 2026-08-27): ya no hace
    falta que alguien loguee a mano en una PC y suba el perfil -- el propio
    VPS se relogea solo cuando hace falta.

    El formulario tiene 'input#email'/'input#password' y un boton submit
    (texto 'Log In' o 'Continue' segun el dominio -- ver _pantalla_de_login,
    por eso se selecciona por `type="submit"` y no por texto). Puede
    aparecer tapado por el banner de cookies -- se intenta cerrar con
    'Accept All', best-effort, igual que _cerrar_popup_racha.

    Flujo de espera/aviso (agregado 2026-08-27, a pedido explicito): tras
    enviar el formulario, un chequeo RAPIDO (_ESPERA_CHEQUEO_RAPIDO_LOGIN_MS)
    cubre el caso comun (dispositivo ya de confianza, entra directo). Si eso
    no alcanza, es porque Buffer probablemente pidio verificacion extra por
    correo (dispositivo nuevo) -- indistinguible desde aca de una password
    vieja/incorrecta, en ambos casos hace falta un humano. Se avisa POR
    TELEGRAM DE INMEDIATO en ese momento (no se espera a que termine todo el
    video, ver _notificar_telegram_inmediato) para que el usuario tenga la
    ventana completa de ESPERA_VERIFICACION_LOGIN_MS para entrar a confirmar
    el correo si eso es lo que hace falta. Si pasan los 3 minutos sin que la
    pagina vuelva a publish.buffer.com, se da por perdido: marca el perfil
    entero como bloqueado (_marcar_perfil_bloqueado_por_login) y devuelve el
    error -- ya no vale la pena seguir reintentando solo en esta corrida."""
    password = obtener_password_buffer(login)
    if not password:
        return "sin_password_guardada_en_buffer_logins"

    try:
        boton_cookies = page.get_by_role("button", name="Accept All")
        if boton_cookies.count() > 0:
            boton_cookies.first.click(timeout=3000)
    except Exception:
        pass

    try:
        page.locator("input#email").fill(login, timeout=_get_timeout())
        page.locator("input#password").fill(password, timeout=_get_timeout())
        page.locator('button[type="submit"]').first.click(timeout=_get_timeout())
    except Exception as e:
        return f"login_no_completado: {e}"

    try:
        page.wait_for_url(re.compile(r"^https://publish\.buffer\.com/"), timeout=_ESPERA_CHEQUEO_RAPIDO_LOGIN_MS)
        return None
    except Exception:
        pass

    _notificar_telegram_inmediato(
        f"🔐 bot-daniel: '{cuenta_key}' (perfil {perfil}, login {login}) no pudo reloguear solo en Buffer. "
        f"Si te llego un correo de Buffer pidiendo confirmar el login, entra a confirmarlo AHORA -- "
        f"tenes unos {ESPERA_VERIFICACION_LOGIN_MS // 60000} minutos antes de que se de por vencido "
        f"y esta cuenta (y las demas del mismo perfil) queden pausadas hasta que resetees los intentos a mano."
    )

    try:
        # OJO: sin el ^https://... al principio, este regex tambien
        # "matchea" pantallas intermedias de Buffer que todavia NO
        # llegaron a publish.buffer.com pero llevan esa URL como parametro
        # (ej. login.buffer.com/verify-waiting?...&redirect=https%3A%2F%2Fpublish.buffer.com%2F...)
        # -- confirmado con un caso real (2026-08-27, prueba local): sin
        # anclar, esto daba un falso positivo de "login exitoso" estando en
        # realidad atascado esperando la verificacion por correo.
        page.wait_for_url(
            re.compile(r"^https://publish\.buffer\.com/"),
            timeout=ESPERA_VERIFICACION_LOGIN_MS - _ESPERA_CHEQUEO_RAPIDO_LOGIN_MS,
        )
        log.info("[%s] Login confirmado a tiempo tras el aviso de Telegram.", cuenta_key)
        return None
    except Exception as e:
        _marcar_perfil_bloqueado_por_login(perfil, cuenta_key)
        return f"login_no_completado: {e}"


# sync_playwright().start() no es seguro de llamar desde varios hilos a la
# vez -- si dos o mas hilos lo arrancan por primera vez en el mismo
# instante (justo lo que pasa con MAX_SUBIDAS_PARALELAS>1, varios hilos
# nuevos del ThreadPoolExecutor llamando a esto casi simultaneamente),
# Playwright tira "It looks like you are using Playwright Sync API inside
# the asyncio loop" -- confirmado en produccion 2026-07-26 (los primeros 4
# hilos de una tanda fallaban asi, los que arrancaban despues no). El
# arranque en si (`.start()`) es rapido -- serializarlo con un lock no le
# saca practicamente nada al paralelismo real (que sigue en el resto de
# `_subir_video_pagina`, la parte lenta).
_playwright_start_lock = threading.Lock()


def _abrir_pagina(perfil: str):
    """Igual que abrir_navegador() en subida_bot.py: reintenta hasta 3 veces
    si el lanzamiento del navegador falla (con 2s de pausa entre intentos)
    antes de darse por vencido -- un fallo transitorio al lanzar Firefox no
    deberia gastar de una todo un intento de subida (ver MAX_INTENTOS_SUBIDA
    en handler.py, que cuenta a nivel mas alto)."""
    if not asegurar_firefox_playwright():
        raise RuntimeError("Firefox de Playwright no disponible en este worker.")
    ultimo_error = None
    for intento in range(1, 4):
        p = None
        try:
            with _playwright_start_lock:
                p = sync_playwright().start()
            browser = p.firefox.launch_persistent_context(
                user_data_dir=os.path.join(CARPETA_FIREFOX_PROFILES, perfil),
                headless=True,
            )
            page = browser.pages[0] if browser.pages else browser.new_page()
            page.on("dialog", lambda dialog: dialog.accept())
            return p, browser, page
        except Exception as e:
            ultimo_error = e
            log.warning("[%s] Error abriendo el navegador (intento %d/3): %s", perfil, intento, e)
            if p:
                try:
                    p.stop()
                except Exception:
                    pass
            time.sleep(2)
    raise RuntimeError(f"No se pudo abrir el navegador para el perfil '{perfil}' tras 3 intentos: {ultimo_error}")


def _subir_video_pagina(page, cuenta_key: str, ruta_archivo: str, descripcion: str, titulo: Optional[str], tipo_cuenta: str, num_channels: int, login_buffer: str, perfil: str, url_canal: Optional[str] = None) -> Optional[str]:
    """Si url_canal esta seteado (viene de cuentas.url, columna agregada
    2026-09-03 -- URL directa al canal, publish.buffer.com/channels/
    {buffer_channel_id}/schedule, mismo id que ya se usaba para la API de
    Buffer): navega DIRECTO ahi, sin pasar por all-channels ni tocar la
    barra lateral en absoluto, y clickea el boton "+ New Post" de arriba a
    la derecha (data-testid=queue-header-create-post) para crear el post
    de ESE canal. Motivo del cambio (a pedido explicito del usuario): cerrar
    popups puntuales (_cerrar_popup_racha, _cerrar_tooltip_promocional) es
    un parche reactivo -- si Buffer agrega un popup nuevo que tape la barra
    lateral, hay que enterarse y corregir de nuevo. Navegando directo al
    canal ese problema desaparece de raiz: no hace falta clickear nada de
    la sidebar para llegar al canal correcto.

    Si NO hay url_canal (cuenta todavia sin backfill de la columna, o fila
    nueva en Supabase sin completar) cae al comportamiento anterior, sin
    cambios: click "New" -> "Post" (cuenta con un solo canal en su login),
    o -- si el perfil de Firefox tiene mas de un canal conectado -- primero
    click en el canal especifico en la barra lateral y despues en "crear
    post" de ESE canal (mismo criterio que subida_bot.py:subir_video(); bug
    real corregido en produccion 2026-07-26: sin esto, las cuentas que
    comparten perfil de Firefox con otros canales fallaban con timeouts
    random en pasos posteriores porque terminaban en el contexto/canal
    equivocado).

    En ambos casos: adjunta el archivo, espera el procesado, y clickea
    "Schedule Post" DE VERDAD. Devuelve None si salio bien,
    "requiere_login_manual" si la sesion caduco y el relogin automatico
    (ver _intentar_login_buffer) tampoco pudo, o un string describiendo el
    error si algo fallo (screenshot ya tomado antes de propagar)."""
    try:
        destino = url_canal if url_canal else "https://publish.buffer.com/all-channels"
        page.goto(destino, timeout=30000, wait_until="load")
        page.wait_for_timeout(2000)

        if _pantalla_de_login(page):
            log.warning("[%s] Sesion de Buffer caducada (redirigido a login.buffer.com), reintentando login...", cuenta_key)
            error_login = _intentar_login_buffer(page, login_buffer, cuenta_key, perfil)
            if error_login:
                _shot(page, cuenta_key, "login_fallido")
                log.error("[%s] No se pudo reloguear en Buffer: %s", cuenta_key, error_login)
                return "requiere_login_manual"
            log.info("[%s] Re-login en Buffer exitoso, continuando con la subida.", cuenta_key)
            page.goto(destino, timeout=30000, wait_until="load")
            page.wait_for_timeout(2000)

        _cerrar_popup_racha(page, cuenta_key)
        _cerrar_tooltip_promocional(page, cuenta_key)
        _cerrar_aviso_canal_desconectado(page, cuenta_key)

        if url_canal:
            page.wait_for_selector('[data-testid="queue-header-create-post"]', timeout=_get_timeout())
            page.locator('[data-testid="queue-header-create-post"]').click(timeout=_get_timeout())
        elif num_channels > 1:
            page.wait_for_selector('[data-sidebar="button"][data-collapsed="false"]', timeout=_get_timeout())
            loc = page.locator('a[data-sidebar="button"]').filter(
                has=page.get_by_text(cuenta_key, exact=True)
            ).first
            loc.click(timeout=_get_timeout())
            _cerrar_popup_racha(page, cuenta_key)
            _cerrar_tooltip_promocional(page, cuenta_key)
            _cerrar_aviso_canal_desconectado(page, cuenta_key)
            page.wait_for_selector('[data-testid="queue-header-create-post"]', timeout=_get_timeout())
            page.locator('[data-testid="queue-header-create-post"]').click(timeout=_get_timeout())
        else:
            page.locator('button[aria-haspopup="menu"]', has_text="New").wait_for(timeout=_get_timeout())
            page.locator('button[type="button"]', has_text="New").first.click(timeout=_get_timeout())
            page.wait_for_selector('[role="menuitem"]:has(label:text-is("Post"))', timeout=_get_timeout())
            page.locator('[role="menuitem"]:has(label:text-is("Post"))').first.click(timeout=_get_timeout())

        page.wait_for_selector('[data-testid="composer-text-area"]', timeout=_get_timeout())
        _descartar_post_sin_terminar(page, cuenta_key)
        editor = page.locator('[data-testid="composer-text-area"]')
        editor.click(timeout=_get_timeout())
        if "instagram" in (tipo_cuenta or "").lower():
            # Instagram rechaza el post con mas de 5 hashtags y la fila queda
            # reintentando hasta bloquear la cola (bug real 2026-09-30: filas
            # de cola_subida con 6-7 hashtags por tags pegados al texto,
            # "disparejo#westcol"). formatear_descripcion ya respeta el tope;
            # esto es la red de seguridad para lo que ya estaba encolado.
            recortada = limitar_hashtags(descripcion, 5)
            if recortada != descripcion:
                log.warning("[%s] Descripcion con mas de 5 hashtags, recortada para Instagram.", cuenta_key)
                descripcion = recortada
        # type() escribe letra por letra; con el timeout por defecto (30 s)
        # una descripcion de ~600 caracteres no terminaba con varios Firefox
        # en paralelo (2026-10-22, video_226: se corto a los ~400) y el video
        # nunca llegaba a adjuntarse. Margen de 150 ms por caracter + base.
        editor.type(descripcion, timeout=_get_timeout() + len(descripcion) * MS_POR_CARACTER)

        if "youtube" in (tipo_cuenta or "").lower():
            if titulo:
                campo_titulo = page.locator('input[aria-label="Video title"]')
                campo_titulo.wait_for(state="visible", timeout=_get_timeout())
                campo_titulo.fill(titulo)
            else:
                log.warning("[%s] Cuenta YouTube sin titulo guardado -- Buffer probablemente rechace el post.", cuenta_key)

        input_file = page.locator("input[type='file']")
        input_file.wait_for(state="attached", timeout=_get_timeout())
        input_file.set_input_files(ruta_archivo)

        page.wait_for_selector('[data-testid="pending-media-thumbnail"]', timeout=_get_timeout())

        viejo = None
        iguales = 0
        while True:
            pendiente = page.locator('[data-testid="pending-media-thumbnail"]')
            if pendiente.count() == 0:
                break
            progreso = page.locator('span.publish_progressText_1f6x-')
            if progreso.count() > 0:
                nuevo = progreso.inner_text()
                if nuevo == viejo:
                    iguales += 1
                else:
                    iguales = 0
                    viejo = nuevo
                if iguales >= 6:
                    return "video_atascado_subiendo"
            time.sleep(10)

        page.wait_for_selector('button:has-text("Remove video")', timeout=_get_timeout())

        boton_schedule = page.get_by_role("button", name="Schedule Post")
        if boton_schedule.count() == 0 or not boton_schedule.is_enabled():
            _shot(page, cuenta_key, "schedule_deshabilitado")
            return "boton_schedule_post_no_disponible"

        boton_schedule.click(timeout=_get_timeout())
        page.wait_for_timeout(3000)
        return None
    except Exception as e:
        _shot(page, cuenta_key, "error")
        return f"error_subiendo_video: {e}"


def _procesar_cuenta_en_pagina(page, cuenta_key: str, canal: dict, ruta_archivo: str, descripcion: str, titulo: Optional[str]) -> dict:
    """
    Logica de subida para UNA cuenta, reusando una `page` de Playwright ya
    abierta -- pensada para que varias cuentas del MISMO perfil de Firefox
    se procesen en secuencia sobre la misma pagina/navegador (ver
    subir_videos_perfil), igual que subida_de_video_definitiva() en
    subida_bot.py. NUNCA se debe llamar con paginas de DOS Playwright
    distintos abiertos sobre el mismo perfil al mismo tiempo -- Firefox
    bloquea el perfil y Playwright tira errores confusos (confirmado en
    produccion 2026-07-26, "Playwright Sync API inside the asyncio loop"
    con MAX_SUBIDAS_PARALELAS>1 y cuentas que compartian perfil).

    Devuelve {"estado": "subida_correcta"|"ya_subido"|"cola_llena"|
              "requiere_login_manual"|"error ...",
              "screenshot": ruta_local_o_None}.
    """
    if not os.path.exists(ruta_archivo):
        return {"estado": f"error archivo_no_encontrado: {ruta_archivo}", "screenshot": None}

    try:
        subido = comprobacion_inicial_subido(cuenta_key, canal, ruta_archivo)
    except Exception as e:
        log.warning("[%s] Fallo la comprobacion inicial (%s), se intenta subir igual.", cuenta_key, e)
        subido = False
    if subido:
        return {"estado": "ya_subido", "screenshot": None}

    api_key = obtener_api_key_buffer(canal["buffer_login"])
    if not api_key:
        return {"estado": f"error no se encontro la cuenta de Buffer (login {canal.get('buffer_login')}) en buffer_logins", "screenshot": None}
    client = BufferClient(api_key, canal["buffer_login"])

    # Mismo chequeo que posts()/procesar_cuenta() en subida_bot.py: no
    # intentar subir si la cola de Buffer de este canal ya esta en el tope
    # del plan (10 en el plan gratuito) -- evita gastar tiempo en una
    # subida que Buffer va a rechazar de entrada.
    try:
        posts_antes = client.get_scheduled_posts(canal["organization_id"], canal["buffer_channel_id"])
    except Exception as e:
        return {"estado": f"error consulta_cola: {e}", "screenshot": None}
    if len(posts_antes) >= MAX_QUEUE_PER_CHANNEL:
        log.info("[%s] Cola llena (%d/%d), no se intenta subir.", cuenta_key, len(posts_antes), MAX_QUEUE_PER_CHANNEL)
        return {"estado": "cola_llena", "screenshot": None}

    # Cuantos canales de Buffer tiene conectados este LOGIN (no esta cuenta
    # sola) -- si es mas de uno, subir_video() en subida_bot.py necesita
    # clickear primero el canal especifico en la barra lateral antes de
    # crear el post (ver _subir_video_pagina). Mismo criterio que
    # posts()/subida_bot.py.
    try:
        channels = client.get_channels(canal["organization_id"])
        num_channels = len(channels)
    except Exception as e:
        return {"estado": f"error consulta_canales: {e}", "screenshot": None}

    ultimo_error = None
    for intento in range(1, 4):
        log.info("[%s] Subiendo (intento %d/3)...", cuenta_key, intento)
        error = _subir_video_pagina(page, cuenta_key, ruta_archivo, descripcion, titulo, canal.get("tipo", ""), num_channels, canal.get("buffer_login"), canal.get("buffer"), url_canal=canal.get("url"))
        if error == "requiere_login_manual":
            # No tiene sentido reintentar dentro de esta misma corrida --
            # si el password guardado esta mal o Buffer pidio verificacion
            # extra, un segundo intento inmediato falla exactamente igual.
            # Tratado como "benigno" (no cuenta como intento fallido de
            # cola_subida, se reintenta solo en el proximo ciclo) igual que
            # cola_llena -- ver _procesar_resultado_cuenta en handler.py.
            return {"estado": "requiere_login_manual", "screenshot": None}
        if error:
            ultimo_error = error
            time.sleep(2)
            continue
        if comprobacion_subido(cuenta_key, canal, posts_antes):
            return {"estado": "subida_correcta", "screenshot": None}
        ultimo_error = "no_se_confirmo_en_buffer_tras_subir"
        time.sleep(2)

    screenshot = _shot(page, cuenta_key, "fallo_final")
    return {"estado": f"error {ultimo_error}", "screenshot": screenshot}


def subir_video_buffer(cuenta_key: str, ruta_archivo: str, descripcion: str, titulo: Optional[str] = None) -> dict:
    """
    Punto de entrada para UNA sola cuenta -- abre y cierra su propio
    Playwright/Firefox para el perfil de esa cuenta. Si vas a subir varias
    cuentas que comparten el MISMO perfil de Firefox, usa
    `subir_videos_perfil()` en su lugar: llamar a esta funcion en paralelo
    (ej. un hilo por cuenta) para cuentas del mismo perfil abre dos Firefox
    sobre el mismo directorio de perfil a la vez, lo cual falla (ver
    docstring de _procesar_cuenta_en_pagina).

    Devuelve {"estado": "subida_correcta"|"ya_subido"|"error ...",
              "screenshot": ruta_local_o_None}.
    """
    canal = obtener_canal(cuenta_key)
    if not canal:
        return {"estado": f"error no hay datos de canal para '{cuenta_key}' en Supabase", "screenshot": None}
    perfil = canal.get("buffer")
    if not perfil:
        return {"estado": f"error '{cuenta_key}' no tiene perfil de Firefox asignado", "screenshot": None}
    if not os.path.isdir(os.path.join(CARPETA_FIREFOX_PROFILES, perfil)):
        return {"estado": f"error perfil '{perfil}' no existe en {CARPETA_FIREFOX_PROFILES} de este worker", "screenshot": None}

    p = browser = None
    _incrementar_perfiles_activos()
    try:
        p, browser, page = _abrir_pagina(perfil)
        return _procesar_cuenta_en_pagina(page, cuenta_key, canal, ruta_archivo, descripcion, titulo)
    except Exception as e:
        log.exception("[%s] Error no controlado subiendo a Buffer", cuenta_key)
        return {"estado": f"error_excepcion: {e}", "screenshot": None}
    finally:
        try:
            if browser:
                browser.close()
        except Exception:
            pass
        try:
            if p:
                p.stop()
        except Exception:
            pass
        _decrementar_perfiles_activos()


class SesionPerfil:
    """
    Un UNICO Firefox headless abierto sobre el perfil `perfil`, reutilizable
    para subir VARIOS videos de VARIAS cuentas de ese perfil, uno tras otro,
    sin cerrar y reabrir el navegador entre medio (2026-10-09, a pedido del
    usuario: asi funcionaba procesar_cuenta() en subida_bot.py y se habia
    perdido al portar la subida al VPS -- se subia un solo video por cuenta
    por ciclo, abriendo un Firefox nuevo cada vez).

    La usa handler._subir_cola_de_perfil, que decide QUE subir y en que
    orden (y vuelve a mirar cola_subida antes de cerrar); esta clase solo
    maneja el navegador. El navegador se abre recien en el primer `subir()`
    (si no hay nada que subir, no se abre). Usar SIEMPRE con `cerrar()` en
    un finally. Nunca crear dos sesiones del mismo perfil a la vez (ver
    docstring de _procesar_cuenta_en_pagina).
    """

    def __init__(self, perfil: str):
        self.perfil = perfil
        self._p = self._browser = self._page = None
        self._error_apertura = None
        # Si una cuenta confirma que el login de Buffer esta bloqueado, todas
        # las demas de este perfil van a chocar con el mismo muro -- no se
        # vuelve a intentar dentro de esta sesion (cada intento repetiria la
        # espera de ESPERA_VERIFICACION_LOGIN_MS). cola_subida ya queda marcada
        # por _marcar_perfil_bloqueado_por_login.
        self._bloqueado_por_login = False

    def _asegurar_abierta(self) -> Optional[str]:
        """Abre el navegador la primera vez. Devuelve un estado de error si no
        se pudo abrir (y lo recuerda, para no reintentar la apertura por cada
        video de la misma sesion)."""
        if self._page is not None:
            return None
        if self._error_apertura:
            return self._error_apertura
        if not os.path.isdir(os.path.join(CARPETA_FIREFOX_PROFILES, self.perfil)):
            self._error_apertura = f"error perfil '{self.perfil}' no existe en {CARPETA_FIREFOX_PROFILES} de este worker"
            return self._error_apertura
        _incrementar_perfiles_activos()
        try:
            self._p, self._browser, self._page = _abrir_pagina(self.perfil)
        except Exception as e:
            log.exception("[%s] No se pudo abrir el perfil de Firefox", self.perfil)
            _decrementar_perfiles_activos()
            self._error_apertura = f"error_excepcion: {e}"
            return self._error_apertura
        return None

    def subir(self, cuenta_key: str, ruta_archivo: str, descripcion: str, titulo: Optional[str] = None) -> dict:
        """Sube UN video para UNA cuenta de este perfil. Devuelve {"estado",
        "screenshot"} igual que _procesar_cuenta_en_pagina."""
        if self._bloqueado_por_login:
            return {"estado": "requiere_login_manual", "screenshot": None}
        error = self._asegurar_abierta()
        if error:
            return {"estado": error, "screenshot": None}
        canal = obtener_canal(cuenta_key)
        if not canal:
            return {"estado": f"error no hay datos de canal para '{cuenta_key}' en Supabase", "screenshot": None}
        try:
            resultado = _procesar_cuenta_en_pagina(self._page, cuenta_key, canal, ruta_archivo, descripcion, titulo)
        except Exception as e:
            log.exception("[%s] Error no controlado subiendo a Buffer", cuenta_key)
            resultado = {"estado": f"error_excepcion: {e}", "screenshot": None}
        if resultado.get("estado") == "requiere_login_manual":
            self._bloqueado_por_login = True
        return resultado

    def cerrar(self):
        if self._page is None:
            return
        try:
            if self._browser:
                self._browser.close()
        except Exception:
            pass
        try:
            if self._p:
                self._p.stop()
        except Exception:
            pass
        self._p = self._browser = self._page = None
        _decrementar_perfiles_activos()


def subir_videos_perfil(perfil: str, entradas: list) -> list:
    """
    Sube una lista fija de `entradas` ({"cuenta_key", "ruta_archivo",
    "descripcion", "titulo"}) del perfil `perfil` en una sola SesionPerfil.
    Ya NO la usa handler.py (desde 2026-10-09 usa SesionPerfil directo, ver
    handler._subir_cola_de_perfil); queda para test_buffer_paralelo.py.
    Devuelve [{"cuenta_key", "estado", "screenshot"}] en el mismo orden.
    """
    sesion = SesionPerfil(perfil)
    try:
        return [
            {"cuenta_key": e["cuenta_key"], **sesion.subir(e["cuenta_key"], e["ruta_archivo"], e["descripcion"], e.get("titulo"))}
            for e in entradas
        ]
    finally:
        sesion.cerrar()
