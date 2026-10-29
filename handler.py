"""
**Copia integrada (rama cpu-integrado, 2026-07-25)**: ademas de procesar
(descargar/renderizar), este worker tambien SUBE a Buffer de una, apenas
termina de procesar cada video -- ya no depende de que un humano corra
subida_bot.py en una PC para el paso de subida. Cambios sobre la version
"cpu" original:
  - `handler()` procesa como maximo UN video pendiente por invocacion (antes
    drenaba hasta BATCH_LIMIT=5 de un tiron) -- para dejar lugar a que cada
    ciclo tambien haga la subida a Buffer de ese video antes de volver a
    dormir (`WORKER_POLL_INTERVAL_SECS`, 300s por defecto).
  - `procesar_un_video_completo()`/`procesar_reintentos_fallidos()` solo
    ENCOLAN en `cola_subida`; la subida la hace `subir_cola_subida()` al
    final de cada ciclo: un Firefox por perfil que sube los videos de cada
    cuenta uno tras otro, en orden, y re-mira la cola antes de cerrar
    (2026-10-09; antes se subia un solo video por cuenta por ciclo).
  - `buffer_uploader.py` (modulo nuevo): la logica de Buffer portada de
    `subida_bot.py`, headless, con "Schedule Post" real.
  - `reportes.py` (modulo nuevo): reemplaza la ventana-resumen y el log de
    sesion de subida_bot.py -- mensaje de Telegram (+ screenshot de cada
    error) y el mismo log estructurado en B2 (`logs/historial_subidas.log`)
    para que "Historial de subidas" siga funcionando igual.
  - Detalle completo de como se armo/probo esto en `PASOS.md` y
    `REGISTRO_DE_PROGRESO.md` de `subir_videos/` (Drive).

Worker de RunPod Serverless -- procesa videos pendientes de Supabase.

Reutiliza la misma logica ya probada en procesador.py (pensado para un Pod
manual): descarga el video fuente (TikTok via yt-dlp+cookies, o Google Drive),
lo convierte con ffmpeg NVENC, genera una copia por cada cuenta TikTok, sube
todo a Backblaze B2, y actualiza `videos`/`video_renders` en Supabase.

Se invoca una vez por "job" via la API de RunPod (sin necesitar input) y drena
todo lo que este 'pendiente' en ese momento. No hay loop de polling ni
auto-apagado: RunPod apaga el worker solo cuando el handler retorna, que es
todo el motivo de usar Serverless en vez de un Pod.

IMPORTANTE -- concurrency_modifier: el SDK de RunPod (runpod-python) tiene un
bug conocido en su endpoint legacy de fetch de jobs (/job-take/, usado cuando
num_jobs=1): si el backend devuelve un dict sin 'id'/'input', tira una
excepcion no manejada que cuelga el worker entero sin nunca llamar al handler
(confirmado en logs: "Failed to get job... Job has missing field(s): id or
input."). Pasar concurrency_modifier fuerza num_jobs>1, que usa el endpoint
batch (/job-take-batch/) y evita ese codepath. Sin esto, TODAS las requests
fallan con "job timed out after 1 retries" pase lo que pase en el handler.

Secretos (Supabase, B2, credenciales de Google) se pasan como variables de
entorno del endpoint de RunPod -- nunca quedan en esta imagen ni en el repo.
Los stickers si van horneados en la imagen (no son secretos).
"""
import os
import re
import json
import time
import base64
import random
import shutil
import logging
import threading
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import requests
import runpod
from yt_dlp import YoutubeDL
from supabase import create_client
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from comun import nombre_tabla_seguro, formatear_descripcion
from config_render import FILTROS, BITRATES, CORTES, SCOPES, VIDEOS_FOLDER_ID
import buffer_uploader
import reportes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("handler")

WORKDIR = "/tmp/procesador"
DESCARGAS_DIR = os.path.join(WORKDIR, "videos_descargados")
TOKEN_PATH = os.path.join(WORKDIR, "token.json")
CREDENTIALS_PATH = os.path.join(WORKDIR, "credentials.json")
COOKIES_PATH = os.path.join(WORKDIR, "cookies.txt")
STICKERS_DIR = "/app/stickers"

FFMPEG = os.getenv("FFMPEG_BIN", "ffmpeg")
FFPROBE = os.getenv("FFPROBE_BIN", "ffprobe")
BATCH_LIMIT = int(os.getenv("BATCH_LIMIT", "5"))

# Cuantos renders por cuenta se procesan en paralelo: segun nucleos de CPU
# disponibles en el worker que nos toque (varia por maquina), con un tope
# maximo -- NVENC en las GPUs profesionales no tiene limite de sesiones, pero
# cuando cae a libx264 (software) el limite real es la CPU, no la GPU.
MAX_RENDERS_PARALELOS = int(os.getenv("MAX_RENDERS_PARALELOS", str(min(os.cpu_count() or 4, 8))))

# Cuantas veces se reintenta automaticamente un render individual que fallo
# (en el siguiente ciclo del worker) antes de dejarlo en error definitivo y
# requerir intervencion manual.
MAX_INTENTOS_RENDER = int(os.getenv("MAX_INTENTOS_RENDER", "3"))

# Cuantas subidas a Buffer (una por cuenta) corren en paralelo, cada una con
# su propio Firefox headless. A diferencia de MAX_RENDERS_PARALELOS (ffmpeg,
# CPU-bound -- ahi si hace falta 1 en este VPS), varios navegadores headless
# en paralelo es un patron ya probado en subida_bot.py (un hilo por perfil)
# -- se prueba en 4 a pedido del usuario (2026-07-25); si el VPS no lo
# banca, cambiar a 1 (secuencial) via esta misma variable de entorno.
MAX_SUBIDAS_PARALELAS = int(os.getenv("MAX_SUBIDAS_PARALELAS", "4"))

# Cuantas veces se reintenta automaticamente una subida a Buffer que fallo
# (una fila de cola_subida, un intento por ciclo del worker) antes de dejarla
# en pausa y avisar por Telegram -- mismo criterio que MAX_INTENTOS_RENDER,
# para no mandar una notificacion cada 5 minutos por el mismo error. Una vez
# alcanzado el limite, esa fila deja de intentarse sola hasta resetear
# 'intentos' a mano en cola_subida (Supabase).
MAX_INTENTOS_SUBIDA = int(os.getenv("MAX_INTENTOS_SUBIDA", "3"))

# Tope de vueltas de _subir_cola_de_perfil dentro de UNA sesion de navegador
# (2026-10-09). Cada vuelta re-consulta cola_subida antes de cerrar Firefox
# por si entraron videos nuevos mientras subia; el tope es solo una red de
# seguridad contra un loop inesperado.
MAX_VUELTAS_SESION = int(os.getenv("MAX_VUELTAS_SESION", "10"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
B2_KEY_ID = os.environ["B2_KEY_ID"]
B2_APP_KEY = os.environ["B2_APP_KEY"]
B2_BUCKET = os.environ["B2_BUCKET"]
B2_ENDPOINT = os.environ["B2_ENDPOINT"]
B2_REGION = os.environ["B2_REGION"]

for _dir in (WORKDIR, DESCARGAS_DIR):
    os.makedirs(_dir, exist_ok=True)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    region_name=B2_REGION,
    aws_access_key_id=B2_KEY_ID,
    aws_secret_access_key=B2_APP_KEY,
)

cuentas_dic: dict = {}


def _escribir_credenciales_drive():
    """Las credenciales de Google no van horneadas en la imagen -- llegan en base64
    por variables de entorno (GOOGLE_TOKEN_JSON_B64 / GOOGLE_CREDENTIALS_JSON_B64)
    y se escriben a disco una sola vez por worker frio."""
    token_b64 = os.getenv("GOOGLE_TOKEN_JSON_B64")
    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_JSON_B64")
    if token_b64 and not os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, "wb") as f:
            f.write(base64.b64decode(token_b64))
    if creds_b64 and not os.path.exists(CREDENTIALS_PATH):
        with open(CREDENTIALS_PATH, "wb") as f:
            f.write(base64.b64decode(creds_b64))


def _escribir_cookies():
    cookies_b64 = os.getenv("TIKTOK_COOKIES_TXT_B64")
    if cookies_b64 and not os.path.exists(COOKIES_PATH):
        with open(COOKIES_PATH, "wb") as f:
            f.write(base64.b64decode(cookies_b64))


def cargar_cuentas():
    global cuentas_dic
    cuentas = supabase.table("cuentas").select("*").execute().data
    cuentas_dic = {d["cuenta"]: {k: v for k, v in d.items() if k != "cuenta"} for d in cuentas}
    log.info("Cuentas cargadas: %d", len(cuentas_dic))


def notificar_error(texto):
    """Avisa por Telegram cuando pasa un error real (no un fallo transitorio
    que ya se va a reintentar solo). Silencioso si no hay token/chat_id
    configurados -- no interrumpe el procesado si Telegram falla."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": f"🔴 bot-daniel: {texto}"},
            timeout=10,
        )
    except Exception as e:
        log.warning("No se pudo notificar por Telegram: %s", e)


def actualizar_progreso(video=None, cuenta=None, etapa="idle", progreso=0):
    """
    Escribe el estado actual en la fila unica de `progreso_actual`, para que
    subida_bot.py pueda mostrar que esta pasando ahora mismo sin necesitar
    acceso al VPS. Como MAX_RENDERS_PARALELOS=1 en la practica, alcanza con
    una sola fila que se va sobreescribiendo. Nunca debe interrumpir el
    procesado real si falla (solo es para mostrar progreso).
    """
    try:
        supabase.table("progreso_actual").update({
            "video": video,
            "cuenta": cuenta,
            "etapa": etapa,
            "progreso": progreso,
            "actualizado_en": datetime.now(timezone.utc).isoformat(),
        }).eq("id", 1).execute()
    except Exception as e:
        log.warning("No se pudo actualizar progreso_actual: %s", e)


def obtener_duracion_video(ruta):
    try:
        resultado = subprocess.run(
            [FFPROBE, "-v", "quiet", "-print_format", "json", "-show_format", ruta],
            capture_output=True, text=True, timeout=30,
        )
        info = json.loads(resultado.stdout)
        return float(info["format"]["duration"])
    except Exception as e:
        log.warning("No se pudo obtener la duracion de %s: %s", ruta, e)
        return None


def obtener_fps_video(ruta):
    try:
        resultado = subprocess.run(
            [FFPROBE, "-v", "quiet", "-print_format", "json", "-show_streams", ruta],
            capture_output=True, text=True, timeout=30,
        )
        info = json.loads(resultado.stdout)
        video_stream = next((s for s in info.get("streams", []) if s.get("codec_type") == "video"), None)
        if not video_stream:
            return None
        num, _, den = video_stream["r_frame_rate"].partition("/")
        den = float(den) if den else 1.0
        return float(num) / den if den else None
    except Exception as e:
        log.warning("No se pudo obtener el fps de %s: %s", ruta, e)
        return None


def _parsear_out_time(valor):
    """Convierte el 'out_time=HH:MM:SS.microsegundos' de ffmpeg -progress a segundos."""
    try:
        h, m, s = valor.strip().split(":")
        return int(h) * 3600 + int(m) * 60 + float(s)
    except Exception:
        return None


class _ResultadoFFmpeg:
    def __init__(self, returncode, stderr):
        self.returncode = returncode
        self.stderr = stderr


def ejecutar_ffmpeg_con_progreso(cmd, duracion_seg, on_progress=None, timeout=300):
    """
    Corre un comando de ffmpeg (debe incluir '-progress pipe:1 -nostats')
    leyendo su salida mientras corre en vez de bloquear hasta el final, y
    llama on_progress(pct) cada vez que avanza al menos un 5%. Devuelve un
    objeto con .returncode/.stderr, como subprocess.run, para no tener que
    cambiar el codigo que ya revisa esos campos.

    Nota sobre el timeout: solo se puede chequear entre lineas de salida --
    si ffmpeg se cuelga sin producir NINGUNA salida, este timeout no
    reacciona (mismo riesgo que ya existia antes en el render por cuenta, que
    no tenia ningun timeout puesto).
    """
    inicio = time.time()
    proceso = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    ultimo_reportado = -10
    lineas_relevantes = []
    prefijos_progreso = ("frame=", "fps=", "stream_", "bitrate=", "total_size=", "out_time_ms=", "out_time_us=", "out_time=", "dup_frames=", "drop_frames=", "speed=", "progress=")
    try:
        for linea in proceso.stdout:
            if time.time() - inicio > timeout:
                proceso.kill()
                proceso.wait()
                return _ResultadoFFmpeg(-1, "timeout esperando progreso de ffmpeg")
            linea = linea.rstrip("\n")
            if linea.startswith("out_time=") and duracion_seg:
                segundos = _parsear_out_time(linea.split("=", 1)[1])
                if segundos is not None:
                    pct = max(0, min(100, int(segundos / duracion_seg * 100)))
                    if on_progress and pct >= ultimo_reportado + 5:
                        on_progress(pct)
                        ultimo_reportado = pct
            elif not linea.startswith(prefijos_progreso):
                lineas_relevantes.append(linea)
        proceso.wait(timeout=max(1, timeout - (time.time() - inicio)))
    except subprocess.TimeoutExpired:
        proceso.kill()
        proceso.wait()
        return _ResultadoFFmpeg(-1, "timeout")
    return _ResultadoFFmpeg(proceso.returncode, "\n".join(lineas_relevantes[-50:]))


def listar_stickers():
    stickers = sorted(str(p) for p in Path(STICKERS_DIR).glob("*") if p.is_file())
    if not stickers:
        raise RuntimeError(f"Faltan stickers horneados en la imagen ({STICKERS_DIR}).")
    return stickers


# ── Google Drive (solo para videos fuente que no vienen de TikTok) ──────

def get_credentials():
    if not os.path.exists(TOKEN_PATH):
        raise RuntimeError(
            f"No existe {TOKEN_PATH}. Configura GOOGLE_TOKEN_JSON_B64 en las variables "
            "de entorno del endpoint (token.json en base64)."
        )
    creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(TOKEN_PATH, "w") as token:
                token.write(creds.to_json())
        else:
            raise RuntimeError("token.json invalido y sin refresh_token -- regeneralo en tu PC.")
    return creds


def descargar_archivo_drive(nombre_archivo, ruta_destino, carpeta_id=VIDEOS_FOLDER_ID, on_progress=None):
    creds = get_credentials()
    service = build("drive", "v3", credentials=creds)
    resultado = service.files().list(
        q=f"name='{nombre_archivo}' and '{carpeta_id}' in parents and trashed=false",
        fields="files(id, name)",
    ).execute()
    archivos = resultado.get("files", [])
    if not archivos:
        log.error("No se encontro en Drive: %s", nombre_archivo)
        return False
    file_id = archivos[0]["id"]
    request = service.files().get_media(fileId=file_id)
    with open(ruta_destino, "wb") as f:
        # 1MB por chunk hacia y vuelta a Drive por cada uno -- para un video de
        # decenas/cientos de MB eso son cientos de round-trips secuenciales.
        # 25MB reduce muchisimo esa sobrecarga sin pedir demasiada memoria.
        downloader = MediaIoBaseDownload(f, request, chunksize=25 * 1024 * 1024)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status and on_progress:
                on_progress(int(status.progress() * 100))
    return True


# ── TikTok (sin probar todavia -- ver REGISTRO_DE_PROGRESO.md) ──────────

def descargar_video_tiktok(url, carpeta_videos, video):
    if not os.path.exists(COOKIES_PATH):
        raise RuntimeError(
            f"No existe {COOKIES_PATH}. Configura TIKTOK_COOKIES_TXT_B64 en las "
            "variables de entorno del endpoint (cookies.txt en base64)."
        )
    ydl_opts = {
        "cookiefile": COOKIES_PATH,
        "format": "bestvideo+bestaudio/best",
        "outtmpl": os.path.join(carpeta_videos, f"{video}.%(ext)s"),
        "merge_output_format": "mp4",
        "postprocessor_args": [
            "-c:v", "libx264",
            "-c:a", "aac",
            "-crf", "23",
            "-preset", "fast",
            "-movflags", "+faststart",
        ],
        "noplaylist": True,
        "quiet": False,
    }
    resultado = {"status": None}

    def worker():
        try:
            with YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
            resultado["status"] = "ok"
        except Exception as e:
            resultado["status"] = f"error_{e}"

    hilo = threading.Thread(target=worker)
    hilo.start()
    hilo.join(60)
    if hilo.is_alive():
        resultado["status"] = "timeout"
        log.warning("La descarga de TikTok se colgo para %s", video)
    return resultado["status"]


def descargar_fuente(video_row, carpeta_tmp):
    """Descarga el video fuente (TikTok, B2 o Drive) a carpeta_tmp/{nombre}.mp4. Devuelve la ruta o None."""
    nombre = video_row["nombre"]
    enlace = video_row.get("enlace")
    fuente_b2_key = video_row.get("fuente_b2_key")
    destino = os.path.join(carpeta_tmp, f"{nombre}.mp4")
    if enlace:
        for intento in range(3):
            nombre_iteracion = f"{nombre}_intento{intento + 1}" if intento > 0 else nombre
            estado = descargar_video_tiktok(enlace, carpeta_tmp, nombre_iteracion)
            if estado == "ok":
                archivo_actual = os.path.join(carpeta_tmp, f"{nombre_iteracion}.mp4")
                os.replace(archivo_actual, destino)
                return destino
            log.warning("Intento %d de descarga fallo para %s: %s", intento + 1, nombre, estado)
        return None
    elif fuente_b2_key:
        # Subido desde la app Android "Enviar videos" -- no pasa por Google
        # Drive (a proposito, para no compartir esa cuenta personal). El
        # archivo ya esta en B2 bajo el prefijo "fuentes/", subido directo
        # por la app; solo hace falta bajarlo con el mismo cliente `s3` que
        # ya se usa para los renders.
        actualizar_progreso(video=nombre, etapa="descargando", progreso=0)
        try:
            s3.download_file(B2_BUCKET, fuente_b2_key, destino)
        except Exception as e:
            log.error("No se pudo descargar fuente de B2 (%s) para %s: %s", fuente_b2_key, nombre, e)
            return None
        actualizar_progreso(video=nombre, etapa="descargando", progreso=100)
        return destino
    else:
        # `nombre` deberia venir sin extension (ej. "video_60"), pero
        # aplicacion_videos/videos.py tiene un bug que a veces la guarda con
        # ".mp4" ya incluido -- normalizar aca en vez de esperar a que se
        # arregle del lado del .exe (asi no hace falta regenerarlo).
        nombre_base = nombre[:-4] if nombre.lower().endswith(".mp4") else nombre
        actualizar_progreso(video=nombre, etapa="descargando", progreso=0)
        ok = descargar_archivo_drive(
            f"{nombre_base}.mp4", destino,
            on_progress=lambda pct: actualizar_progreso(video=nombre, etapa="descargando", progreso=pct),
        )
        return destino if ok else None


# ── Conversion / render ffmpeg NVENC (identico a procesador.py) ─────────

def arreglar_video(input_file, output_file, nombre_video=None):
    duracion = obtener_duracion_video(input_file)

    # Instagram (via Buffer) rechaza videos con mas de 60fps -- fuentes de
    # TikTok/Drive a veces traen fps rara (ej. 66fps confirmado, 2026-08-18,
    # ver REGISTRO_DE_PROGRESO.md). Esa vez en particular Buffer lo rechazo
    # con un video cuyo r_frame_rate/avg_frame_rate Y espaciado real
    # frame-a-frame median ~29.7fps CFR limpio (sin picos, verificado a mano)
    # -- osea que Buffer esta viendo/calculando el fps de otra forma que
    # ffprobe (probablemente metadata del contenedor, no los timestamps
    # reales). Por eso ESTO NO ES CONDICIONAL: se fuerza `-r` siempre (no
    # solo cuando fps_origen>60), a un valor limpio y constante, para que
    # cualquier metadata de frame rate ambigua/inconsistente que traiga la
    # fuente quede re-escrita de una sola forma, sin importar que numero
    # reporte cada herramienta. Cap a 60 si la fuente ya viene mas alta.
    fps_origen = obtener_fps_video(input_file)
    fps_final = min(60, round(fps_origen)) if fps_origen else 30
    fps_args = ["-r", str(fps_final)]
    log.info("%s: fuente a %s fps, master forzado a %d fps constante.", nombre_video, fps_origen, fps_final)

    def _reportar(pct):
        actualizar_progreso(video=nombre_video, etapa="convirtiendo_master", progreso=pct)

    def _run(codec_args):
        cmd = [FFMPEG, "-y", "-i", input_file, *codec_args, *fps_args, "-c:a", "copy", "-progress", "pipe:1", "-nostats", output_file]
        return ejecutar_ffmpeg_con_progreso(cmd, duracion, on_progress=_reportar, timeout=300)

    try:
        actualizar_progreso(video=nombre_video, etapa="convirtiendo_master", progreso=0)
        res = _run(["-c:v", "h264_nvenc"])
        if res.returncode != 0:
            log.warning("NVENC fallo en master (%s), reintentando con libx264 por software.", input_file)
            res = _run(["-c:v", "libx264", "-preset", "fast", "-crf", "20"])
            if res.returncode != 0:
                log.error("ffmpeg stderr: %s", res.stderr[-500:])
                return f"error_code_{res.returncode}"
        return "ok"
    except Exception as e:
        return f"error_{e}"


def convertir_video(input_file, output_file, nombre_video=None):
    for intento in range(3):
        estado = arreglar_video(input_file, output_file, nombre_video=nombre_video)
        if estado == "ok":
            return True
        log.warning("Fallo conversion (intento %d): %s", intento + 1, estado)
        time.sleep(1)
    return False


def construir_filtro_ffmpeg(r, g, b, brillo):
    r_final = min(r * brillo, 1.0)
    g_final = min(g * brillo, 1.0)
    b_final = min(b * brillo, 1.0)
    return (
        f"curves="
        f"r='0/0 1/{r_final:.4f}':"
        f"g='0/0 1/{g_final:.4f}':"
        f"b='0/0 1/{b_final:.4f}'"
    )


def _procesar_un_video(tarea):
    ultimo_error = None
    for _intento in range(1, 4):
        try:
            ruta_video = tarea["ruta_video"]
            ruta_final = tarea["ruta_final"]
            sticker_path = tarea["sticker_path"]
            corte = tarea["corte"]
            filtro = tarea["filtro"]
            bitrate_extra = tarea["bitrate_extra"]
            version = tarea["version"]

            speed = random.uniform(0.98, 1.02)
            # crop_x/crop_y deben quedar dentro de lo que crop_w/crop_h recortan
            # (-crop_w, -crop_h), si no el recorte pide una region que se sale
            # del cuadro original y ffmpeg falla con "Invalid argument".
            crop_w = random.randint(-8, 0)
            crop_h = random.randint(-8, 0)
            crop_x = random.randint(0, -crop_w)
            crop_y = random.randint(0, -crop_h)
            sticker_x = random.randint(755, 785)
            sticker_y = random.randint(1490, 1510)

            resultado = subprocess.run(
                [FFPROBE, "-v", "quiet", "-print_format", "json", "-show_streams", "-show_format", ruta_video],
                capture_output=True, text=True,
            )
            info = json.loads(resultado.stdout)
            bitrate_original = int(info["streams"][0].get("bit_rate", 0)) // 1000
            bitrate_final = bitrate_original + bitrate_extra

            # Duracion esperada del render (post trim/velocidad) para poder
            # calcular un % de progreso -- aproximado, alcanza para una barra.
            duracion_master = float(info.get("format", {}).get("duration", 0) or 0)
            duracion_render = max((duracion_master - corte) / speed, 0) if duracion_master else None

            video_nombre = tarea.get("video_nombre")
            cuenta = tarea["cuenta"]

            def _reportar(pct):
                actualizar_progreso(video=video_nombre, cuenta=cuenta, etapa="renderizando", progreso=pct)

            filtro_str = construir_filtro_ffmpeg(*filtro) + "," if filtro else ""

            filter_complex = (
                f"[0:v]trim=start={corte},setpts=PTS-STARTPTS,setpts={round(1 / speed, 4)}*PTS,"
                f"crop=iw{crop_w}:ih{crop_h}:{crop_x}:{crop_y},"
                f"scale=iw:ih,"
                f"{filtro_str}"
                f"scale=trunc(iw/2)*2:trunc(ih/2)*2,"
                f"format=yuv420p[base];"
                f"[base][1:v]overlay={sticker_x}:{sticker_y}[out]"
            )

            def _run(extra_args, codec_args):
                cmd = [
                    FFMPEG, "-y",
                    *extra_args,
                    "-i", ruta_video,
                    "-i", sticker_path,
                    "-filter_complex", filter_complex,
                    "-map", "[out]",
                    "-map", "0:a",
                    "-af", f"atrim=start={corte},asetrate=44100*{round(speed, 4)},aresample=44100,asetpts=PTS-STARTPTS",
                    *codec_args,
                    "-c:a", "aac",
                    "-metadata", f"title=Video {version}",
                    "-metadata", f"comment=Version {version}",
                    "-metadata", f"encoder=Studio{random.randint(1, 999)}",
                    "-progress", "pipe:1", "-nostats",
                    ruta_final,
                ]
                return ejecutar_ffmpeg_con_progreso(cmd, duracion_render, on_progress=_reportar, timeout=300)

            nvenc_args = ["-c:v", "h264_nvenc", "-preset", "p4", "-rc", "vbr", "-b:v", f"{bitrate_final}k"]
            # -b:v (bitrate fijo) hace que libx264 tarde varios frames en
            # "calibrar" su control de tasa cuando el video arranca con un
            # salto de PTS (como el que deja trim+setpts de arriba, aunque
            # ya este reseteado a 0) -- durante esa calibracion emite frames
            # en gris solido (Y=128, el valor neutro sin datos), NO negros,
            # con audio ya sonando normal. Confirmado en pruebas 2026-08-03
            # (fuera de esta VPS, en video_60.mp4): con -b:v tarda ~9 frames
            # (0.3s) en corregirse solo; con -crf (calidad constante, sin
            # calibrar una tasa objetivo) no aparece nunca, desde el primer
            # frame. -maxrate/-bufsize mantienen el mismo techo de bitrate
            # por version (bitrate_final, ver BITRATES en config_render.py)
            # que ya se usaba para diferenciar las 15 versiones entre si.
            libx264_args = ["-c:v", "libx264", "-preset", "fast", "-crf", "20", "-maxrate", f"{bitrate_final}k", "-bufsize", f"{bitrate_final * 2}k"]

            actualizar_progreso(video=video_nombre, cuenta=cuenta, etapa="renderizando", progreso=0)
            res = _run(["-hwaccel", "cuda"], nvenc_args)
            if res.returncode != 0:
                log.warning("NVENC fallo en render (%s), reintentando por software con libx264.", ruta_video)
                res = _run([], libx264_args)
                if res.returncode != 0:
                    raise RuntimeError(res.stderr[-500:])

            return tarea, True, None
        except Exception as e:
            ultimo_error = e
    return tarea, False, str(ultimo_error)


# ── Backblaze B2 ─────────────────────────────────────────────────────────

def subir_a_b2(ruta_local, key):
    s3.upload_file(ruta_local, B2_BUCKET, key)
    return key


# ── Supabase: coordinacion de estado (identico a procesador.py) ─────────

def claim_video(nombre):
    resp = (
        supabase.table("videos")
        .update({"estado": "procesando", "claimed_at": datetime.now(timezone.utc).isoformat()})
        .eq("nombre", nombre)
        .eq("estado", "pendiente")
        .execute()
    )
    return bool(resp.data)


def marcar_video(nombre, **campos):
    supabase.table("videos").update(campos).eq("nombre", nombre).execute()


def upsert_render(video_nombre, cuenta, nombre_tabla, descripcion_formateada, **campos):
    fila = {
        "video_nombre": video_nombre,
        "cuenta": cuenta,
        "nombre_tabla": nombre_tabla,
        "descripcion_formateada": descripcion_formateada,
        **campos,
    }
    supabase.table("video_renders").upsert(fila, on_conflict="video_nombre,cuenta").execute()


def encolar_para_subida(video, cuenta, descripcion_formateada, b2_key, titulo=None):
    """
    Escribe directo en `cola_subida` apenas un render (o el master) queda
    listo en B2 -- antes esto lo hacia subida_bot.py al apretar "Revisar
    videos listos", traduciendo videos/video_renders a cola_subida. Hacerlo
    aca, en el momento exacto en que cada pieza termina, evita ese paso
    intermedio redundante (subida_bot.py ya sabe re-descargar de B2 lo que
    le falte solo con leer cola_subida, ver cargar_cola_subida()).

    `titulo` (opcional) es el titulo capturado en videos.py, necesario para
    que subida_bot.py pueda completar el post de YouTube en Buffer -- se
    guarda tal cual venga en `videos.titulo`, sin importar si la cuenta
    destino es YouTube o no (subida_bot.py decide si lo usa).
    """
    try:
        supabase.table("cola_subida").upsert({
            "video": video,
            "cuenta": cuenta,
            "descripcion": descripcion_formateada,
            "b2_key": b2_key,
            "usable": "si",
            "titulo": titulo,
        }, on_conflict="video,cuenta").execute()
    except Exception as e:
        log.error("No se pudo encolar %s para %s en cola_subida: %s", video, cuenta, e)


def intentar_finalizar_video(nombre, video_row):
    """
    Borra `videos`/`video_renders` de este video una vez que (1) no queda
    ningun render pendiente ni en error, y (2) TODAS sus cuentas ya tienen
    fila en `cola_subida`. El punto (2) es a proposito, aunque parezca
    redundante con encolar_para_subida(): si ese upsert fallo por un
    problema transitorio de red, sin este chequeo se borraria igual el
    rastro en videos/video_renders y el video se perderia para siempre (B2
    tendria el archivo pero nada lo apuntaria). Si falta alguna, se
    reintenta encolar aca mismo antes de decidir borrar o no.
    """
    try:
        pendientes = (
            supabase.table("video_renders")
            .select("cuenta")
            .eq("video_nombre", nombre)
            .neq("estado", "listo")
            .execute().data
        )
    except Exception as e:
        log.error("No se pudo revisar renders pendientes de %s: %s", nombre, e)
        return
    if pendientes:
        return

    cuentas_raw = video_row["cuentas"]
    cuentas = json.loads(cuentas_raw) if isinstance(cuentas_raw, str) else cuentas_raw
    try:
        encoladas = {
            r["cuenta"] for r in
            supabase.table("cola_subida").select("cuenta").eq("video", nombre).execute().data
        }
        # CRITICO: una cuenta que ya subio con exito se BORRA de cola_subida
        # (ver _procesar_resultado_cuenta) -- sin este chequeo, "no esta en
        # cola_subida" se confundia con "nunca se encolo", y esta funcion la
        # volvia a encolar (y por lo tanto a subir) una SEGUNDA vez. Bug real
        # en produccion 2026-07-27: un render que fallo y se reintento con
        # exito mas tarde disparo este chequeo de nuevo cuando las otras 11
        # cuentas ya habian subido y sido borradas de cola_subida -- se
        # volvieron a encolar y 7 de ellas (las que compartian el master,
        # todavia en B2) se publicaron DUPLICADAS de verdad en Buffer.
        confirmadas = {
            r["cuenta"] for r in
            supabase.table("subidas_confirmadas").select("cuenta").eq("video", nombre).execute().data
        }
    except Exception as e:
        log.error("No se pudo verificar cola_subida/subidas_confirmadas de %s: %s", nombre, e)
        return
    faltantes = [c for c in cuentas if c not in encoladas and c not in confirmadas]
    if faltantes:
        log.warning("%s: %d cuenta/s sin encolar todavia, reintentando: %s", nombre, len(faltantes), faltantes)
        master_key = video_row.get("master_b2_key")
        renders_por_cuenta = {
            r["cuenta"]: r for r in
            supabase.table("video_renders").select("*").eq("video_nombre", nombre).execute().data
        }
        titulo = video_row.get("titulo")
        for cuenta in faltantes:
            render = renders_por_cuenta.get(cuenta)
            if render and render.get("b2_key"):
                encolar_para_subida(nombre, cuenta, render["descripcion_formateada"], render["b2_key"], titulo)
            elif master_key:
                # Idioma e Instagram (tope real de 5 hashtags) son propios de
                # CADA cuenta -- se formatea una vez por cuenta en vez de
                # reusar una descripcion "master" compartida, para que cada
                # una reciba el pool de hashtags de su propio idioma.
                idioma_cuenta = cuentas_dic.get(cuenta, {}).get("idioma", "ES")
                es_instagram = cuentas_dic.get(cuenta, {}).get("tipo") == "INSTAGRAM"
                max_hashtags = 5 if es_instagram else None
                descripcion_cuenta = formatear_descripcion(video_row["descripcion"], idioma=idioma_cuenta, max_hashtags=max_hashtags)
                encolar_para_subida(nombre, cuenta, descripcion_cuenta, master_key, titulo)
        return  # se vuelve a revisar la proxima vez que se llame para este video

    try:
        supabase.table("video_renders").delete().eq("video_nombre", nombre).execute()
        supabase.table("videos").delete().eq("nombre", nombre).execute()
        log.info("%s completamente encolado para subida, borrado de Supabase (videos/video_renders).", nombre)
    except Exception as e:
        log.error("Error borrando %s de Supabase tras completar el encolado: %s", nombre, e)


# ── Subida a Buffer inmediata (reemplaza el paso manual de subida_bot.py) ─

def _borrar_de_b2_si_procede(nombre, b2_key, cuenta, es_tiktok):
    """
    Mismo criterio que limpiar_archivos_subidos() en subida_bot.py: las
    cuentas TikTok tienen un render exclusivo (se borra de una). Las
    no-TikTok comparten el master -- solo se borra de B2 cuando (1) ninguna
    otra cuenta no-TikTok de este video sigue pendiente en cola_subida, y
    (2) no quedan renders TikTok de este video sin terminar (el master
    tambien lo usa procesar_reintentos_fallidos para reintentar).
    """
    if es_tiktok:
        try:
            s3.delete_object(Bucket=B2_BUCKET, Key=b2_key)
            log.info("Render de %s para %s borrado de B2 (%s).", nombre, cuenta, b2_key)
        except Exception as e:
            log.warning("No se pudo borrar de B2 el render de %s para %s: %s", nombre, cuenta, e)
        return

    try:
        pendientes_cola = (
            supabase.table("cola_subida").select("cuenta")
            .eq("video", nombre).eq("b2_key", b2_key).execute().data
        )
    except Exception as e:
        log.warning("No se pudo verificar si el master de %s sigue en uso (cola_subida): %s", nombre, e)
        return
    if pendientes_cola:
        return  # otra cuenta no-TikTok de este video todavia lo necesita

    try:
        renders_pendientes = (
            supabase.table("video_renders").select("estado")
            .eq("video_nombre", nombre).neq("estado", "listo").execute().data
        )
    except Exception as e:
        log.warning("No se pudo verificar renders pendientes de %s: %s", nombre, e)
        return
    if renders_pendientes:
        return  # todavia hay renders TikTok sin terminar, el master puede hacer falta para reintentar

    try:
        s3.delete_object(Bucket=B2_BUCKET, Key=b2_key)
        log.info("Master de %s borrado de B2 (%s), ninguna cuenta lo necesita ya.", nombre, b2_key)
    except Exception as e:
        log.warning("No se pudo borrar de B2 el master de %s: %s", nombre, e)


def _procesar_resultado_cuenta(nombre, entrada, estado, screenshot):
    """Bookkeeping de Supabase/B2 para el resultado de UNA cuenta ya
    intentada (la llama _subir_cola_de_perfil una vez por cada video y
    cuenta, justo despues de SesionPerfil.subir). Devuelve (resultado_final_dict, lineas_log_list)."""
    cuenta = entrada["cuenta"]
    perfil = cuentas_dic.get(cuenta, {}).get("buffer") or cuenta
    lineas = [
        f"[{cuenta}] Intentando subir {nombre} a Buffer (perfil {perfil})...",
        f"[{cuenta}] Resultado: {estado}",
    ]

    if estado in ("subida_correcta", "ya_subido"):
        try:
            supabase.table("cola_subida").delete().eq("video", nombre).eq("cuenta", cuenta).execute()
            supabase.table("subidas_confirmadas").upsert(
                {"video": nombre, "cuenta": cuenta}, on_conflict="video,cuenta"
            ).execute()
        except Exception as e:
            log.warning("No se pudo actualizar cola_subida/subidas_confirmadas de %s/%s: %s", nombre, cuenta, e)
        _borrar_de_b2_si_procede(nombre, entrada["b2_key"], cuenta, entrada.get("es_tiktok", False))
        log.info("%s subido a Buffer OK para %s (%s).", nombre, cuenta, estado)
    elif estado == "cola_llena":
        # Benigno, no es un error -- mismo criterio que subida_bot.py: la
        # fila se deja en cola_subida tal cual (sin contar como intento
        # fallido), se reintenta sola cuando se libere un lugar en la cola
        # de Buffer de ese canal.
        log.warning("%s no se subio para %s: cola de Buffer llena, se reintenta despues.", nombre, cuenta)
    elif estado == "requiere_login_manual":
        # Tambien benigno en el mismo sentido que cola_llena: no es un bug
        # de este codigo, es un bloqueo externo (sesion de Buffer caducada y
        # el reloguin automatico -- ver buffer_uploader._intentar_login_buffer
        # -- no pudo completarlo solo, por password mala o verificacion
        # extra de Buffer). No cuenta como intento fallido; se reintenta
        # solo en el proximo ciclo por si alguien ya arreglo la password o
        # confirmo el login a mano.
        log.warning("%s no se subio para %s: Buffer pide reautenticacion manual, se reintenta despues.", nombre, cuenta)
    else:
        # Error real -- cuenta como un intento fallido de ESTA fila de
        # cola_subida (columna 'intentos'). Solo se notifica al llegar al
        # limite (MAX_INTENTOS_SUBIDA), para no mandar un mensaje de
        # Telegram cada 5 minutos por el mismo error -- una vez ahi, la fila
        # deja de reintentarse sola (ver _subir_cola_de_perfil) hasta
        # que se resetee 'intentos' a mano.
        intentos_nuevos = entrada.get("intentos_previos", 0) + 1
        try:
            supabase.table("cola_subida").update({"intentos": intentos_nuevos}).eq("video", nombre).eq("cuenta", cuenta).execute()
        except Exception as e:
            log.warning("No se pudo actualizar el contador de intentos de %s/%s: %s", nombre, cuenta, e)
        if intentos_nuevos >= MAX_INTENTOS_SUBIDA:
            log.error("%s fallo al subir a Buffer para %s tras %d intentos, se deja en pausa: %s", nombre, cuenta, intentos_nuevos, estado)
            notificar_error(
                f"La subida de '{nombre}' para la cuenta '{cuenta}' fallo {intentos_nuevos} veces seguidas "
                f"y no se va a reintentar sola. Revisa el error y resetea 'intentos' en cola_subida (Supabase) "
                f"para que se vuelva a intentar.\n\nError: {estado}"
            )
        else:
            log.warning("%s fallo al subir a Buffer para %s (intento %d/%d): %s", nombre, cuenta, intentos_nuevos, MAX_INTENTOS_SUBIDA, estado)

    resultado_final = {
        "perfil": perfil, "cuenta": cuenta, "video": nombre,
        "estado": estado, "screenshot": screenshot,
    }
    return resultado_final, lineas


def _numero_video(nombre_video: str) -> int:
    m = re.search(r"\d+", nombre_video)
    return int(m.group()) if m else 0


def _video_mas_viejo_bloqueado(cuenta, video_nombre_actual):
    """
    Devuelve el nombre del video mas viejo (numero mas bajo) que sigue en
    `video_renders.estado='error'` para esta cuenta, si hay uno estrictamente
    mas viejo que `video_nombre_actual` -- para no renderizar/reintentar un
    video mas nuevo de la misma cuenta mientras uno viejo sigue sin resolver.
    Mismo criterio que ya existia para `cola_subida` (ver
    `_subir_cola_de_perfil`), extendido a la etapa de render/subida a
    B2 tras el incidente de `video_126.mp4` (2026-08-18): un error ahi
    quedaba huerfano -- sin bloquear videos mas nuevos de la misma cuenta ni
    reintentarse solo -- porque solo `cola_subida` tenia esta proteccion.
    Ver REGISTRO_DE_PROGRESO.md.
    """
    try:
        errores = (
            supabase.table("video_renders")
            .select("video_nombre")
            .eq("cuenta", cuenta)
            .eq("estado", "error")
            .execute()
            .data
        )
    except Exception as e:
        log.warning("No se pudo chequear renders bloqueados para %s, se procesa igual: %s", cuenta, e)
        return None
    actual_num = _numero_video(video_nombre_actual)
    mas_viejo, mas_viejo_num = None, actual_num
    for r in errores:
        v = r["video_nombre"]
        if v == video_nombre_actual:
            continue
        n = _numero_video(v)
        if n < mas_viejo_num:
            mas_viejo, mas_viejo_num = v, n
    return mas_viejo


def _pendientes_por_cuenta(cuentas):
    """Filas de `cola_subida` (usable='si') de las `cuentas` dadas, agrupadas
    por cuenta y ordenadas por numero de video (mas viejo primero). La usa
    _subir_cola_de_perfil al empezar cada vuelta. Devuelve None si la
    consulta falla (quien llama corta la sesion: mejor esperar al proximo
    ciclo que subir sin saber el orden)."""
    try:
        filas = (
            supabase.table("cola_subida").select("*")
            .eq("usable", "si").in_("cuenta", list(cuentas))
            .execute().data
        )
    except Exception as e:
        log.error("Error consultando cola_subida (%s): %s", ", ".join(cuentas), e)
        return None
    por_cuenta: dict = {}
    for f in filas:
        por_cuenta.setdefault(f["cuenta"], []).append(f)
    for lista in por_cuenta.values():
        lista.sort(key=lambda f: _numero_video(f["video"]))
    return por_cuenta


def _subir_cola_de_perfil(perfil, cuentas_del_perfil):
    """
    Vacia la cola de subida de UN perfil de Firefox (un login de Buffer)
    abriendo UN SOLO navegador (buffer_uploader.SesionPerfil) para todo.
    Corre en su propio hilo (uno por perfil, ver subir_cola_subida). Reemplaza
    (2026-10-09) a intentar_subir_a_buffer + procesar_backlog_cola_subida,
    que subian un solo video por cuenta por ciclo y abrian un Firefox por
    cada tanda -- el siguiente video de una cuenta esperaba ~5 min.

    Por cada cuenta del perfil sube sus videos pendientes UNO TRAS OTRO, en
    orden estricto (numero de video mas bajo primero), mismo criterio que
    procesar_cuenta() en subida_bot.py:
      - exito/ya_subido -> sigue con el siguiente video de esa cuenta.
      - error -> +1 intento (ver _procesar_resultado_cuenta) y la cuenta se
        corta por esta sesion; los videos mas nuevos esperan detras.
      - cola_llena / requiere_login_manual -> la cuenta se corta, sin intento.
      - el mas viejo con intentos >= MAX_INTENTOS_SUBIDA -> cuenta DETENIDA,
        no se intenta nada hasta el reset manual en Supabase.
    Al terminar una vuelta, ANTES de cerrar el navegador vuelve a consultar
    cola_subida para TODAS las cuentas del perfil (puede haber entrado algo
    mientras subia) y repite, hasta que una vuelta no tenga nada que hacer
    (tope MAX_VUELTAS_SESION). Una fila que ya se intento en esta sesion no
    se reintenta en la misma sesion (si no, gastaria sus 3 intentos en
    minutos sobre el mismo error).

    Devuelve una lista de (resultado, lineas_log) en el orden en que se
    intentaron -- resultado como el de _procesar_resultado_cuenta.
    """
    sesion = buffer_uploader.SesionPerfil(perfil)
    carpeta_tmp = os.path.join(DESCARGAS_DIR, f"_subida_{perfil}")
    os.makedirs(carpeta_tmp, exist_ok=True)
    intentadas: set = set()  # (video, cuenta) ya intentadas en esta sesion
    cortadas: set = set()    # cuentas que ya no siguen en esta sesion
    salida = []
    try:
        for vuelta in range(1, MAX_VUELTAS_SESION + 1):
            pendientes = _pendientes_por_cuenta(cuentas_del_perfil)
            if pendientes is None:
                break
            hubo_trabajo = False
            for cuenta, filas in pendientes.items():
                if cuenta in cortadas:
                    continue
                for fila in filas:
                    nombre = fila["video"]
                    if (nombre, cuenta) in intentadas:
                        # Sigue en la cola tras intentarla -> no se subio; no
                        # pasar a un video mas nuevo de esta cuenta.
                        cortadas.add(cuenta)
                        break
                    if fila.get("intentos", 0) >= MAX_INTENTOS_SUBIDA:
                        log.info(
                            "[%s] cuenta '%s' detenida -- %s agoto sus intentos y sigue sin resolver; no se sube nada mas para esta cuenta.",
                            perfil, cuenta, nombre,
                        )
                        cortadas.add(cuenta)
                        break
                    hubo_trabajo = True
                    intentadas.add((nombre, cuenta))
                    ruta_local = os.path.join(carpeta_tmp, f"{nombre}_{nombre_tabla_seguro(cuenta)}.mp4")
                    try:
                        s3.download_file(B2_BUCKET, fila["b2_key"], ruta_local)
                    except Exception as e:
                        log.error("No se pudo descargar %s (%s) desde B2 para subirlo: %s", nombre, cuenta, e)
                        # Cuenta como intento fallido -- sin esto, una fila con
                        # el archivo ya borrado de B2 queda reintentando en
                        # silencio para siempre (bug real, 2026-07-27).
                        intentos_nuevos = fila.get("intentos", 0) + 1
                        try:
                            supabase.table("cola_subida").update({"intentos": intentos_nuevos}).eq("video", nombre).eq("cuenta", cuenta).execute()
                        except Exception as e2:
                            log.warning("No se pudo actualizar el contador de intentos de %s/%s: %s", nombre, cuenta, e2)
                        if intentos_nuevos >= MAX_INTENTOS_SUBIDA:
                            notificar_error(
                                f"No se pudo descargar el archivo de '{nombre}' para la cuenta '{cuenta}' desde B2 "
                                f"tras {intentos_nuevos} intentos (probablemente el archivo ya no existe) -- no se va "
                                f"a reintentar sola. Revisa 'cola_subida' en Supabase.\n\nError: {e}"
                            )
                        cortadas.add(cuenta)
                        break
                    try:
                        log.info("[%s] Subiendo %s para %s (vuelta %d)...", perfil, nombre, cuenta, vuelta)
                        r = sesion.subir(cuenta, ruta_local, fila["descripcion"], fila.get("titulo"))
                        entrada = {
                            "cuenta": cuenta, "b2_key": fila["b2_key"],
                            "es_tiktok": cuentas_dic.get(cuenta, {}).get("tipo") == "TIKTOK",
                            "intentos_previos": fila.get("intentos", 0),
                        }
                        salida.append(_procesar_resultado_cuenta(nombre, entrada, r["estado"], r.get("screenshot")))
                    finally:
                        try:
                            os.remove(ruta_local)
                        except OSError:
                            pass
                    if r["estado"] not in ("subida_correcta", "ya_subido"):
                        cortadas.add(cuenta)
                        break
            if not hubo_trabajo:
                break
        else:
            log.warning("[%s] Se alcanzo el tope de %d vueltas en una sesion; lo que quede sigue el proximo ciclo.", perfil, MAX_VUELTAS_SESION)
    finally:
        sesion.cerrar()
        shutil.rmtree(carpeta_tmp, ignore_errors=True)
    return salida


def subir_cola_subida():
    """
    Unica via de subida a Buffer (2026-10-09). La llama handler() una vez
    por ciclo, DESPUES de procesar el video nuevo y los reintentos de render
    (esos solo encolan en cola_subida, ya no suben por su cuenta). Agrupa
    las cuentas con algo pendiente por perfil de Firefox (`cuentas.buffer`)
    y lanza un hilo por perfil (hasta MAX_SUBIDAS_PARALELAS a la vez,
    perfiles distintos no se pisan); cada hilo considera TODAS las cuentas
    de su perfil, no solo las que tenian algo al empezar, para que la
    re-consulta antes de cerrar el navegador las cubra. Al final manda un
    reporte por video (reportes.reportar_resultado_subida), como antes.
    """
    try:
        cuentas_con_algo = {
            f["cuenta"] for f in
            supabase.table("cola_subida").select("cuenta").eq("usable", "si").execute().data
        }
    except Exception as e:
        log.error("Error consultando cola_subida: %s", e)
        return
    if not cuentas_con_algo:
        return

    def _perfil(cuenta):
        return cuentas_dic.get(cuenta, {}).get("buffer") or cuenta

    cuentas_por_perfil: dict = {}
    for c in set(cuentas_dic) | cuentas_con_algo:
        cuentas_por_perfil.setdefault(_perfil(c), set()).add(c)
    perfiles = {_perfil(c) for c in cuentas_con_algo}
    log.info("Subida a Buffer: %d perfil(es) con videos en cola.", len(perfiles))

    inicio_sesion = datetime.now(timezone.utc)
    por_video: dict = {}  # video -> ([resultados], [lineas])
    with ThreadPoolExecutor(max_workers=MAX_SUBIDAS_PARALELAS) as executor:
        futures = {
            executor.submit(_subir_cola_de_perfil, perfil, sorted(cuentas_por_perfil[perfil])): perfil
            for perfil in perfiles
        }
        for future in as_completed(futures):
            try:
                salida = future.result()
            except Exception:
                log.exception("Error no controlado en el hilo de subida del perfil %s", futures[future])
                continue
            for resultado, lineas in salida:
                res, lin = por_video.setdefault(resultado["video"], ([], []))
                res.append(resultado)
                lin.extend(lineas)

    for nombre, (resultados_video, lineas_video) in por_video.items():
        reportes.reportar_resultado_subida(nombre, resultados_video, inicio_sesion, lineas_video)


# ── Pipeline por video ───────────────────────────────────────────────────

def procesar_un_video_completo(video_row, stickers):
    nombre = video_row["nombre"]
    log.info("=== Procesando %s ===", nombre)

    if not claim_video(nombre):
        log.info("%s ya no esta pendiente (lo tomo otra corrida), saltando.", nombre)
        return

    carpeta_tmp = os.path.join(DESCARGAS_DIR, nombre)
    os.makedirs(carpeta_tmp, exist_ok=True)
    try:
        ruta_descarga = descargar_fuente(video_row, carpeta_tmp)
        if not ruta_descarga:
            marcar_video(nombre, estado="error", error_msg="fallo la descarga de la fuente")
            notificar_error(f"'{nombre}' fallo al descargar la fuente (Drive/TikTok). Revisa el enlace o las credenciales.")
            return

        ruta_master = os.path.join(carpeta_tmp, f"{nombre}_master.mp4")
        if not convertir_video(ruta_descarga, ruta_master, nombre_video=nombre):
            marcar_video(nombre, estado="error", error_msg="fallo la conversion NVENC")
            notificar_error(f"'{nombre}' fallo al convertir el video fuente (master). Revisa el archivo original.")
            return

        master_key = f"renders/{nombre}/master.mp4"
        subir_a_b2(ruta_master, master_key)
        marcar_video(nombre, master_b2_key=master_key)
        video_row["master_b2_key"] = master_key  # para que intentar_finalizar_video lo tenga sin re-consultar
        log.info("Master de %s subido a B2 (%s)", nombre, master_key)

        cuentas_raw = video_row["cuentas"]
        cuentas = json.loads(cuentas_raw) if isinstance(cuentas_raw, str) else cuentas_raw

        # Si esta funcion se esta corriendo de nuevo para este mismo video
        # (ej. se reintenta tras un error inesperado a mitad de camino, ver
        # notificar_error() en handler()), no hay que reprocesar cuentas que
        # ya se confirmaron con exito -- ni volver a renderizar (TikTok) ni
        # volver a encolar/intentar subir a Buffer (todas). Sin este chequeo,
        # un reintento vuelve a hacer todo el trabajo de las cuentas que ya
        # habian salido bien, con riesgo real de subida DUPLICADA a Buffer
        # (bug real, 2026-08-03, video_116.mp4: crash de B2 en una sola
        # cuenta tumbo el video entero a estado='error'; las otras 11 cuentas
        # ya se habian subido bien via el backlog de cola_subida antes de
        # reintentar, y el reintento las iba a re-renderizar/re-subir igual).
        try:
            ya_confirmadas = {
                r["cuenta"] for r in
                supabase.table("subidas_confirmadas").select("cuenta").eq("video", nombre).execute().data
            }
        except Exception as e:
            log.warning("No se pudo consultar subidas_confirmadas de %s, se procesa igual: %s", nombre, e)
            ya_confirmadas = set()
        if ya_confirmadas:
            log.info("%s: %d cuenta/s ya confirmadas de antes, no se reprocesan: %s", nombre, len(ya_confirmadas), sorted(ya_confirmadas))

        cuentas_pendientes = [c for c in cuentas if c not in ya_confirmadas]
        cuentas_tiktok_candidatas = [c for c in cuentas_pendientes if cuentas_dic.get(c, {}).get("tipo") == "TIKTOK"]
        cuentas_no_tiktok = [c for c in cuentas_pendientes if c not in cuentas_tiktok_candidatas]

        # No renderizar para una cuenta si un video mas viejo sigue trabado en
        # error para ella (ver _video_mas_viejo_bloqueado) -- evita que un
        # video nuevo se procese/publique fuera de orden mientras uno viejo
        # sigue sin resolver, mismo criterio que ya existia para cola_subida.
        cuentas_tiktok = []
        for cuenta in cuentas_tiktok_candidatas:
            bloqueador = _video_mas_viejo_bloqueado(cuenta, nombre)
            if bloqueador:
                log.info(
                    "%s: se saltea el render para %s -- hay un video mas viejo (%s) todavia sin resolver para la misma cuenta.",
                    nombre, cuenta, bloqueador,
                )
                continue
            cuentas_tiktok.append(cuenta)

        if cuentas_no_tiktok:
            # Idioma e Instagram (tope real de 5 hashtags) son propios de
            # CADA cuenta -- se formatea una vez por cuenta en vez de reusar
            # una descripcion "master" compartida, para que cada una reciba
            # el pool de hashtags de su propio idioma (ver cuentas.idioma).
            for cuenta in cuentas_no_tiktok:
                idioma_cuenta = cuentas_dic.get(cuenta, {}).get("idioma", "ES")
                es_instagram = cuentas_dic.get(cuenta, {}).get("tipo") == "INSTAGRAM"
                max_hashtags = 5 if es_instagram else None
                descripcion_cuenta = formatear_descripcion(video_row["descripcion"], idioma=idioma_cuenta, max_hashtags=max_hashtags)
                encolar_para_subida(nombre, cuenta, descripcion_cuenta, master_key, video_row.get("titulo"))
            log.info("%s: master encolado directo para subida (%d cuenta/s no-TikTok).", nombre, len(cuentas_no_tiktok))

        if cuentas_tiktok:
            futures = {}
            with ThreadPoolExecutor(max_workers=MAX_RENDERS_PARALELOS) as executor:
                for cuenta in cuentas_tiktok:
                    version = cuentas_dic[cuenta]["contador"]
                    nombre_tabla = nombre_tabla_seguro(cuenta)
                    descripcion_formateada = formatear_descripcion(video_row["descripcion"], idioma=cuentas_dic[cuenta].get("idioma", "ES"))
                    ruta_final = os.path.join(carpeta_tmp, f"{nombre_tabla}.mp4")
                    upsert_render(nombre, cuenta, nombre_tabla, descripcion_formateada, estado="pendiente")
                    tarea = {
                        "ruta_video": ruta_master,
                        "ruta_final": ruta_final,
                        "sticker_path": stickers[version - 1],
                        "corte": CORTES[version - 1],
                        "filtro": FILTROS[version - 1],
                        "bitrate_extra": BITRATES[version - 1],
                        "version": version,
                        "cuenta": cuenta,
                        "nombre_tabla": nombre_tabla,
                        "descripcion_formateada": descripcion_formateada,
                        "video_nombre": nombre,
                    }
                    futures[executor.submit(_procesar_un_video, tarea)] = tarea

                for future in as_completed(futures):
                    tarea, ok, error = future.result()
                    if ok:
                        # subir_a_b2() puede fallar (ej. InternalError transitorio
                        # de B2, ver video_126.mp4/2026-08-18) -- si eso pasa SIN
                        # atajarlo aca, la excepcion se escapa del for, aborta el
                        # resto de las cuentas de este mismo lote (que ya habian
                        # renderizado bien) y tumba el video ENTERO a
                        # estado='error' sin reintento automatico ni Telegram
                        # confiable (bug real que dejo huerfano a video_126.mp4).
                        # Tratarlo como un fallo de render mas lo mete en el
                        # mismo circuito de reintento/notificacion que ya existe
                        # para renders (procesar_reintentos_fallidos, MAX_INTENTOS_RENDER).
                        try:
                            key = f"renders/{nombre}/{tarea['nombre_tabla']}.mp4"
                            subir_a_b2(tarea["ruta_final"], key)
                        except Exception as e:
                            upsert_render(
                                nombre, tarea["cuenta"], tarea["nombre_tabla"], tarea["descripcion_formateada"],
                                estado="error", error_msg=f"fallo al subir el render a B2: {e}",
                            )
                            log.error("Render de %s para %s: fallo al subir a B2: %s", nombre, tarea["cuenta"], e)
                            continue
                        upsert_render(
                            nombre, tarea["cuenta"], tarea["nombre_tabla"], tarea["descripcion_formateada"],
                            estado="listo", b2_key=key,
                        )
                        encolar_para_subida(nombre, tarea["cuenta"], tarea["descripcion_formateada"], key, video_row.get("titulo"))
                        log.info("Render de %s para %s listo (%s)", nombre, tarea["cuenta"], key)
                    else:
                        upsert_render(
                            nombre, tarea["cuenta"], tarea["nombre_tabla"], tarea["descripcion_formateada"],
                            estado="error", error_msg=error,
                        )
                        log.error("Render de %s para %s fallo: %s", nombre, tarea["cuenta"], error)

        marcar_video(nombre, estado="listo")
        log.info("%s listo.", nombre)
        intentar_finalizar_video(nombre, video_row)
        # La subida a Buffer ya no se hace aca: lo encolado se sube al final
        # del ciclo en subir_cola_subida(), junto con el resto de la cola y
        # en orden estricto por cuenta (2026-10-09).
    finally:
        shutil.rmtree(carpeta_tmp, ignore_errors=True)
        actualizar_progreso(etapa="idle", progreso=0)


def procesar_reintentos_fallidos(stickers):
    """
    Reintenta renders individuales que quedaron en 'error' (de videos que ya
    estan en estado='listo' -- el video en general no vuelve a 'pendiente'
    solo porque una cuenta fallo, ver REGISTRO_DE_PROGRESO.md). Reusa el
    master ya subido a B2 en vez de re-descargar/re-convertir la fuente.
    Hasta MAX_INTENTOS_RENDER veces por render; despues queda en error
    definitivo y solo se puede reintentar reseteando 'intentos' a mano.
    """
    try:
        fallidos = (
            supabase.table("video_renders")
            .select("*")
            .eq("estado", "error")
            .lt("intentos", MAX_INTENTOS_RENDER)
            .execute()
            .data
        )
    except Exception as e:
        log.error("Error consultando renders fallidos para reintentar: %s", e)
        return
    if not fallidos:
        return

    por_video: dict = {}
    for f in fallidos:
        por_video.setdefault(f["video_nombre"], []).append(f)

    for nombre, renders_fallidos in por_video.items():
        video_rows = (
            supabase.table("videos").select("*").eq("nombre", nombre).eq("estado", "listo").execute().data
        )
        if not video_rows:
            continue  # el video ya no existe (se borro tras staging completo) o no esta 'listo'
        master_key = video_rows[0].get("master_b2_key")
        if not master_key:
            continue

        # No reintentar el render de una cuenta si un video mas viejo sigue
        # trabado en error para ella (mismo criterio que en
        # procesar_un_video_completo, ver _video_mas_viejo_bloqueado) -- si
        # no, este reintento podria terminar publicando fuera de orden.
        renders_fallidos = [
            r for r in renders_fallidos
            if not _video_mas_viejo_bloqueado(r["cuenta"], nombre)
        ]
        if not renders_fallidos:
            continue

        log.info("Reintentando %d render(s) fallido(s) de %s", len(renders_fallidos), nombre)
        carpeta_tmp = os.path.join(DESCARGAS_DIR, f"{nombre}_retry")
        os.makedirs(carpeta_tmp, exist_ok=True)
        try:
            ruta_master = os.path.join(carpeta_tmp, f"{nombre}_master.mp4")
            try:
                s3.download_file(B2_BUCKET, master_key, ruta_master)
            except Exception as e:
                log.error("No se pudo re-descargar el master de %s desde B2: %s", nombre, e)
                continue

            futures = {}
            with ThreadPoolExecutor(max_workers=MAX_RENDERS_PARALELOS) as executor:
                for render in renders_fallidos:
                    cuenta = render["cuenta"]
                    version = cuentas_dic.get(cuenta, {}).get("contador")
                    if not version:
                        log.warning("Cuenta %s (de %s) ya no existe en `cuentas`, no se puede reintentar.", cuenta, nombre)
                        continue
                    nombre_tabla = render["nombre_tabla"]
                    ruta_final = os.path.join(carpeta_tmp, f"{nombre_tabla}.mp4")
                    tarea = {
                        "ruta_video": ruta_master,
                        "ruta_final": ruta_final,
                        "sticker_path": stickers[version - 1],
                        "corte": CORTES[version - 1],
                        "filtro": FILTROS[version - 1],
                        "bitrate_extra": BITRATES[version - 1],
                        "version": version,
                        "cuenta": cuenta,
                        "nombre_tabla": nombre_tabla,
                        "descripcion_formateada": render["descripcion_formateada"],
                        "video_nombre": nombre,
                    }
                    futures[executor.submit(_procesar_un_video, tarea)] = render

                for future in as_completed(futures):
                    render = futures[future]
                    tarea, ok, error = future.result()
                    intentos_nuevos = render.get("intentos", 0) + 1
                    if ok:
                        try:
                            key = f"renders/{nombre}/{tarea['nombre_tabla']}.mp4"
                            subir_a_b2(tarea["ruta_final"], key)
                        except Exception as e:
                            ok, error = False, f"fallo al subir el render a B2: {e}"
                    if ok:
                        upsert_render(
                            nombre, tarea["cuenta"], tarea["nombre_tabla"], tarea["descripcion_formateada"],
                            estado="listo", b2_key=key, intentos=intentos_nuevos, error_msg=None,
                        )
                        encolar_para_subida(nombre, tarea["cuenta"], tarea["descripcion_formateada"], key, video_rows[0].get("titulo"))
                        log.info("Reintento OK: render de %s para %s (intento %d)", nombre, tarea["cuenta"], intentos_nuevos)
                    else:
                        upsert_render(
                            nombre, tarea["cuenta"], tarea["nombre_tabla"], tarea["descripcion_formateada"],
                            estado="error", error_msg=error, intentos=intentos_nuevos,
                        )
                        log.warning(
                            "Reintento fallo (%d/%d): render de %s para %s: %s",
                            intentos_nuevos, MAX_INTENTOS_RENDER, nombre, tarea["cuenta"], error,
                        )
                        if intentos_nuevos >= MAX_INTENTOS_RENDER:
                            notificar_error(
                                f"Render de '{nombre}' para la cuenta '{tarea['cuenta']}' fallo "
                                f"{intentos_nuevos} veces seguidas y ya no se va a reintentar solo.\n\n"
                                f"Error: {str(error)[:300]}"
                            )
            intentar_finalizar_video(nombre, video_rows[0])
            # Sin subida aca: la hace subir_cola_subida() al final del ciclo.
        finally:
            shutil.rmtree(carpeta_tmp, ignore_errors=True)
            actualizar_progreso(etapa="idle", progreso=0)


# ── Entrypoint de RunPod Serverless ─────────────────────────────────────

def handler(job):
    """
    Se llama una vez por invocacion (cada WORKER_POLL_INTERVAL_SECS, ver
    worker_loop.py). A diferencia de la version "cpu" original -- que
    drenaba hasta BATCH_LIMIT videos de un tiron -- esta copia procesa como
    MAXIMO UN video pendiente por ciclo (procesar = renderizar + encolar);
    el resto de los pendientes se procesan en los ciclos siguientes.

    Al final de cada ciclo (haya o no un video nuevo) `subir_cola_subida()`
    vacia `cola_subida`: lo recien encolado y lo que fallo antes (hasta
    MAX_INTENTOS_SUBIDA veces cada fila), varios videos por cuenta en la
    misma sesion de navegador, en orden estricto por cuenta. Una cuenta
    trabada no frena a las demas (2026-07-26, a pedido del usuario).
    """
    _escribir_credenciales_drive()
    _escribir_cookies()
    cargar_cuentas()
    stickers = listar_stickers()
    log.info("CPUs detectadas: %s -- renders en paralelo: %d", os.cpu_count(), MAX_RENDERS_PARALELOS)

    procesados = []
    errores = []

    rows = (
        supabase.table("videos")
        .select("*")
        .eq("estado", "pendiente")
        .order("created_at")
        .limit(1)
        .execute()
        .data
    )
    if rows:
        row = rows[0]
        nombre = row["nombre"]
        log.info("Video pendiente: %s -- se procesa este ciclo (uno solo).", nombre)
        try:
            procesar_un_video_completo(row, stickers)
        except Exception as e:
            log.exception("Error inesperado procesando %s", nombre)
            marcar_video(nombre, estado="error", error_msg=str(e))
            notificar_error(f"Error inesperado procesando '{nombre}': {str(e)[:300]}")
            errores.append(nombre)
        else:
            estado_final = supabase.table("videos").select("estado").eq("nombre", nombre).execute().data
            if not estado_final:
                # Ya no existe: intentar_finalizar_video() lo borro porque quedo
                # completamente encolado/subido -- cuenta como procesado bien
                # (antes esto se contaba como "error" por una comparacion que no
                # contemplaba este caso -- bug menor preexistente, corregido aca).
                procesados.append(nombre)
            elif estado_final[0]["estado"] == "listo":
                procesados.append(nombre)
            else:
                errores.append(nombre)
    else:
        log.info("No hay videos pendientes en este ciclo.")

    procesar_reintentos_fallidos(stickers)

    # Unica via de subida a Buffer: todo lo que haya en cola_subida (lo
    # recien encolado arriba + lo pendiente de ciclos anteriores), un Firefox
    # por perfil, varios videos por cuenta seguidos y en orden (2026-10-09).
    subir_cola_subida()

    log.info("Job terminado. Procesados: %s. Con error: %s", procesados, errores)
    return {"procesados": procesados, "errores": errores}


if __name__ == "__main__":
    runpod.serverless.start({
        "handler": handler,
        "concurrency_modifier": lambda current: 2,
    })
