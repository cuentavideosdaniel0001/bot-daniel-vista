# Vista de codigo: llamadas a Buffer (solo lectura)

Copia SOLO de lectura de `buffer_uploader.py` y `handler.py` (rama cpu-integrado, commit 2ac535f).

Todas las llamadas a la API de Buffer pasan por `BufferClient._query` (buffer_uploader.py ~l.227).

Funciones que llaman a Buffer (1 llamada cada una): `get_channels` (l.246), `get_scheduled_posts` (l.259),
`get_ultimo_post_cola` (l.270), `get_ultimo_post_enviado` (l.283).

Por cuenta: cola llena = 2 llamadas; subida correcta = 4 (5 si cola vacia); reintento sin confirmar = +1.
Flujo: `comprobacion_inicial_subido` (l.353) -> `get_scheduled_posts` -> `get_channels` -> subida por navegador -> `comprobacion_subido` (l.387).
Ciclo de subida por perfiles: handler.py ~l.830-1080.
