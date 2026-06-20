"""
Lambda (Worker LLM) - Centinela de Integridad Cientifica
Variables de entorno (todas requeridas; salen del .env en VM/Docker o de la
config de la funcion en Lambda):
  TABLE_NAME     -> tabla DynamoDB
  GROQ_API_KEY   -> tu API key de Groq
  GROQ_MODEL     -> modelo de Groq, p.ej. 'openai/gpt-oss-120b'
  CONTACT_EMAIL  -> tu correo (Crossref pide uno para el "polite pool")
"""
import os
import json
import time
import urllib.parse
import urllib.request
import urllib.error
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

# Carga el archivo .env si existe (VM / Docker / local).
# En Lambda no hay .env: este bloque se ignora y las variables vienen
# de la configuracion de la funcion. Asi el mismo codigo sirve en ambos lados.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Todo se lee del entorno, sin valores por defecto: si falta alguna, falla claro.
TABLE_NAME = os.environ["TABLE_NAME"]
GROQ_MODEL = os.environ["GROQ_MODEL"]
CONTACT_EMAIL = os.environ["CONTACT_EMAIL"]
GROQ_API_KEY = os.environ["GROQ_API_KEY"]

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

tabla = boto3.resource("dynamodb").Table(TABLE_NAME)

# --- Prompts especializados por disciplina (la pieza "elige prompt segun tema") ---
PROMPTS_POR_TEMA = {
    "Biologia": "Eres un revisor experto en biologia y ciencias de la vida.",
    "Matematicas": "Eres un revisor experto en matematicas y ciencias formales.",
    "Medicina": "Eres un revisor experto en medicina y ciencias clinicas.",
}
PROMPT_DEFECTO = "Eres un revisor experto en integridad cientifica."

INSTRUCCION = (
    " El articulo citado fue RETRACTADO. Analiza como lo usa el autor y responde "
    "UNICAMENTE con un JSON valido con esta forma: "
    '{"veredicto": "ignora_retraccion" | "cita_como_error" | "incierto", '
    '"justificacion": "<una frase breve>", "confianza": <numero entre 0 y 1>}. '
    'Usa "ignora_retraccion" si el autor lo presenta como evidencia valida sin notar '
    'que fue retractado; usa "cita_como_error" si lo menciona justamente como ejemplo '
    "de error, fraude o caso retractado."
)


def handler(event, context):
    """Punto de entrada. Devuelve las referencias que fallaron para que SQS
    reintente SOLO esas (fallo parcial de lote)."""
    fallidas = []
    for record in event["Records"]:
        try:
            _procesar(json.loads(record["body"]))
        except Exception as e:
            print(f"ERROR en mensaje {record['messageId']}: {e}")
            fallidas.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": fallidas}


def _procesar(msg):
    manuscript_id = msg["manuscriptId"]
    ref_id = msg["refId"]
    tema = msg.get("tema", "General")

    retraccion = _consultar_crossref(msg.get("doi"))

    veredicto = None
    if retraccion.get("retractada"):
        estado = "RETRACTADA"
        veredicto = _juzgar_con_groq(tema, msg.get("citaCruda", ""), msg.get("contexto", ""))
    elif not retraccion.get("verificada"):
        estado = "NO_VERIFICADA"   # el DOI no aparece en Crossref
    else:
        estado = "OK"              # existe y no esta retractada

    # Solo se cuenta como procesada si era la PRIMERA vez (idempotencia).
    # Si el mensaje es un duplicado, _guardar_resultado devuelve False y no se recuenta.
    primera_vez = _guardar_resultado(manuscript_id, ref_id, estado, retraccion, veredicto)
    if primera_vez:
        _sumar_procesada(manuscript_id, bool(retraccion.get("retractada")))


def _consultar_crossref(doi):
    """Devuelve si el DOI esta retractado segun Crossref (datos de Retraction Watch)."""
    if not doi:
        return {"verificada": False}

    url = ("https://api.crossref.org/works/"
           + urllib.parse.quote(doi, safe="")
           + "?mailto=" + urllib.parse.quote(CONTACT_EMAIL))
    req = urllib.request.Request(
        url, headers={"User-Agent": f"CentinelaIntegridad/1.0 (mailto:{CONTACT_EMAIL})"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return {"verificada": False}     # DOI desconocido para Crossref
        cuerpo = e.read().decode("utf-8", "ignore")[:300]
        print(f"[Crossref] HTTP {e.code} para DOI {doi}: {cuerpo}")
        raise                                # 5xx / 429 -> que SQS reintente

    actualizaciones = data.get("message", {}).get("updated-by", []) or []
    retracciones = [u for u in actualizaciones if u.get("type") == "retraction"]
    preocupaciones = [u for u in actualizaciones if "concern" in (u.get("type") or "")]
    return {
        "verificada": True,
        "retractada": len(retracciones) > 0,
        "expresionPreocupacion": len(preocupaciones) > 0,
    }


def _juzgar_con_groq(tema, cita, contexto):
    """Le pide a Groq (via HTTP, sin SDK) que clasifique como el autor usa una cita retractada."""
    sistema = PROMPTS_POR_TEMA.get(tema, PROMPT_DEFECTO) + INSTRUCCION
    usuario = (f"Entrada bibliografica:\n{cita}\n\n"
               f"Contexto donde aparece la cita:\n{contexto}")
    cuerpo = json.dumps({
        "model": GROQ_MODEL,
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": sistema},
            {"role": "user", "content": usuario},
        ],
    }).encode("utf-8")
    req = urllib.request.Request(
        GROQ_URL, data=cuerpo, method="POST",
        headers={
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json",
            # Sin esto, urllib manda "Python-urllib/..." y Cloudflare lo bloquea (error 1010).
            "User-Agent": "CentinelaIntegridad/1.0",
        },
    )

    # Reintentos ante 429 (limite de uso) con backoff: 1s, 2s, 4s.
    # Si tras los intentos sigue fallando, se propaga y SQS reintenta el mensaje.
    for intento in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                # parse_float=Decimal: convierte numeros como "confianza": 0.9 a
                # Decimal, porque DynamoDB no acepta float.
                contenido = data["choices"][0]["message"]["content"]
                return json.loads(contenido, parse_float=Decimal)
        except urllib.error.HTTPError as e:
            if e.code == 429 and intento < 2:
                time.sleep(2 ** intento)
                continue
            cuerpo = e.read().decode("utf-8", "ignore")[:300]
            print(f"[Groq] HTTP {e.code}: {cuerpo}")
            raise


def _guardar_resultado(manuscript_id, ref_id, estado, retraccion, veredicto):
    """Escribe el resultado SOLO si la referencia seguia PENDIENTE.
    Devuelve True si fue la primera vez (para contarla), False si era un duplicado."""
    expr = "SET #estado = :e, estaRetractada = :r, verificada = :v"
    vals = {
        ":e": estado,
        ":r": bool(retraccion.get("retractada")),
        ":v": bool(retraccion.get("verificada")),
        ":pendiente": "PENDIENTE",
    }
    if veredicto is not None:
        expr += ", veredictoLLM = :vd"
        vals[":vd"] = veredicto
    try:
        tabla.update_item(
            Key={"PK": f"MANUSCRIPT#{manuscript_id}", "SK": f"REF#{ref_id}"},
            UpdateExpression=expr,
            ConditionExpression="#estado = :pendiente",   # solo si aun no fue procesada
            ExpressionAttributeNames={"#estado": "estado"},
            ExpressionAttributeValues=vals,
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            print(f"[idempotencia] REF#{ref_id} ya estaba procesada; no se recuenta")
            return False
        raise


def _sumar_procesada(manuscript_id, retractada):
    """Suma 1 al contador (y a las retractadas si aplica) de forma atomica.
    Si esta referencia es la ultima (refsProcesadas == totalRefs), cierra el manuscrito."""
    resp = tabla.update_item(
        Key={"PK": f"MANUSCRIPT#{manuscript_id}", "SK": "METADATA"},
        UpdateExpression="ADD refsProcesadas :uno, refsRetractadas :r",
        ExpressionAttributeValues={":uno": 1, ":r": (1 if retractada else 0)},
        ReturnValues="ALL_NEW",   # me devuelve el item ya actualizado
    )
    item = resp["Attributes"]
    total = int(item.get("totalRefs", 0))
    procesadas = int(item.get("refsProcesadas", 0))
    if total and procesadas >= total:
        _cerrar_manuscrito(manuscript_id, total, int(item.get("refsRetractadas", 0)))


def _cerrar_manuscrito(manuscript_id, total, retractadas):
    """Marca el manuscrito como COMPLETADO y calcula el Indice de Integridad
    (porcentaje de referencias no retractadas)."""
    indice = int(round((1 - retractadas / total) * 100)) if total else 0
    tabla.update_item(
        Key={"PK": f"MANUSCRIPT#{manuscript_id}", "SK": "METADATA"},
        UpdateExpression="SET #estado = :c, indiceIntegridad = :i",
        ExpressionAttributeNames={"#estado": "estado"},
        ExpressionAttributeValues={":c": "COMPLETADO", ":i": indice},
    )
    print(f"[cierre] {manuscript_id} COMPLETADO. Indice de Integridad: {indice} "
          f"({retractadas}/{total} retractadas)")
