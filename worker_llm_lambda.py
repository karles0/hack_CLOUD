
import os
import json
import urllib.parse
import urllib.request
import urllib.error

import boto3
from groq import Groq

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

tabla = boto3.resource("dynamodb").Table(TABLE_NAME)
# max_retries=2 -> un par de reintentos rapidos ante 429 antes de delegar a SQS.
groq_client = Groq(api_key=GROQ_API_KEY, max_retries=2)

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

    _guardar_resultado(manuscript_id, ref_id, estado, retraccion, veredicto)
    _sumar_procesada(manuscript_id)


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
    """Le pide a Groq que clasifique como el autor usa una cita retractada."""
    sistema = PROMPTS_POR_TEMA.get(tema, PROMPT_DEFECTO) + INSTRUCCION
    usuario = (f"Entrada bibliografica:\n{cita}\n\n"
               f"Contexto donde aparece la cita:\n{contexto}")
    resp = groq_client.chat.completions.create(
        model=GROQ_MODEL,
        temperature=0.2,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": sistema},
            {"role": "user", "content": usuario},
        ],
    )
    return json.loads(resp.choices[0].message.content)


def _guardar_resultado(manuscript_id, ref_id, estado, retraccion, veredicto):
    expr = "SET #estado = :e, estaRetractada = :r, verificada = :v"
    vals = {
        ":e": estado,
        ":r": bool(retraccion.get("retractada")),
        ":v": bool(retraccion.get("verificada")),
    }
    if veredicto is not None:
        expr += ", veredictoLLM = :vd"
        vals[":vd"] = veredicto
    tabla.update_item(
        Key={"PK": f"MANUSCRIPT#{manuscript_id}", "SK": f"REF#{ref_id}"},
        UpdateExpression=expr,
        ExpressionAttributeNames={"#estado": "estado"},
        ExpressionAttributeValues=vals,
    )


def _sumar_procesada(manuscript_id):
    """Contador atomico: cada Worker suma 1, sin pisarse entre invocaciones."""
    tabla.update_item(
        Key={"PK": f"MANUSCRIPT#{manuscript_id}", "SK": "METADATA"},
        UpdateExpression="ADD refsProcesadas :uno",
        ExpressionAttributeValues={":uno": 1},
    )
