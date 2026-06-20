import os
import re
import json
import urllib.parse

import boto3

# Carga el archivo .env si existe (VM / Docker / local).

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

QUEUE_URL = os.environ["QUEUE_URL"]
TABLE_NAME = os.environ["TABLE_NAME"]

s3 = boto3.client("s3")
sqs = boto3.client("sqs")
tabla = boto3.resource("dynamodb").Table(TABLE_NAME)

# Un DOI siempre empieza con "10." seguido de 4-9 digitos, una barra y mas texto.
DOI_RE = re.compile(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+", re.I)
# Encabezado de la seccion de referencias (en espanol o ingles, con o sin "#").
ENCABEZADO_REF = re.compile(r"^#{0,6}\s*(referenc|bibliograf)", re.I | re.M)


def handler(event, context):
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        tema, manuscript_id = _parse_key(key)
        md = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8", "ignore")

        referencias = _extraer_referencias(md)
        print(f"[{manuscript_id}] tema={tema} referencias_con_doi={len(referencias)}")

        _registrar_total(manuscript_id, tema, len(referencias))
        _guardar_refs_pendientes(manuscript_id, tema, referencias)
        _enviar_a_cola(manuscript_id, tema, referencias)

    return {"ok": True}


def _parse_key(key):
    """ 'Biologia/a1b2c3.md' -> ('Biologia', 'a1b2c3') """
    partes = key.split("/")
    tema = partes[-2] if len(partes) >= 2 else "General"
    manuscript_id = os.path.splitext(partes[-1])[0]
    return tema, manuscript_id


def _extraer_referencias(md):
    """Busca la seccion de referencias y devuelve solo las entradas con DOI."""
    m = ENCABEZADO_REF.search(md)
    bloque_refs = md[m.start():] if m else md
    cuerpo = md[:m.start()] if m else ""

    referencias = []
    i = 0
    for linea in bloque_refs.splitlines():
        entrada = linea.strip()
        if not entrada:
            continue
        doi_match = DOI_RE.search(entrada)
        if not doi_match:
            continue  # sin DOI no podemos verificar retraccion -> la saltamos en v1
        i += 1
        doi = doi_match.group(0).rstrip(".,);")
        contexto = _buscar_contexto(cuerpo, entrada) or entrada
        referencias.append({
            "refId": f"{i:04d}",
            "doi": doi,
            "citaCruda": entrada[:1000],
            "contexto": contexto[:1500],
        })
    return referencias


def _buscar_contexto(cuerpo, entrada):
    """Mejor esfuerzo: si la entrada empieza con un numero (estilo [12] o 12.),
    busca esa marca en el cuerpo del texto y devuelve un fragmento alrededor."""
    m = re.match(r"^\[?(\d{1,3})\]?[.\)]?\s", entrada)
    if not (cuerpo and m):
        return None
    n = m.group(1)
    for marca in (f"[{n}]", f"({n})"):
        pos = cuerpo.find(marca)
        if pos != -1:
            return cuerpo[max(0, pos - 300): pos + 300]
    return None


def _registrar_total(manuscript_id, tema, total):
    """Actualiza el item METADATA con el total de referencias a procesar.
    Reinicia los contadores a 0: registrar = empezar un proceso nuevo."""
    tabla.update_item(
        Key={"PK": f"MANUSCRIPT#{manuscript_id}", "SK": "METADATA"},
        UpdateExpression=(
            "SET tema = :t, totalRefs = :n, refsProcesadas = :z, "
            "refsRetractadas = :z, #estado = :e"
        ),
        ExpressionAttributeNames={"#estado": "estado"},
        ExpressionAttributeValues={":t": tema, ":n": total, ":z": 0, ":e": "PROCESANDO"},
    )


def _guardar_refs_pendientes(manuscript_id, tema, referencias):
    with tabla.batch_writer() as bw:
        for r in referencias:
            bw.put_item(Item={
                "PK": f"MANUSCRIPT#{manuscript_id}",
                "SK": f"REF#{r['refId']}",
                "tipo": "referencia",
                "tema": tema,
                "doi": r["doi"],
                "citaCruda": r["citaCruda"],
                "contexto": r["contexto"],
                "estado": "PENDIENTE",
            })


def _enviar_a_cola(manuscript_id, tema, referencias):
    """send_message_batch admite hasta 10 mensajes por llamada."""
    for i in range(0, len(referencias), 10):
        lote = referencias[i:i + 10]
        entries = [{
            "Id": r["refId"],
            "MessageBody": json.dumps({
                "manuscriptId": manuscript_id,
                "tema": tema,
                "refId": r["refId"],
                "doi": r["doi"],
                "citaCruda": r["citaCruda"],
                "contexto": r["contexto"],
            }),
        } for r in lote]
        sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=entries)
