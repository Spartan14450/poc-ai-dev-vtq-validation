# ValidateVtqCsv/__init__.py

import json
import base64
import logging
import azure.functions as func

from . import validator


def _extract_csv_from_request(req: func.HttpRequest) -> str:
    """
    Try to extract the CSV text from the HTTP request.

    Supported:
      - JSON: { "csv": "<csv contents>" }
      - JSON: { "csv_base64": "<base64-encoded CSV>" }
      - Raw body: CSV text (text/csv, text/plain)
    """
    body_bytes = req.get_body() or b""

    # Try JSON first
    try:
        data = req.get_json()
    except ValueError:
        data = None

    if isinstance(data, dict):
        if "csv" in data and isinstance(data["csv"], str):
            return data["csv"]
        if "csv_base64" in data and isinstance(data["csv_base64"], str):
            return base64.b64decode(data["csv_base64"]).decode("utf-8-sig")

    # Fallback: treat whole body as CSV text
    return body_bytes.decode("utf-8-sig")


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("VTQ CSV validation function triggered.")

    try:
        csv_text = _extract_csv_from_request(req)
    except Exception as ex:
        logging.exception("Failed to read request body.")
        return func.HttpResponse(
            json.dumps(
                {
                    "error": "BadRequest",
                    "message": f"Could not read CSV from request: {str(ex)}",
                }
            ),
            status_code=400,
            mimetype="application/json",
        )

    if not csv_text.strip():
        return func.HttpResponse(
            json.dumps(
                {
                    "valid": False,
                    "errorCount": 1,
                    "errors": [
                        {
                            "rowNumber": 1,
                            "field": "*FILE*",
                            "errorCode": "EMPTY",
                            "message": "No CSV content provided in request.",
                            "value": "",
                        }
                    ],
                }
            ),
            status_code=200,
            mimetype="application/json",
        )

    errors = validator.validate_csv_text(csv_text)

    response_payload = {
        "valid": len(errors) == 0,
        "errorCount": len(errors),
        "errors": [
            {
                "rowNumber": e.row_number,
                "field": e.field,
                "errorCode": e.error_code,
                "message": e.message,
                "value": e.value,
            }
            for e in errors
        ],
    }

    return func.HttpResponse(
        json.dumps(response_payload, ensure_ascii=False),
        status_code=200,
        mimetype="application/json",
    )
