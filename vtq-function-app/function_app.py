import json
import base64
import azure.functions as func

import validator

app = func.FunctionApp()


def extract_csv(req: func.HttpRequest) -> str:
    """
    Extract CSV content from the request.

    Supported:
      - JSON body: { "csv": "<csv-text>" }
      - JSON body: { "csv_base64": "<base64-of-csv>" }
      - Raw body:  CSV text (text/plain, text/csv, etc.)
    """
    body = req.get_body() or b""

    # Try JSON first
    try:
        data = req.get_json()
    except Exception:
        data = None

    if isinstance(data, dict):
        csv_text = data.get("csv")
        if isinstance(csv_text, str):
            return csv_text

        b64 = data.get("csv_base64")
        if isinstance(b64, str):
            try:
                return base64.b64decode(b64).decode("utf-8-sig")
            except Exception:
                pass

    # Fallback: treat entire body as CSV text
    return body.decode("utf-8-sig")


@app.function_name("validate_vtq")
@app.route(route="validate-vtq", methods=["POST"])
def validate_vtq(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered VTQ CSV validator.

    Returns JSON:
    {
      "valid": bool,
      "errorCount": int,
      "errors": [
        {
          "rowNumber": int,
          "field": str,
          "errorCode": str,
          "message": str,
          "value": str
        },
        ...
      ]
    }
    """
    csv_text = extract_csv(req)

    if not csv_text.strip():
        payload = {
            "valid": False,
            "errorCount": 1,
            "errors": [
                {
                    "rowNumber": 1,
                    "field": "*FILE*",
                    "errorCode": "EMPTY",
                    "message": "No CSV content provided in request.",
                    "value": ""
                }
            ]
        }
        return func.HttpResponse(
            json.dumps(payload),
            mimetype="application/json"
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
                "value": e.value
            }
            for e in errors
        ]
    }

    return func.HttpResponse(
        json.dumps(response_payload, ensure_ascii=False),
        mimetype="application/json"
    )
