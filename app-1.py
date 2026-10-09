from flask import Flask, request, jsonify
import requests
import os

app = Flask(__name__)

AEROSEND = "https://aerosend.bond/tgtonum.php"
ASTHA    = "https://astha-9vd8.onrender.com/tapi-e79c7312cf5329b07b0ad3ee2aeaea19"

@app.after_request
def cors(r):
    r.headers["Access-Control-Allow-Origin"]  = "*"
    r.headers["Access-Control-Allow-Headers"] = "*"
    r.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    return r

@app.route("/")
def index():
    return jsonify({"status": "online", "service": "tg-osint-proxy"})

@app.route("/lookup")
def lookup():
    tg_id = request.args.get("id", "").strip()
    if not tg_id.isdigit():
        return jsonify({"error": "invalid id"}), 400

    # aerosend
    try:
        r = requests.get(
            AEROSEND,
            params={"key": "crazy", "num": tg_id},
            timeout=10
        )
        j = r.json()
        if j.get("status") == "success":
            return jsonify({"src": "aerosend", "data": {
                "tg_id":        j.get("tg_id", tg_id),
                "mobile":       j.get("mobile"),
                "country_code": j.get("country_code"),
                "country":      j.get("country"),
            }})
    except Exception:
        pass

    # astha
    try:
        r = requests.get(
            ASTHA,
            params={"Astha": tg_id},
            timeout=30
        )
        j = r.json()
        res = j.get("result", {})
        if j.get("status") != "error" and res.get("number"):
            return jsonify({"src": "astha", "data": {
                "tg_id":        res.get("tg_id", tg_id),
                "mobile":       res.get("number"),
                "country_code": res.get("country_code"),
                "country":      res.get("country"),
            }})
    except Exception:
        pass

    return jsonify({"error": "not found"}), 404

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
