from dotenv import load_dotenv
from flask import Flask, request, redirect, jsonify, url_for, make_response
from jobs import run_jobs
import os
import threading
from utils import get_public_ip

load_dotenv()

FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY")
API_KEY = os.getenv("API_KEY")

app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY

ip = get_public_ip()
print(ip)


@app.route('/bubble_sync', methods=['POST'])
def run_bubble_sync():
    api_key = request.headers.get('Authorization').split()[1]
    resp = {}
    if not api_key == API_KEY:
        status_code = 404
        resp['message'] = 'Unauthorized'
    else:
        thread = threading.Thread(target=run_jobs)
        thread.start()
        status_code = 200
        resp['message'] = 'Bubble sync started'
    return make_response(jsonify(resp['message']), status_code)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5005)
