from flask import Blueprint, request, jsonify

api_blueprint = Blueprint('api', __name__)


@api_blueprint.route('/init_client', methods=['GET'])
def init_client():
    remote_addr = (request.headers.environ["REMOTE_ADDR"], request.headers.environ["REMOTE_ADDR"])
    print(remote_addr)
    tmp = ["http://127.0.0.1:8000", "http://127.0.0.1:8001", "http://127.0.0.1:8002"]
    return jsonify(tmp)


@api_blueprint.route('/subscribe', methods=['POST'])
def subscribe():
    return 'Hello from api!'
