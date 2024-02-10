from flask import Blueprint, jsonify

api_blueprint = Blueprint('api', __name__)


@api_blueprint.route('/', methods=['GET'])
def hello():
    return jsonify({"message": "Hello from API!"})


@api_blueprint.route('/init_client', methods=['POST'])
def init_client():
    return 'Hello from API!'


@api_blueprint.route('/subscribe', methods=['POST'])
def init_client():
    return 'Hello from API!'