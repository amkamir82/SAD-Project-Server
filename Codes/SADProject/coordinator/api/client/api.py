from flask import Blueprint, jsonify

api_blueprint = Blueprint('api', __name__)


@api_blueprint.route('/init_client', methods=['POST'])
def init_client():
    return 'Hello from api!'


@api_blueprint.route('/subscribe', methods=['POST'])
def subscribe():
    return 'Hello from api!'
