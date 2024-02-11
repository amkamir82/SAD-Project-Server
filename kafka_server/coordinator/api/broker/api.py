from flask import Blueprint, jsonify

api_blueprint = Blueprint('api', __name__)


@api_blueprint.route('/init_broker', methods=['POST'])
def init_broker():
    return 'Hello from API!'
