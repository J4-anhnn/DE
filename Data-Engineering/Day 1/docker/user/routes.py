from flask import Blueprint, request, jsonify
from sqlalchemy.exc import IntegrityError
from .models import User
from database import db_session

user_bp = Blueprint("user", __name__)


@user_bp.route("/users", methods=["POST"])
def create_user():
    data = request.get_json()

    if not all(k in data for k in ("username", "email")):
        return jsonify({"error": "Missing required fields"}), 400

    user = User(username=data["username"], email=data["email"])

    try:
        db_session.add(user)
        db_session.commit()
        return jsonify(user.to_dict()), 201
    except IntegrityError:
        db_session.rollback()
        return jsonify({"error": "Username or email already exists"}), 400


@user_bp.route("/users", methods=["GET"])
def get_users():
    users = User.query.all()
    return jsonify([user.to_dict() for user in users])


@user_bp.route("/users/<int:user_id>", methods=["GET"])
def get_user(user_id):
    user = User.query.get_or_404(user_id)
    return jsonify(user.to_dict())


@user_bp.route("/users/<int:user_id>", methods=["PUT"])
def update_user(user_id):
    user = User.query.get_or_404(user_id)
    data = request.get_json()

    try:
        if "username" in data:
            user.username = data["username"]
        if "email" in data:
            user.email = data["email"]

        db_session.commit()
        return jsonify(user.to_dict())
    except IntegrityError:
        db_session.rollback()
        return jsonify({"error": "Username or email already exists"}), 400


@user_bp.route("/users/<int:user_id>", methods=["DELETE"])
def delete_user(user_id):
    user = User.query.get_or_404(user_id)
    db_session.delete(user)
    db_session.commit()
    return "", 204
