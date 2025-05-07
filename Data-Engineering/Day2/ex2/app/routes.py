from app import app
from flask import render_template, request, jsonify
from app.models import User
from app import db

@app.route('/')
def index():
    return "Hello, World!"

@app.route('/users', methods=['GET'])
def get_users():
    users = User.query.all()
    return jsonify([user.username for user in users])

@app.route('/add_user', methods=['POST'])
def add_user():
    username = request.form['username']
    email = request.form['email']
    new_user = User(username=username, email=email)
    db.session.add(new_user)
    db.session.commit()
    return jsonify({"message": "User added successfully!"})
