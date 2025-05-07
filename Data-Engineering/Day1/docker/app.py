from flask import Flask
from user.routes import user_bp

app = Flask(__name__)
app.register_blueprint(user_bp)

@app.route("/")
def index():
    return "Hello from Flask Docker App!"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

