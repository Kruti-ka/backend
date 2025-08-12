import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv("DATABASE_URL")
db = SQLAlchemy(app)

@app.route("/")
def home():
    return {"message": "Flask + PostgreSQL running on Render"}

if __name__ == "__main__":
    app.run()
