from flask import Flask, request, jsonify
import requests
import smtplib
from email.mime.text import MIMEText
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from init_db import NotificationLog, Base
import datetime

app = Flask(__name__)

# Config
CUSTOMER_SERVICE_URL = "http://customer-service:8000/customers"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

# Database setup
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "notifications")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def get_customer_email(customer_id):
    try:
        response = requests.get(f"{CUSTOMER_SERVICE_URL}/{customer_id}")
        response.raise_for_status()
        return response.json().get("email")
    except requests.RequestException as e:
        print(f"Error fetching customer email: {e}")
        return None

def log_notification(customer_id, txn_id, status, email, message=None):
    session = SessionLocal()
    try:
        log_entry = NotificationLog(
            customer_id=customer_id,
            txn_id=txn_id,
            status=status,
            email=email,
            message=message,
            created_at=datetime.datetime.utcnow()
        )
        session.add(log_entry)
        session.commit()
    except Exception as e:
        print(f"Error logging notification: {e}")
    finally:
        session.close()

def send_email(to_email, subject, body):
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = SMTP_USER
        msg['To'] = to_email

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, to_email, msg.as_string())

        return True
    except Exception as e:
        print(f"Email send failed: {e}")
        return False

@app.route("/notify", methods=["POST"])
def notify_transaction():
    payload = request.json
    customer_id = payload.get("customer_id")
    account_id = payload.get("account_id")
    txn_id = payload.get("txn_id")
    reference = payload.get("reference")
    status = payload.get("status")

    if not customer_id or not txn_id:
        return jsonify({"error": "Missing required fields"}), 400

    email = get_customer_email(customer_id)
    if not email:
        log_notification(customer_id, txn_id, "failed", "", "Customer email not found")
        return jsonify({"error": "Could not fetch customer email"}), 500

    subject = f"Transaction {status.capitalize()} - {txn_id}"
    body = f"Dear Customer,\n\nYour transaction with ID {txn_id} and reference {reference} has been {status}.\nAccount ID: {account_id}\n\nThank you."

    if send_email(email, subject, body):
        log_notification(customer_id, txn_id, "success", email, body)
        return jsonify({"message": "Notification sent successfully"}), 200
    else:
        log_notification(customer_id, txn_id, "failed", email, "Email send failed")
        return jsonify({"error": "Failed to send email"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=True)
