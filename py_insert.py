import os
import json
import logging
import snowflake.connector
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

# Load environment variables from new.env
load_dotenv("new.env")

logging.basicConfig(level=logging.WARNING)
snowflake.connector.paramstyle = 'qmark'

def load_private_key():
    """Load the private key from the specified file."""
    private_key_path = os.getenv("PRIVATE_KEY")
    if not private_key_path:
        raise ValueError("PRIVATE_KEY environment variable is not set.")

    try:
        with open(private_key_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None
            )
            return private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
    except Exception as e:
        raise ValueError(f"Error loading private key from {private_key_path}: {e}")

def connect_snow():
    """Connect to Snowflake using RSA authentication."""
    private_key = load_private_key()

    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            private_key=private_key,
            role="INGEST",
            database="INGEST",
            schema="INGEST",
            warehouse="INGEST",
            session_parameters={'QUERY_TAG': 'py-insert'}
        )
        logging.info("Successfully connected to Snowflake.")
        return conn
    except Exception as e:
        raise ConnectionError(f"Failed to connect to Snowflake: {e}")

# Use connect_snow() in your main script as before.