import firebase_admin
from firebase_admin import credentials, firestore
import os

def init_firebase():
    """Inicializa Firebase solo una vez."""
    if not firebase_admin._apps:
        cred_path = os.getenv("FIREBASE_CREDENTIALS_PATH")
        if not cred_path:
            raise ValueError("FIREBASE_CREDENTIALS_PATH no está configurado en el .env")

        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)

    return firestore.client()


# Instancia global reutilizable
firebase_db = init_firebase()
