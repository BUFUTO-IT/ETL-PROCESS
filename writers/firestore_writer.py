from clients.firebase_client import firebase_db
from datetime import datetime, timezone

# ---------------------------------------------------------
# 🔧 Normalizador universal para Firestore
# Convierte datetime → ISO, y procesa dicts anidados
# ---------------------------------------------------------
def normalize_for_firestore(value):
    if isinstance(value, datetime):
        # Convertir a timestamp UTC válido para Firestore
        return value.replace(tzinfo=timezone.utc)
    
    if isinstance(value, dict):
        return {k: normalize_for_firestore(v) for k, v in value.items()}
    
    if isinstance(value, list):
        return [normalize_for_firestore(v) for v in value]
    
    return value


# ---------------------------------------------------------
# 🔥 Writer final
# ---------------------------------------------------------
class FirestoreWriter:

    @staticmethod
    def save_reading(tipo_sensor: str, data: dict):
        """
        Guarda datos en:
        sensores/{tipo_sensor}/lecturas/{autoID}
        con conversión automática de datetime.
        """

        try:
            cleaned_data = normalize_for_firestore(data)

            ref = (
                firebase_db
                .collection("sensores")
                .document(tipo_sensor)
                .collection("lecturas")
            )

            ref.add(cleaned_data)

            print(f"[Firestore] OK → sensores/{tipo_sensor}/lecturas")
            return True

        except Exception as e:
            print(f"[Firestore] ❌ Error → {e}")
            return False
