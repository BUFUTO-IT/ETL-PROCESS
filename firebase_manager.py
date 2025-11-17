import firebase_admin
from firebase_admin import credentials, firestore
import logging
from typing import Dict, Any, List
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class FirebaseManager:
    def __init__(self, credential_path=None):
        try:
            if not firebase_admin._apps:
                if credential_path and os.path.exists(credential_path):
                    cred = credentials.Certificate(credential_path)
                else:
                    # Buscar credenciales en variables de entorno o archivo por defecto
                    cred_path = os.getenv('FIREBASE_CREDENTIALS', 'firebase-credentials.json')
                    if os.path.exists(cred_path):
                        cred = credentials.Certificate(cred_path)
                    else:
                        raise FileNotFoundError("No se encontraron credenciales de Firebase")
                
                firebase_admin.initialize_app(cred)
            
            self.db = firestore.client()
            logger.info("✅ Conectado a Firebase Firestore")
            
        except Exception as e:
            logger.error(f"❌ Error conectando a Firebase: {e}")
            raise
    
    def insert_sensor_data(self, sensor_type: str, data: Dict[str, Any]) -> bool:
        """Inserta datos de sensor en Firestore"""
        try:
            # Convertir objetos datetime a string para Firestore
            firestore_data = self._prepare_data_for_firestore(data)
            
            # Crear referencia al documento
            doc_ref = self.db.collection(sensor_type).document(firestore_data.get('_id', ''))
            
            # Establecer datos con merge para actualizar si existe
            doc_ref.set(firestore_data, merge=True)
            
            logger.debug(f"🔥 Datos insertados en Firebase: {sensor_type} - {firestore_data.get('_id')}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error insertando en Firebase: {e}")
            return False
    
    def batch_insert_sensor_data(self, sensor_type: str, data_list: List[Dict[str, Any]]) -> int:
        """Inserta múltiples registros en lote"""
        try:
            batch = self.db.batch()
            success_count = 0
            
            for data in data_list:
                firestore_data = self._prepare_data_for_firestore(data)
                doc_ref = self.db.collection(sensor_type).document(firestore_data.get('_id', ''))
                batch.set(doc_ref, firestore_data, merge=True)
                success_count += 1
            
            batch.commit()
            logger.info(f"🔥 Lote insertado en Firebase: {success_count} registros")
            return success_count
            
        except Exception as e:
            logger.error(f"❌ Error en inserción por lote: {e}")
            return 0
    
    def _prepare_data_for_firestore(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepara datos para Firestore (convierte tipos no soportados)"""
        firestore_data = {}
        
        for key, value in data.items():
            if hasattr(value, '__dict__'):
                # Si es un objeto, convertirlo a dict
                firestore_data[key] = vars(value)
            elif isinstance(value, datetime):
                # Convertir datetime a string ISO
                firestore_data[key] = value.isoformat()
            else:
                firestore_data[key] = value
        
        # Añadir timestamp de Firebase
        firestore_data['firestore_created'] = firestore.SERVER_TIMESTAMP
        
        return firestore_data
    
    def get_sensor_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas de Firestore"""
        try:
            stats = {}
            collections = ['air_quality', 'sound', 'water']
            
            for collection in collections:
                docs = self.db.collection(collection).limit(1).get()
                stats[collection] = len(list(docs)) if docs else 0
            
            stats['total_records'] = sum(stats.values())
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo estadísticas de Firebase: {e}")
            return {}