# producer/state_manager.py
import json
import os
import time  # ← NECESARIO para time.time()
class ProducerStateManager:
    """Manejador de estado del producer"""
    
    STATE_FILE = "producer_state.json"
    
    def __init__(self):
        self.state = self._load_state()
    
    def _load_state(self):
        """Cargar estado desde archivo"""
        if os.path.exists(self.STATE_FILE):
            try:
                with open(self.STATE_FILE, 'r') as f:
                    return json.load(f)
            except:
                pass
        
        # Estado inicial
        return {
            'sensors': {
                'aire': {'last_index': 0, 'total_sent': 0},
                'sonido': {'last_index': 0, 'total_sent': 0},
                'agua': {'last_index': 0, 'total_sent': 0}
            },
            'last_run': None
        }
    
    def save_state(self):
        """Guardar estado a archivo"""
        with open(self.STATE_FILE, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def update_sensor_state(self, sensor_type, last_index, sent_count):
        """Actualizar estado de un sensor"""
        if sensor_type not in self.state['sensors']:
            self.state['sensors'][sensor_type] = {'last_index': 0, 'total_sent': 0}
        
        self.state['sensors'][sensor_type]['last_index'] = last_index
        self.state['sensors'][sensor_type]['total_sent'] += sent_count
        self.state['last_run'] = time.time()
        self.save_state()
    
    def get_sensor_state(self, sensor_type):
        """Obtener estado de un sensor"""
        return self.state['sensors'].get(sensor_type, {'last_index': 0, 'total_sent': 0})