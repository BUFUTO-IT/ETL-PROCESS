from clients.supabase_client import supabase
from utils.normalizer import normalize_for_supabase

class SupabaseWriter:

    @staticmethod
    def save_sensor_record(sensor_type: str, data: dict):
        """
        Guarda un registro en Supabase en:
        → sensor_base_data
        → tabla específica según tipo de sensor
        """

        try:
            data = normalize_for_supabase(data)
            # ---------------------------
            # 1) INSERT EN TABLA BASE
            # ---------------------------
            base_payload = {
                "_id": data.get("_id"),
                "dev_addr": data.get("devAddr"),
                "deduplication_id": data.get("deduplicationId"),
                "time": data.get("time"),
                "device_class": data.get("device_class"),
                "tenant_name": data.get("tenant_name"),
                "device_name": data.get("device_name"),
                "location": data.get("location"),
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude"),
                "battery_level": data.get("battery_level"),
                "fcnt": data.get("fCnt"),
                "rssi": data.get("rssi"),
                "snr": data.get("snr"),
                "sensor_type": sensor_type
            }

            base_result = supabase.table("sensor_base_data").insert(base_payload)

            if not base_result.data:
                print("[Supabase] ❌ Error: no volvió base_id")
                print("Detalle:", base_result.raw)
                return False

            base_id = base_result.data[0]["id"]

            # ---------------------------
            # 2) TABLA ESPECÍFICA
            # ---------------------------
            if sensor_type == "air_quality":
                sensor_payload = {
                    "sensor_base_id": base_id,
                    "co2": data.get("co2"),
                    "temperature": data.get("temperature"),
                    "humidity": data.get("humidity"),
                    "pressure": data.get("pressure"),
                    "co2_status": data.get("co2_status"),
                    "temperature_status": data.get("temperature_status"),
                    "humidity_status": data.get("humidity_status"),
                    "pressure_status": data.get("pressure_status"),
                }
                supabase.table("air_quality_data").insert(sensor_payload)

            elif sensor_type == "sound":
                sensor_payload = {
                    "sensor_base_id": base_id,
                    "laeq": data.get("laeq"),
                    "lai": data.get("lai"),
                    "laimax": data.get("laimax"),
                    "sound_status": data.get("sound_status")
                }
                supabase.table("sound_data").insert(sensor_payload)

            elif sensor_type == "water":
                sensor_payload = {
                    "sensor_base_id": base_id,
                    "distance": data.get("distance"),
                    "position": data.get("position"),
                    "water_status": data.get("water_status")
                }
                supabase.table("water_data").insert(sensor_payload)

            print(f"[Supabase] OK → {sensor_type}")
            return True

        except Exception as e:
            print("[Supabase] ❌ Error:", e)
            return False
