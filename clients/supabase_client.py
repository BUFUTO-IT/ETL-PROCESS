import os
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

class SupabaseTable:
    def __init__(self, table_name: str):
        self.table_name = table_name

    def insert(self, data: dict):
        url = f"{SUPABASE_URL}/rest/v1/{self.table_name}"

        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }

        resp = requests.post(url, json=data, headers=headers)

        class Result:
            def __init__(self, data, status):
                self.data = data if status in (200, 201) else None
                self.status = status
                self.raw = resp.text

        if resp.status_code not in (200, 201):
            print(f"❌ Supabase error → {self.table_name}: {resp.status_code} {resp.text}")
            return Result(None, resp.status_code)

        print(f"✅ Supabase OK → {self.table_name}")
        return Result(resp.json(), resp.status_code)


class SupabaseClient:
    def table(self, name: str):
        return SupabaseTable(name)


# ESTE es el cliente que importas en el writer
supabase = SupabaseClient()
