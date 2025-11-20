from datetime import datetime

def normalize_for_supabase(value):
    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, dict):
        return {k: normalize_for_supabase(v) for k, v in value.items()}

    if isinstance(value, list):
        return [normalize_for_supabase(v) for v in value]

    return value


