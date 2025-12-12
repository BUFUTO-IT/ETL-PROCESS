-- init-scripts/02-permissions.sql
-- Otorgar permisos completos al usuario iot_user

GRANT ALL PRIVILEGES ON DATABASE iot_monitoring TO iot_user;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO iot_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO iot_user;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO iot_user;

-- Permisos para tablas futuras
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT ALL ON TABLES TO iot_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT ALL ON SEQUENCES TO iot_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT ALL ON FUNCTIONS TO iot_user;