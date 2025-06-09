CREATE TABLE balanza_data (
    "ID Pesada" TEXT,
    "Cliente" TEXT,
    "CUIT Cliente" TEXT,
    "ATA" TEXT,
    "CUIT ATA" TEXT,
    "Contenedor" TEXT,
    "Entrada" TEXT,
    "Salida" TEXT,
    "Peso Bruto" TEXT,
    "Peso Tara" TEXT,
    "Peso Neto" TEXT,
    "Peso Mercadería" TEXT,
    "Tara CNT" TEXT,
    "Descripción" TEXT,
    "Patente Chasis" TEXT,
    "Patente Semi" TEXT,
    "Chofer" TEXT,
    "Tipo Doc" TEXT,
    "DNI" TEXT,
    "Observaciones" TEXT,
    "tipo_oper" TEXT,
    "Booking" TEXT,
    "Permiso Emb." TEXT,
    "Precinto" TEXT,
    "Estado" TEXT,
    PRIMARY KEY ("ID Pesada")
);

CREATE TABLE update_log (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    last_update TIMESTAMP NOT NULL
);