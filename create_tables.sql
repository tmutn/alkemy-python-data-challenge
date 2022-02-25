DROP TABLE IF EXISTS info_normalizada;
DROP TABLE IF EXISTS info_registros;
DROP TABLE IF EXISTS info_cines;

CREATE TABLE info_normalizada (
    cod_localidad INT,
	id_provincia INT,
	id_departamento INT,
	categoria TEXT,
	provincia TEXT,
	localidad TEXT,
	nombre TEXT,
	domicilio TEXT,
	codigo_postal TEXT,
	numero_de_telefono TEXT,
	mail TEXT, 
    web TEXT,
    fecha_actualizacion TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE info_registros (
    categoria TEXT,
    provincia TEXT,
    cantidad_total INT,
	fuente	TEXT,
    fecha_actualizacion TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE info_cines (
	provincia TEXT,
	cant_pantallas INT,
	cant_butacas INT,
	espacios_incaa TEXT,
	fecha_actualizacion TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);