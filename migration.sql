CREATE OR REPLACE FUNCTION removeInvalidNullFields() RETURNS VOID as $$
	
	BEGIN
	DELETE FROM recorrido_temp
	WHERE id_usuario isNULL or fecha_hora_retiro isNULL or origen_estacion
 	isNULL or destino_estacion isNULL or tiempo_uso isNULL;
END; 
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION castTimeUsedToInterval() RETURNS VOID as $$
	
	BEGIN
	CREATE VIEW recorrido_view AS

select PERIODO,
ID_USUARIO,
FECHA_HORA_RETIRO,
ORIGEN_ESTACION,
NOMBRE_ORIGEN,
DESTINO_ESTACION,
NOMBRE_DESTINO,

((SUBSTRING(TIEMPO_USO, 0, position('H' in TIEMPO_USO)) || 'hours') :: interval
+ (SUBSTRING(TIEMPO_USO, position('H' in TIEMPO_USO) + 2, 
 position('M' in TIEMPO_USO) - (position('H' in TIEMPO_USO) + 2)) || 'min') :: interval
 +(SUBSTRING(TIEMPO_USO, position('M' in TIEMPO_USO) + 4, position('S' in TIEMPO_USO) - (position('M' in TIEMPO_USO) + 4)) || 'second'):: interval) as TIEMPO_USO
 
,FECHA_CREACION
from recorrido_temp
Where TIEMPO_USO not like '-%';
	
END; 
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION migracion () RETURNS VOID as $$
	BEGIN
	perform removeInvalidNullFields();
        perform castTimeUsedToInterval();
END; 
$$ LANGUAGE plpgsql;

select migracion();