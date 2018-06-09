create table recorrido_bridge(
PERIODO INTEGER,
ID_USUARIO INTEGER,
FECHA_HORA_RETIRO TIMESTAMP,
ORIGEN_ESTACION INTEGER,
NOMBRE_ORIGEN TEXT,
DESTINO_ESTACION INTEGER,
NOMBRE_DESTINO TEXT,
TIEMPO_USO TEXT
);

CREATE TABLE recorrido_final
(periodo        TEXT,
usuario         INTEGER,
fecha_hora_ret  TIMESTAMP NOT NULL,
est_origen      INTEGER NOT NULL,
est_destino     INTEGER NOT NULL,
fecha_hora_dev TIMESTAMP NOT NULL CHECK(fecha_hora_dev >= fecha_hora_ret),
PRIMARY KEY(usuario,fecha_hora_ret));

create table recorrido_temp(
PERIODO INTEGER,
ID_USUARIO INTEGER,
FECHA_HORA_RETIRO TIMESTAMP,
ORIGEN_ESTACION INTEGER,
NOMBRE_ORIGEN TEXT,
DESTINO_ESTACION INTEGER,
NOMBRE_DESTINO TEXT,
TIEMPO_USO INTERVAL
);

CREATE OR REPLACE FUNCTION removeInvalidNullFieldsAndTimeUseInvalidFormat() RETURNS VOID as $$
	
	BEGIN
	INSERT INTO recorrido_bridge(PERIODO, ID_USUARIO, FECHA_HORA_RETIRO,
	ORIGEN_ESTACION, NOMBRE_ORIGEN, DESTINO_ESTACION, NOMBRE_DESTINO,
	TIEMPO_USO)
	SELECT PERIODO, ID_USUARIO, FECHA_HORA_RETIRO,
	ORIGEN_ESTACION, NOMBRE_ORIGEN, DESTINO_ESTACION, NOMBRE_DESTINO,
	TIEMPO_USO
	FROM recorrido_import
	WHERE id_usuario IS NOT NULL and fecha_hora_retiro IS NOT NULL and origen_estacion
	IS NOT NULL and destino_estacion IS NOT NULL and tiempo_uso IS NOT NULL 
	and (tiempo_uso LIKE '_H _MIN _SEG' or tiempo_uso LIKE '_H _MIN __SEG' or
	tiempo_uso LIKE '_H __MIN _SEG' or tiempo_uso LIKE '_H __MIN __SEG' or
	tiempo_uso LIKE '__H _MIN _SEG' or tiempo_uso LIKE '__H _MIN __SEG' or
	tiempo_uso LIKE '__H __MIN _SEG' or tiempo_uso LIKE '__H __MIN __SEG');
END; 
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION castTimeUsedToInterval() RETURNS VOID as $$
	
	BEGIN
	INSERT INTO recorrido_temp(PERIODO, ID_USUARIO, FECHA_HORA_RETIRO,
	ORIGEN_ESTACION, NOMBRE_ORIGEN, DESTINO_ESTACION, NOMBRE_DESTINO,
	TIEMPO_USO)
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
	from recorrido_bridge
	Where TIEMPO_USO not like '-%' and (ID_USUARIO, FECHA_HORA_RETIRO) NOT IN (SELECT ID_USUARIO, FECHA_HORA_RETIRO FROM recorrido_bridge
	GROUP BY ID_USUARIO, FECHA_HORA_RETIRO HAVING count(*) > 1 );
	
END; 
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION saveSecondTupple
(RUSUARIO_ID recorrido_temp.ID_USUARIO%TYPE,
RFECHA recorrido_temp.FECHA_HORA_RETIRO%TYPE) RETURNS VOID AS $$
DECLARE
CKEY CURSOR FOR
SELECT * FROM recorrido_bridge
where ID_USUARIO = RUSUARIO_ID and FECHA_HORA_RETIRO = RFECHA;

RCKEY RECORD;
BEGIN
  OPEN CKEY;
  FETCH CKEY INTO RCKEY;
  EXIT WHEN NOT FOUND;
  FETCH CKEY INTO RCKEY;
  INSERT INTO recorrido_temp(PERIODO, ID_USUARIO, FECHA_HORA_RETIRO,
	ORIGEN_ESTACION, NOMBRE_ORIGEN, DESTINO_ESTACION, NOMBRE_DESTINO,
	TIEMPO_USO) values(RCKEY.PERIODO, RCKEY.ID_USUARIO, RCKEY.FECHA_HORA_RETIRO, RCKEY. ORIGEN_ESTACION, RCKEY.NOMBRE_ORIGEN, RCKEY.DESTINO_ESTACION, RCKEY.NOMBRE_DESTINO, 
  ((SUBSTRING(RCKEY.TIEMPO_USO, 0, position('H' in RCKEY.TIEMPO_USO)) || 'hours') :: interval
	+ (SUBSTRING(RCKEY.TIEMPO_USO, position('H' in RCKEY.TIEMPO_USO) + 2, 
	 position('M' in RCKEY.TIEMPO_USO) - (position('H' in RCKEY.TIEMPO_USO) + 2)) || 'min') :: interval
	 +(SUBSTRING(RCKEY.TIEMPO_USO, position('M' in RCKEY.TIEMPO_USO) + 4, position('S' in RCKEY.TIEMPO_USO) - (position('M' in RCKEY.TIEMPO_USO) + 4)) || 'second'):: interval));
  CLOSE CKEY;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION removeRepeatedKeys() RETURNS VOID
AS $$
DECLARE
CKEYS CURSOR FOR
SELECT ID_USUARIO, FECHA_HORA_RETIRO FROM recorrido_bridge
GROUP BY ID_USUARIO, FECHA_HORA_RETIRO HAVING count(*) > 1;
RCKEYS RECORD;
BEGIN
  OPEN CKEYS;
  LOOP
    FETCH CKEYS INTO RCKEYS;
    EXIT WHEN NOT FOUND;
    PERFORM saveSecondTupple(RCKEYS.ID_USUARIO, RCKEYS.FECHA_HORA_RETIRO);
  END LOOP;
  CLOSE CKEYS;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION ISUM
(RUSER_ID recorrido_temp.id_usuario%TYPE) RETURNS VOID AS $$
DECLARE
CTRAIL CURSOR FOR
SELECT periodo, id_usuario, fecha_hora_retiro, origen_estacion,
destino_estacion, tiempo_uso
FROM recorrido_temp
WHERE RUSER_ID = id_usuario
ORDER BY fecha_hora_retiro ASC;

RCTRAIL RECORD;
STARTS  TIMESTAMP;
ENDS    TIMESTAMP;
GREATEST_TIME TIMESTAMP;
FIRST_STATION INTEGER;
LAST_STATION INTEGER;
FIRST_PERIOD TEXT;

BEGIN
  OPEN CTRAIL;
  FETCH CTRAIL INTO RCTRAIL;
  STARTS := RCTRAIL.fecha_hora_retiro;
  ENDS := RCTRAIL.fecha_hora_retiro + RCTRAIL.tiempo_uso;
  FIRST_STATION := RCTRAIL.origen_estacion;
  LAST_STATION := RCTRAIL.destino_estacion;
  FIRST_PERIOD := RCTRAIL.periodo;
  LOOP
    FETCH CTRAIL INTO RCTRAIL;
    EXIT WHEN NOT FOUND;
    IF ENDS >= RCTRAIL.fecha_hora_retiro THEN
      LAST_STATION := RCTRAIL.destino_estacion;
      ENDS := RCTRAIL.fecha_hora_retiro + RCTRAIL.tiempo_uso;
    ELSE
      INSERT INTO recorrido_final(periodo, usuario, fecha_hora_ret,
      est_origen, est_destino, fecha_hora_dev)
      values(FIRST_PERIOD, RUSER_ID, STARTS, FIRST_STATION, LAST_STATION, ENDS);
      STARTS := RCTRAIL.fecha_hora_retiro;
      ENDS := RCTRAIL.fecha_hora_retiro + RCTRAIL.tiempo_uso;
      FIRST_STATION := RCTRAIL.origen_estacion;
      LAST_STATION := RCTRAIL.destino_estacion;
      FIRST_PERIOD := RCTRAIL.periodo;
    END IF;
  END LOOP;
  INSERT INTO recorrido_final(periodo, usuario, fecha_hora_ret,
  est_origen, est_destino, fecha_hora_dev)
  values(FIRST_PERIOD, RUSER_ID, STARTS, FIRST_STATION, LAST_STATION, ENDS);
  CLOSE CTRAIL;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION removeIntervalOverlap() RETURNS VOID
AS $$
DECLARE
CUSER CURSOR FOR
SELECT id_usuario FROM recorrido_temp
GROUP BY id_usuario;
RCUSER RECORD;
BEGIN
  OPEN CUSER;
  LOOP
    FETCH CUSER INTO RCUSER;
    EXIT WHEN NOT FOUND;
    PERFORM ISUM(RCUSER.id_usuario);
END LOOP;
  CLOSE CUSER;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION migracion () RETURNS VOID as $$
	BEGIN
	delete from recorrido_final;
	perform removeInvalidNullFieldsAndTimeUseInvalidFormat();
	perform castTimeUsedToInterval();
	perform removeRepeatedKeys();
	perform removeIntervalOverlap();
	drop table recorrido_bridge;
	drop table recorrido_temp;
	drop table recorrido_import;
END; 
$$ LANGUAGE plpgsql;


select migracion();

select * from recorrido_final

--select * from recorrido_final order by usuario ASC , fecha_hora_ret ASC;

