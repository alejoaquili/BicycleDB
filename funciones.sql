/************************************************* IMPORT-FILES *************************************************/
DROP TABLE recorrido_import;
SET DATESTYLE = DMY;
CREATE TABLE recorrido_import
(periodo          INTEGER,
id_usuario        INTEGER,
fecha_hora_retiro TIMESTAMP,
origen_estacion   INTEGER,
nombre_origen     TEXT,
destino_estacion  INTEGER,
nombre_destino    TEXT,
tiempo_uso        TEXT,
fecha_creacion    TEXT
);
\COPY recorrido_import FROM './CSV Files/test1.csv' CSV HEADER DELIMITER ';';

/************************************************* DEFINITIONS *************************************************/

CREATE TABLE recorrido_bridge
(periodo           INTEGER,
id_usuario        INTEGER,
fecha_hora_retiro TIMESTAMP,
origen_estacion   INTEGER,
nombre_origen     TEXT,
destino_estacion  INTEGER,
nombre_destino    TEXT,
tiempo_uso        TEXT
);

CREATE TABLE recorrido_final
(periodo        TEXT,
usuario         INTEGER,
fecha_hora_ret  TIMESTAMP NOT NULL,
est_origen      INTEGER NOT NULL,
est_destino     INTEGER NOT NULL,
fecha_hora_dev TIMESTAMP NOT NULL CHECK(fecha_hora_dev >= fecha_hora_ret),
PRIMARY KEY(usuario,fecha_hora_ret));

CREATE TABLE recorrido_temp(
periodo           INTEGER,
id_usuario        INTEGER,
fecha_hora_retiro TIMESTAMP,
origen_estacion   INTEGER,
nombre_origen     TEXT,
destino_estacion  INTEGER,
nombre_destino    TEXT,
tiempo_uso        INTERVAL,
PRIMARY KEY(id_usuario,fecha_hora_retiro)
);

/***************************************************** FUNCTIONS *****************************************************/

CREATE OR REPLACE FUNCTION cast_text_to_interval (
rtime_use IN recorrido_bridge.tiempo_uso%type
) RETURNS recorrido_temp.tiempo_uso%TYPE AS $$ 
DECLARE
new_interval recorrido_temp.tiempo_uso%TYPE;
BEGIN
    new_interval := (CAST((SUBSTRING(rtime_use, 0, POSITION('H' IN rtime_use)) || 'hours') AS INTERVAL)
    + CAST((SUBSTRING(rtime_use, POSITION('H' IN rtime_use) + 2, 
    POSITION('M' IN rtime_use) - (POSITION('H' IN rtime_use) + 2)) || 'min') AS INTERVAL)
    + CAST((SUBSTRING(rtime_use, POSITION('M' IN rtime_use) + 4, POSITION('S' IN rtime_use) - (POSITION('M' IN rtime_use) + 4)) || 'second') AS INTERVAL));
  RETURN new_interval;
 END;
  $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION remove_invalid_null_fields_and_time_used_invalid_format () RETURNS VOID 
AS $$
  BEGIN
  INSERT INTO recorrido_bridge (periodo, id_usuario, fecha_hora_retiro,
  origen_estacion, nombre_origen, destino_estacion, nombre_destino,
  tiempo_uso)
  SELECT periodo, id_usuario, fecha_hora_retiro,
  origen_estacion, nombre_origen, destino_estacion, nombre_destino,
  tiempo_uso
  FROM recorrido_import
  WHERE id_usuario IS NOT NULL AND fecha_hora_retiro IS NOT NULL and origen_estacion
  IS NOT NULL AND destino_estacion IS NOT NULL and tiempo_uso IS NOT NULL 
  AND (tiempo_uso LIKE '_H _MIN _SEG' or tiempo_uso LIKE '_H _MIN __SEG' or
  tiempo_uso LIKE '_H __MIN _SEG' or tiempo_uso LIKE '_H __MIN __SEG' or
  tiempo_uso LIKE '__H _MIN _SEG' or tiempo_uso LIKE '__H _MIN __SEG' or
  tiempo_uso LIKE '__H __MIN _SEG' or tiempo_uso LIKE '__H __MIN __SEG');
END; 
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION cast_time_used_to_interval () RETURNS VOID 
AS $$ 
  BEGIN
    INSERT INTO recorrido_temp (periodo, id_usuario, fecha_hora_retiro,
    origen_estacion, nombre_origen, destino_estacion, nombre_destino,
    tiempo_uso)
    select periodo,
    id_usuario,
    fecha_hora_retiro,
    origen_estacion,
    nombre_origen,
    destino_estacion,
    nombre_destino,
    cast_text_to_interval(tiempo_uso) as tiempo_uso
    FROM recorrido_bridge
    WHERE tiempo_uso NOT LIKE '-%' AND (id_usuario, fecha_hora_retiro) NOT IN (SELECT id_usuario, fecha_hora_retiro FROM recorrido_bridge
    GROUP BY id_usuario, fecha_hora_retiro HAVING COUNT(*) > 1 );
  END; 
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION save_second_tupple (rusuario_id recorrido_temp.ID_USUARIO%TYPE,
rfecha recorrido_temp.FECHA_HORA_RETIRO%TYPE) RETURNS VOID 
AS $$
DECLARE
  ckey CURSOR FOR
  SELECT * FROM recorrido_bridge
  WHERE id_usuario = rusuario_id AND fecha_hora_retiro = rfecha
  ORDER BY tiempo_uso ASC;
  rckey RECORD;
BEGIN
  OPEN ckey;
  FETCH ckey INTO rckey;
  FETCH ckey INTO rckey;
  INSERT INTO recorrido_temp (periodo, id_usuario, fecha_hora_retiro,
  origen_estacion, nombre_origen, destino_estacion, nombre_destino,
  tiempo_uso) VALUES(rckey.periodo, rckey.id_usuario, rckey.fecha_hora_retiro, rckey. origen_estacion, rckey.nombre_origen, rckey.destino_estacion, rckey.nombre_destino, 
        cast_text_to_interval(rckey.tiempo_uso));
  CLOSE ckey;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION remove_repeated_keys () RETURNS VOID
AS $$
DECLARE
  ckeys CURSOR FOR
  SELECT id_usuario, fecha_hora_retiro FROM recorrido_bridge
  GROUP BY id_usuario, fecha_hora_retiro HAVING COUNT(*) > 1;
  rckeys RECORD;
BEGIN
  OPEN ckeys;
  LOOP
    FETCH ckeys INTO rckeys;
    EXIT WHEN NOT FOUND;
    PERFORM save_second_tupple (rckeys.id_usuario, rckeys.fecha_hora_retiro);
  END LOOP;
  CLOSE ckeys;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION isum (ruser_id recorrido_temp.id_usuario%TYPE) RETURNS VOID 
AS $$
DECLARE
  ctrail CURSOR FOR
  SELECT periodo, id_usuario, fecha_hora_retiro, origen_estacion,
  destino_estacion, tiempo_uso
  FROM recorrido_temp
  WHERE ruser_id = id_usuario
  ORDER BY fecha_hora_retiro ASC;
  rctrail RECORD;
  starts  TIMESTAMP;
  ends    TIMESTAMP;
  greatest_time TIMESTAMP;
  first_station INTEGER;
  last_station INTEGER;
  first_period TEXT;
BEGIN
  OPEN ctrail;
  FETCH ctrail INTO rctrail;
  starts := rctrail.fecha_hora_retiro;
  ends := rctrail.fecha_hora_retiro + rctrail.tiempo_uso;
  first_station := rctrail.origen_estacion;
  last_station := rctrail.destino_estacion;
  first_period := rctrail.periodo;
  LOOP
    FETCH ctrail INTO rctrail;
    EXIT WHEN NOT FOUND;
    IF ends >= rctrail.fecha_hora_retiro THEN
      last_station := rctrail.destino_estacion;
      ends := rctrail.fecha_hora_retiro + rctrail.tiempo_uso;
    ELSE
      INSERT INTO recorrido_final(periodo, usuario, fecha_hora_ret,
      est_origen, est_destino, fecha_hora_dev)
      VALUES(first_period, ruser_id, starts, first_station, last_station, ends);
      starts := rctrail.fecha_hora_retiro;
      ends := rctrail.fecha_hora_retiro + rctrail.tiempo_uso;
      first_station := rctrail.origen_estacion;
      last_station := rctrail.destino_estacion;
      first_period := rctrail.periodo;
    END IF;
  END LOOP;
  INSERT INTO recorrido_final(periodo, usuario, fecha_hora_ret,
  est_origen, est_destino, fecha_hora_dev)
  VALUES(first_period, ruser_id, starts, first_station, last_station, ends);
  CLOSE ctrail;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION remove_interval_overlap () RETURNS VOID
AS $$
DECLARE
  cuser CURSOR FOR
  SELECT id_usuario FROM recorrido_temp
  GROUP BY id_usuario;
  rcuser RECORD;
BEGIN
  OPEN cuser;
  LOOP
    FETCH cuser INTO rcuser;
    EXIT WHEN NOT FOUND;
    PERFORM isum(rcuser.id_usuario);
END LOOP;
CLOSE cuser;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION migration () RETURNS VOID 
AS $$
  BEGIN
  DELETE FROM recorrido_final;
  PERFORM remove_invalid_null_fields_and_time_used_invalid_format ();
  PERFORM cast_time_used_to_interval ();
  PERFORM remove_repeated_keys ();
  PERFORM remove_interval_overlap ();
  DROP TABLE recorrido_bridge;
  DROP TABLE recorrido_temp;
  DROP TABLE recorrido_import;
END; 
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION validate_intervals () RETURNS TRIGGER 
AS $$
DECLARE
  cval CURSOR FOR
  SELECT periodo, usuario, fecha_hora_ret , est_origen,
  est_destino, fecha_hora_dev
  FROM recorrido_final
  WHERE usuario = new.usuario;
  rcval RECORD;
BEGIN
  OPEN cval;
  LOOP
      FETCH cval INTO rcval;
    EXIT WHEN NOT FOUND;
      IF (new.fecha_hora_ret >= rcval.fecha_hora_ret
          AND new.fecha_hora_ret <= rcval.fecha_hora_dev)
          OR (rcval.fecha_hora_ret >= new.fecha_hora_ret
          AND rcval.fecha_hora_ret <= new.fecha_hora_dev) THEN
        RAISE EXCEPTION 'INSERCION IMPOSIBLE POR SOLAPAMIENTO';
      END IF;
  END LOOP;
  CLOSE cval;
  RETURN new;
END;
$$ LANGUAGE PLPGSQL;

/************************************************* TRIGGERS **************************************************/

DROP TRIGGER IF EXISTS detecta_solapado ON recorrido_final;

SELECT migration ();

CREATE TRIGGER detecta_solapado BEFORE INSERT 
ON recorrido_final FOR EACH ROW
EXECUTE PROCEDURE validate_intervals ();

/************************************************* EXECUTION *************************************************/

-- MIGRATION
SELECT * FROM recorrido_final order by usuario asc, fecha_hora_ret asc;

-- TRIGGER-TEST
INSERT INTO recorrido_final VALUES('201601',8,'2016-01-18 16:28:00',23,23, '2016-01-18 20:28:00');
INSERT INTO recorrido_final VALUES('201601', 7410, '2016-09-29 11:30:00', 23, 23, '2016-09-29 11:32:00');
select * FROM recorrido_final;



