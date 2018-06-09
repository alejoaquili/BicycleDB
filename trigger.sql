CREATE TRIGGER DETECTA_SOLAPADO BEFORE INSERT ON recorrido_final
FOR EACH ROW
EXECUTE PROCEDURE VALIDATE_INTERVALS();

CREATE OR REPLACE FUNCTION VALIDATE_INTERVALS() RETURNS Trigger AS $$
DECLARE
CTRAIL CURSOR FOR
SELECT periodo, id_usuario, fecha_hora_retiro, origen_estacion,
destino_estacion, tiempo_uso
FROM recorrido_temp;

RCTRAIL RECORD;

BEGIN

	OPEN CTRAIL;
	LOOP
  		FETCH CTRAIL INTO RCTRAIL;
    	IF new.fecha_hora_ret >= RCTRAIL.fecha_hora_ret 
    		AND new.fecha_hora_ret <= RCTRAIL.fecha_hora_dev
			Raise exception 'INSERCION IMPOSIBLE POR SOLAPAMIENTO';
		END IF;
	END LOOP;
	RETURN new;
END;
$$ LANGUAGE plpgsql;