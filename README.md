CREATE USER PRY2206_P6 IDENTIFIED BY "PRY2206.practica_6"
DEFAULT TABLESPACE "DATA"
TEMPORARY TABLESPACE "TEMP";
ALTER USER PRY2206_P6  QUOTA UNLIMITED ON DATA;
GRANT CREATE SESSION TO PRY2206_P6;
GRANT "RESOURCE" TO PRY2206_P6;
ALTER USER PRY2206_P6 DEFAULT ROLE "RESOURCE";

-------------------------------------------------------------------

SELECT * FROM GASTO_COMUN_PAGO_CERO;

SELECT 
    ANNO_MES_PCGC, 
    ID_EDIF, 
    NRO_DEPTO, 
    FECHA_DESDE_GC, 
    FECHA_HASTA_GC, 
    MULTA_GC 
FROM 
    GASTO_COMUN 
WHERE 
    ANNO_MES_PCGC = 202505  -- Solo del mes mayo 2025
    AND MULTA_GC >= 29509  -- Multas de 1 UF o más
ORDER BY 
    ID_EDIF, NRO_DEPTO;


BEGIN
    SP_PROC_GASTO_COMUN_PAGO_CERO(202505, 29509);
END;
/

DECLARE
    v_registros_actualizados NUMBER;  -- Variable para almacenar el resultado del parámetro OUT
BEGIN
    -- Llamar al procedimiento con el parámetro OUT
    sp_CalcularGastosComunes(202505, v_registros_actualizados);

    -- Mostrar el número de registros actualizados
    DBMS_OUTPUT.PUT_LINE('Registros actualizados: ' || v_registros_actualizados);
END;
/

------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE sp_CalcularGastosComunes(
    p_periodo_cobro NUMBER,  -- Año y mes de cobro, formato YYYYMM
    p_registros_actualizados OUT NUMBER  -- Devuelve la cantidad de registros actualizados
) AS
    v_error VARCHAR2(250);
    v_contador NUMBER := 0;  -- Contador de registros actualizados
BEGIN
    -- Recorrer todos los departamentos con gastos comunes en el período especificado
    FOR gc_record IN (
        SELECT nro_depto, id_edif, anno_mes_pcgc
        FROM GASTO_COMUN
        WHERE anno_mes_pcgc = p_periodo_cobro
    ) LOOP
        BEGIN
            -- Actualizar el monto total en GASTO_COMUN usando la función fn_CalcularTotalGastoComun
            UPDATE GASTO_COMUN
            SET MONTO_TOTAL_GC = fn_CalcularTotalGastoComun(gc_record.nro_depto, gc_record.id_edif, gc_record.anno_mes_pcgc)
            WHERE nro_depto = gc_record.nro_depto
            AND id_edif = gc_record.id_edif
            AND anno_mes_pcgc = gc_record.anno_mes_pcgc;

            -- Incrementar el contador de registros actualizados
            v_contador := v_contador + 1;

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('Advertencia: No se encontró registro para ' || gc_record.nro_depto || ', ' || gc_record.id_edif);
            WHEN OTHERS THEN
                v_error := SQLERRM;
                DBMS_OUTPUT.PUT_LINE('Error en sp_CalcularGastosComunes: ' || v_error);
        END;
    END LOOP;

    -- Asignar el total de registros actualizados al parámetro OUT
    p_registros_actualizados := v_contador;

    -- Confirmar cambios
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('GASTO_COMUN actualizado correctamente para el período ' || p_periodo_cobro);

EXCEPTION
    WHEN OTHERS THEN
        v_error := SQLERRM;
        DBMS_OUTPUT.PUT_LINE('Error general en sp_CalcularGastosComunes: ' || v_error);
        ROLLBACK;
END;

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE SP_PROC_GASTO_COMUN_PAGO_CERO (
    p_anno_mes_pcgc NUMBER,  -- Año y mes de cobro (ejemplo: 202505)
    p_valor_uf NUMBER        -- Valor de la UF para calcular multas
) AS
    v_error VARCHAR2(250);
    v_monto_total NUMBER;  -- Monto total del gasto común calculado
    v_fecha_corte DATE;     -- Fecha estimada para corte de suministros
    v_multa NUMBER;         -- Valor de la multa calculada
BEGIN
    -- Borrar datos previos para evitar duplicados en la tabla GASTO_COMUN_PAGO_CERO
    DELETE FROM GASTO_COMUN_PAGO_CERO WHERE anno_mes_pcgc = p_anno_mes_pcgc;

    -- Recorrer todos los departamentos que no han pagado el gasto común
    FOR depto_record IN (
        SELECT 
            gc.anno_mes_pcgc,
            gc.id_edif,
            e.nombre_edif,
            TO_CHAR(a.numrun_adm, '99G999G999') || '-' || a.dvrun_adm AS run_administrador,
            INITCAP(a.pnombre_adm) || ' ' || INITCAP(NVL(a.snombre_adm, '')) || ' ' || INITCAP(a.appaterno_adm) || ' ' || INITCAP(NVL(a.apmaterno_adm, '')) AS nombre_admnistrador,
            gc.nro_depto,
            TO_CHAR(r.numrun_rpgc, '99G999G999') || '-' || r.dvrun_rpgc AS run_responsable_pago_gc,
            INITCAP(r.pnombre_rpgc) || ' ' || INITCAP(NVL(r.snombre_rpgc, '')) || ' ' || INITCAP(r.appaterno_rpgc) || ' ' || INITCAP(NVL(r.apmaterno_rpgc, '')) AS nombre_responsable_pago_gc
        FROM GASTO_COMUN gc
        JOIN EDIFICIO e ON gc.id_edif = e.id_edif
        JOIN ADMINISTRADOR a ON e.numrun_adm = a.numrun_adm
        JOIN RESPONSABLE_PAGO_GASTO_COMUN r ON gc.numrun_rpgc = r.numrun_rpgc
        WHERE gc.anno_mes_pcgc = p_anno_mes_pcgc
        AND NOT EXISTS (
            SELECT 1 
            FROM PAGO_GASTO_COMUN p
            WHERE p.id_edif = gc.id_edif
            AND p.nro_depto = gc.nro_depto
            AND p.anno_mes_pcgc = p_anno_mes_pcgc - 1
        )
    ) LOOP
        -- Calcular el monto total del gasto común para este departamento utilizando la función
        v_monto_total := fn_CalcularTotalGastoComun(depto_record.nro_depto, depto_record.id_edif, depto_record.anno_mes_pcgc);

        -- Calcular la fecha de corte utilizando la función
        v_fecha_corte := fn_CalcularFechaCorte(depto_record.nro_depto, depto_record.id_edif, depto_record.anno_mes_pcgc);

        -- Calcular la multa utilizando la función
        v_multa := fn_CalcularMulta(depto_record.nro_depto, depto_record.id_edif, depto_record.anno_mes_pcgc, p_valor_uf);

        -- Insertar en la tabla GASTO_COMUN_PAGO_CERO con el monto total calculado y otros valores
        INSERT INTO GASTO_COMUN_PAGO_CERO (
            anno_mes_pcgc, 
            id_edif, 
            nombre_edif, 
            run_administrador, 
            nombre_admnistrador,
            nro_depto, 
            run_responsable_pago_gc, 
            nombre_responsable_pago_gc, 
            valor_multa_pago_cero, 
            observacion
        )
        VALUES (
            depto_record.anno_mes_pcgc,
            depto_record.id_edif,
            depto_record.nombre_edif,
            depto_record.run_administrador,
            depto_record.nombre_admnistrador,
            depto_record.nro_depto,
            depto_record.run_responsable_pago_gc,
            depto_record.nombre_responsable_pago_gc,
            v_multa,  -- Asignar el valor de la multa calculada
            'Se Realizara el Corte del Combustible y Agua a Contar del: ' || TO_CHAR(v_fecha_corte, 'DD/MM/YYYY')  -- Observación, por ejemplo, la fecha de corte
        );
    END LOOP;

    -- Confirmar cambios en la base de datos
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Proceso completado exitosamente para el período ' || p_anno_mes_pcgc);

EXCEPTION
    WHEN OTHERS THEN
        v_error := SQLERRM;
        DBMS_OUTPUT.PUT_LINE('Error en SP_PROC_GASTO_COMUN_PAGO_CERO: ' || v_error);
        ROLLBACK;
END;


-------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_CalcularFechaCorte(
    p_nro_depto IN NUMBER,
    p_id_edif IN NUMBER,
    p_anno_mes IN NUMBER  -- Formato YYYYMM
) RETURN DATE IS
    v_cantidad_meses_no_pagados NUMBER := 0;
    v_fecha_corte DATE := NULL;
    v_error VARCHAR2(250);
BEGIN
    BEGIN
        -- Contar los meses sin pago del departamento
        SELECT COUNT(*) INTO v_cantidad_meses_no_pagados
        FROM GASTO_COMUN gc
        WHERE gc.nro_depto = p_nro_depto
        AND gc.id_edif = p_id_edif
        AND NOT EXISTS (
            SELECT 1 FROM PAGO_GASTO_COMUN pg
            WHERE pg.nro_depto = gc.nro_depto
            AND pg.id_edif = gc.id_edif
            AND pg.anno_mes_pcgc = gc.anno_mes_pcgc
        )
        AND gc.anno_mes_pcgc < p_anno_mes;

        -- Definir la fecha de corte solo si hay más de un mes sin pago
        IF v_cantidad_meses_no_pagados > 1 THEN
            v_fecha_corte := ADD_MONTHS(TO_DATE(p_anno_mes || '01', 'YYYYMMDD'), 1);
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_fecha_corte := NULL;
        WHEN OTHERS THEN
            v_error := SQLERRM;
            DBMS_OUTPUT.PUT_LINE('Error en fn_CalcularFechaCorte: ' || v_error);
            RETURN NULL;
    END;

    RETURN v_fecha_corte;
END fn_CalcularFechaCorte;


-------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_CalcularMulta(
    p_nro_depto IN NUMBER,
    p_id_edif IN NUMBER,
    p_anno_mes IN NUMBER,  -- Formato YYYYMM
    p_valor_uf IN NUMBER   -- Necesario para calcular la multa
) RETURN NUMBER IS
    v_cantidad_meses_no_pagados NUMBER := 0;
    v_multa NUMBER := 0;
    v_error VARCHAR2(250);
BEGIN
    BEGIN
        -- Contar la cantidad de meses sin pago
        SELECT COUNT(*) INTO v_cantidad_meses_no_pagados
        FROM GASTO_COMUN gc
        WHERE gc.nro_depto = p_nro_depto
        AND gc.id_edif = p_id_edif
        AND NOT EXISTS (
            SELECT 1 FROM PAGO_GASTO_COMUN pg
            WHERE pg.nro_depto = gc.nro_depto
            AND pg.id_edif = gc.id_edif
            AND pg.anno_mes_pcgc = gc.anno_mes_pcgc
        )
        AND gc.anno_mes_pcgc < p_anno_mes;

        -- Aplicar la multa según la cantidad de meses sin pago
        IF v_cantidad_meses_no_pagados = 1 THEN
            v_multa := 2 * p_valor_uf;
        ELSIF v_cantidad_meses_no_pagados > 1 THEN
            v_multa := 4 * p_valor_uf;
        ELSE
            v_multa := 0;
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_multa := 0;
        WHEN OTHERS THEN
            v_error := SQLERRM;
            DBMS_OUTPUT.PUT_LINE('Error en fn_CalcularMulta: ' || v_error);
            RETURN 0;
    END;

    RETURN v_multa;
END fn_CalcularMulta;


-----------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_CalcularTotalGastoComun(
    p_nro_depto IN NUMBER,
    p_id_edif IN NUMBER,
    p_anno_mes IN NUMBER
) RETURN NUMBER IS
    v_monto_total NUMBER := 0;
    v_multa NUMBER := 0;
    v_error VARCHAR2(250);
BEGIN
    BEGIN
        -- Obtener la multa desde la tabla GASTO_COMUN_PAGO_CERO o calcularla
        SELECT COALESCE(valor_multa_pago_cero, 0) 
        INTO v_multa
        FROM GASTO_COMUN_PAGO_CERO
        WHERE nro_depto = p_nro_depto
        AND id_edif = p_id_edif
        AND anno_mes_pcgc = p_anno_mes;

        -- Calcular el monto total del gasto común incluyendo la multa
        SELECT (prorrateado_gc + fondo_reserva_gc + agua_individual_gc + 
                combustible_individual_gc + COALESCE(lavanderia_gc, 0) + 
                COALESCE(evento_gc, 0) + COALESCE(servicio_gc, 0)) + v_multa
        INTO v_monto_total
        FROM GASTO_COMUN
        WHERE nro_depto = p_nro_depto
        AND id_edif = p_id_edif
        AND anno_mes_pcgc = p_anno_mes;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_monto_total := 0;
        WHEN OTHERS THEN
            v_error := SQLERRM;
            DBMS_OUTPUT.PUT_LINE('Error en fn_CalcularTotalGastoComun: ' || v_error);
            RETURN 0;
    END;

    RETURN v_monto_total;
END fn_CalcularTotalGastoComun;
