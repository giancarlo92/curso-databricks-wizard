-- ═══════════════════════════════════════════════════════════════════════════
-- WIZARD BANK · Módulo de Lending
-- Fuente transaccional del proyecto integrador — Data Wizard Academy
--
-- Motor:  Azure SQL Database  ·  Schema: lending  ·  8 tablas
--         (FUENTE PRINCIPAL DEL CURSO)
--
-- Por qué Azure SQL y no PostgreSQL:
--   · El conector de SQL Server en Lakeflow Connect está GA; el de
--     PostgreSQL sigue en Public Preview y requiere enrolamiento.
--   · Soporta Change Tracking Y CDC, lo que permite enseñar ambas
--     estrategias de ingesta incremental (Sesiones 9 y 12).
--   · Es coherente con un curso "Databricks + Fabric sobre Azure", y
--     habilita la demo de mirroring hacia Fabric en la Sesión 26.
--
--   Existe la variante wizard_bank_lending_postgres.sql como fallback
--   para quien no tenga presupuesto de Azure (levantable en Docker),
--   pero con ella NO se puede hacer el lab de Lakeflow Connect.
--
-- Uso:
--   sqlcmd -S <servidor>.database.windows.net -d wizardbank -U <user> -G \
--          -i wizard_bank_lending_azuresql.sql
-- ═══════════════════════════════════════════════════════════════════════════

-- ── Schema ────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'lending')
    EXEC('CREATE SCHEMA lending');
GO

-- ═══════════════════════════════════════════════════════════════════════════
-- NOTA SOBRE LAS COLUMNAS DE FECHA
--
-- Cada tabla lleva dos tipos de fecha, y NO son intercambiables:
--
--   · Fecha de NEGOCIO   → cuándo ocurrió el hecho (fecha_hora_solicitud,
--     fecha_hora_desembolso, fecha_registro...). No cambian nunca.
--     Sirven para analítica y particionamiento.
--
--   · Fecha de AUDITORÍA → cuándo tocamos la fila (fecha_creacion,
--     fecha_actualizacion). Mantenidas por trigger.
--     Sirven para DETECTAR CAMBIOS.
--
-- fecha_actualizacion es la CURSOR COLUMN que consume el query-based
-- connector de Lakeflow Connect (Sesión 9). Si usaras la fecha de negocio,
-- una solicitud creada el lunes y aprobada el viernes nunca se releería:
-- su fecha de negocio sigue siendo lunes.
-- ═══════════════════════════════════════════════════════════════════════════


-- ── Países donde opera el banco ───────────────────────────────────────────
CREATE TABLE lending.paises (
    id_pais              SMALLINT      NOT NULL PRIMARY KEY,
    codigo_iso           CHAR(2)       NOT NULL UNIQUE,
    nombre_pais          NVARCHAR(60)  NOT NULL,
    moneda_codigo        CHAR(3)       NOT NULL,
    fecha_creacion       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    fecha_actualizacion  DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

INSERT INTO lending.paises (id_pais, codigo_iso, nombre_pais, moneda_codigo) VALUES
  (1, 'PE', N'Perú',     'PEN'),
  (2, 'CO', N'Colombia', 'COP'),
  (3, 'BO', N'Bolivia',  'BOB');
GO


-- ── Campañas de marketing ─────────────────────────────────────────────────
CREATE TABLE lending.campanias (
    id_campania          INT            NOT NULL PRIMARY KEY,
    id_pais              SMALLINT       NOT NULL
                                        REFERENCES lending.paises(id_pais),
    nombre_campania      NVARCHAR(120)  NOT NULL,
    tipo_campania        NVARCHAR(30)   NOT NULL,   -- Captacion|Reactivacion|Cross-sell
    fecha_inicio         DATE           NOT NULL,   -- fecha de NEGOCIO
    fecha_fin            DATE,                      -- fecha de NEGOCIO
    presupuesto          DECIMAL(14,2),
    fecha_creacion       DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    fecha_actualizacion  DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
GO


-- ── Catálogo de productos (la tasa cambia → SCD2 en Silver) ───────────────
CREATE TABLE lending.productos_prestamo (
    id_producto          INT            NOT NULL PRIMARY KEY,
    id_pais              SMALLINT       NOT NULL
                                        REFERENCES lending.paises(id_pais),
    nombre_producto      NVARCHAR(120)  NOT NULL,
    tipo_producto        NVARCHAR(40)   NOT NULL,
    monto_minimo         DECIMAL(14,2)  NOT NULL,
    monto_maximo         DECIMAL(14,2)  NOT NULL,
    plazo_min_meses      SMALLINT       NOT NULL,
    plazo_max_meses      SMALLINT       NOT NULL,
    tasa_interes_anual   DECIMAL(6,3)   NOT NULL,
    es_activo            BIT            NOT NULL DEFAULT 1,
    fecha_creacion       DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    fecha_actualizacion  DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
GO


-- ── Maestro de clientes · contiene PII + atributos que cambian ────────────
CREATE TABLE lending.clientes (
    id_cliente                 BIGINT         NOT NULL PRIMARY KEY,
    id_pais                    SMALLINT       NOT NULL
                                              REFERENCES lending.paises(id_pais),
    tipo_documento             NVARCHAR(10)   NOT NULL,   -- DNI | CC | CI
    numero_documento           NVARCHAR(20)   NOT NULL,   -- PII
    nombres                    NVARCHAR(80)   NOT NULL,   -- PII
    apellidos                  NVARCHAR(80)   NOT NULL,   -- PII
    fecha_nacimiento           DATE           NOT NULL,   -- PII
    email                      NVARCHAR(120),             -- PII
    ciudad                     NVARCHAR(80),
    situacion_laboral          NVARCHAR(30),   -- cambia → SCD2
    ingreso_mensual_declarado  DECIMAL(14,2),  -- cambia → SCD2
    score_interno              SMALLINT,       -- cambia → SCD2
    nivel_riesgo               CHAR(1),        -- A (mejor) .. E (peor)
    id_campania_captacion      INT            REFERENCES lending.campanias(id_campania),
    fecha_registro             DATETIME2      NOT NULL,   -- fecha de NEGOCIO
    fecha_creacion             DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    fecha_actualizacion        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT uq_cliente_doc UNIQUE (id_pais, tipo_documento, numero_documento)
);
GO


-- ── Ofertas preaprobadas · entrada del funnel ─────────────────────────────
--    OJO: estado_oferta muta (Vigente → Expirada → Aceptada),
--    por eso necesita fecha_actualizacion como cursor column.
CREATE TABLE lending.ofertas_preaprobadas (
    id_oferta             BIGINT         NOT NULL PRIMARY KEY,
    id_cliente            BIGINT         NOT NULL
                                         REFERENCES lending.clientes(id_cliente),
    id_producto           INT            NOT NULL
                                         REFERENCES lending.productos_prestamo(id_producto),
    id_campania           INT            REFERENCES lending.campanias(id_campania),
    monto_ofertado        DECIMAL(14,2)  NOT NULL,
    plazo_meses_ofertado  SMALLINT       NOT NULL,
    tasa_ofertada         DECIMAL(6,3)   NOT NULL,
    motor_asignacion      NVARCHAR(40)   NOT NULL,  -- modelo_v1|modelo_v2 → A/B testing
    fecha_generacion      DATETIME2      NOT NULL,  -- fecha de NEGOCIO
    fecha_vigencia_fin    DATE           NOT NULL,
    estado_oferta         NVARCHAR(20)   NOT NULL,  -- Vigente|Expirada|Aceptada
    fecha_creacion        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    fecha_actualizacion   DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
GO


-- ── Solicitudes · cambian de estado → CDC + MERGE idempotente ─────────────
CREATE TABLE lending.solicitudes_prestamo (
    id_solicitud            BIGINT         NOT NULL PRIMARY KEY,
    id_oferta               BIGINT         REFERENCES lending.ofertas_preaprobadas(id_oferta),
    id_cliente              BIGINT         NOT NULL
                                           REFERENCES lending.clientes(id_cliente),
    id_producto             INT            NOT NULL
                                           REFERENCES lending.productos_prestamo(id_producto),
    canal                   NVARCHAR(20)   NOT NULL,  -- APP_IOS|APP_ANDROID|WEB
    monto_solicitado        DECIMAL(14,2)  NOT NULL,
    plazo_meses_solicitado  SMALLINT       NOT NULL,
    fecha_hora_solicitud    DATETIME2      NOT NULL,  -- fecha de NEGOCIO
    estado_solicitud        NVARCHAR(25)   NOT NULL,  -- Iniciada|En evaluacion|Aprobada|Rechazada|Desistida
    -- evaluación de riesgo embebida (relación 1:1, no justifica tabla aparte)
    score_evaluacion        SMALLINT,
    decision_motor          NVARCHAR(20),             -- Aprobar|Rechazar|Revision manual
    monto_aprobado          DECIMAL(14,2),
    tasa_aprobada           DECIMAL(6,3),
    motivo_rechazo          NVARCHAR(120),
    fecha_hora_resolucion   DATETIME2,
    fecha_creacion          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    fecha_actualizacion     DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
GO


-- ── Desembolsos · cierra el funnel ────────────────────────────────────────
--    OJO: estado_desembolso muta (Procesando → Completado → Reversado).
CREATE TABLE lending.desembolsos (
    id_desembolso          BIGINT         NOT NULL PRIMARY KEY,
    id_solicitud           BIGINT         NOT NULL UNIQUE
                                          REFERENCES lending.solicitudes_prestamo(id_solicitud),
    id_cliente             BIGINT         NOT NULL
                                          REFERENCES lending.clientes(id_cliente),
    monto_desembolsado     DECIMAL(14,2)  NOT NULL,
    moneda                 CHAR(3)        NOT NULL,  -- PEN|COP|BOB
    tasa_aplicada          DECIMAL(6,3)   NOT NULL,
    plazo_meses            SMALLINT       NOT NULL,
    comision_cobrada       DECIMAL(12,2)  NOT NULL DEFAULT 0,
    cuenta_destino_masked  NVARCHAR(30),             -- PII parcial: ****1234
    fecha_hora_desembolso  DATETIME2      NOT NULL,  -- fecha de NEGOCIO
    estado_desembolso      NVARCHAR(20)   NOT NULL,  -- Completado|Fallido|Reversado
    referencia_bancaria    NVARCHAR(50),
    fecha_creacion         DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    fecha_actualizacion    DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
GO


-- ── Tipos de cambio · sin FK, se cruza por (fecha, moneda) en Gold ────────
CREATE TABLE lending.tipos_cambio (
    id_tipo_cambio       INT            NOT NULL PRIMARY KEY,
    fecha                DATE           NOT NULL,  -- fecha de NEGOCIO
    moneda_origen        CHAR(3)        NOT NULL,
    moneda_destino       CHAR(3)        NOT NULL DEFAULT 'USD',
    tasa_compra          DECIMAL(12,6)  NOT NULL,
    tasa_venta           DECIMAL(12,6)  NOT NULL,
    fecha_creacion       DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    fecha_actualizacion  DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT uq_tipo_cambio UNIQUE (fecha, moneda_origen, moneda_destino)
);
GO


-- ═══════════════════════════════════════════════════════════════════════════
-- TRIGGERS DE AUDITORÍA
--
-- SQL Server no tiene ON UPDATE CURRENT_TIMESTAMP: DEFAULT solo aplica en el
-- INSERT. Sin estos triggers fecha_actualizacion se congela, el query-based
-- connector deja de ver los cambios, y el fallo es invisible: no revienta
-- nada, simplemente faltan datos en Bronze.
--
-- Se escriben uno por tabla porque cada una tiene su propia PK para el JOIN
-- contra la tabla lógica `inserted`.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TRIGGER lending.trg_paises_actualizacion ON lending.paises AFTER UPDATE AS
BEGIN
    SET NOCOUNT ON;
    UPDATE t SET fecha_actualizacion = SYSUTCDATETIME()
    FROM lending.paises t INNER JOIN inserted i ON t.id_pais = i.id_pais;
END;
GO

CREATE TRIGGER lending.trg_campanias_actualizacion ON lending.campanias AFTER UPDATE AS
BEGIN
    SET NOCOUNT ON;
    UPDATE t SET fecha_actualizacion = SYSUTCDATETIME()
    FROM lending.campanias t INNER JOIN inserted i ON t.id_campania = i.id_campania;
END;
GO

CREATE TRIGGER lending.trg_productos_actualizacion ON lending.productos_prestamo AFTER UPDATE AS
BEGIN
    SET NOCOUNT ON;
    UPDATE t SET fecha_actualizacion = SYSUTCDATETIME()
    FROM lending.productos_prestamo t INNER JOIN inserted i ON t.id_producto = i.id_producto;
END;
GO

CREATE TRIGGER lending.trg_clientes_actualizacion ON lending.clientes AFTER UPDATE AS
BEGIN
    SET NOCOUNT ON;
    UPDATE t SET fecha_actualizacion = SYSUTCDATETIME()
    FROM lending.clientes t INNER JOIN inserted i ON t.id_cliente = i.id_cliente;
END;
GO

CREATE TRIGGER lending.trg_ofertas_actualizacion ON lending.ofertas_preaprobadas AFTER UPDATE AS
BEGIN
    SET NOCOUNT ON;
    UPDATE t SET fecha_actualizacion = SYSUTCDATETIME()
    FROM lending.ofertas_preaprobadas t INNER JOIN inserted i ON t.id_oferta = i.id_oferta;
END;
GO

CREATE TRIGGER lending.trg_solicitudes_actualizacion ON lending.solicitudes_prestamo AFTER UPDATE AS
BEGIN
    SET NOCOUNT ON;
    UPDATE t SET fecha_actualizacion = SYSUTCDATETIME()
    FROM lending.solicitudes_prestamo t INNER JOIN inserted i ON t.id_solicitud = i.id_solicitud;
END;
GO

CREATE TRIGGER lending.trg_desembolsos_actualizacion ON lending.desembolsos AFTER UPDATE AS
BEGIN
    SET NOCOUNT ON;
    UPDATE t SET fecha_actualizacion = SYSUTCDATETIME()
    FROM lending.desembolsos t INNER JOIN inserted i ON t.id_desembolso = i.id_desembolso;
END;
GO

CREATE TRIGGER lending.trg_tipos_cambio_actualizacion ON lending.tipos_cambio AFTER UPDATE AS
BEGIN
    SET NOCOUNT ON;
    UPDATE t SET fecha_actualizacion = SYSUTCDATETIME()
    FROM lending.tipos_cambio t INNER JOIN inserted i ON t.id_tipo_cambio = i.id_tipo_cambio;
END;
GO


-- ═══════════════════════════════════════════════════════════════════════════
-- ÍNDICES SOBRE LA CURSOR COLUMN  (Sesiones 9, 10 y 14)
-- El query-based connector filtra SIEMPRE por fecha_actualizacion.
-- Sin estos índices cada corrida haría un scan completo del origen.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE INDEX idx_paises_actualizacion       ON lending.paises(fecha_actualizacion);
CREATE INDEX idx_campanias_actualizacion    ON lending.campanias(fecha_actualizacion);
CREATE INDEX idx_productos_actualizacion    ON lending.productos_prestamo(fecha_actualizacion);
CREATE INDEX idx_clientes_actualizacion     ON lending.clientes(fecha_actualizacion);
CREATE INDEX idx_ofertas_actualizacion      ON lending.ofertas_preaprobadas(fecha_actualizacion);
CREATE INDEX idx_solicitudes_actualizacion  ON lending.solicitudes_prestamo(fecha_actualizacion);
CREATE INDEX idx_desembolsos_actualizacion  ON lending.desembolsos(fecha_actualizacion);
CREATE INDEX idx_tipos_cambio_actualizacion ON lending.tipos_cambio(fecha_actualizacion);
GO

-- Índices para joins frecuentes durante la ingesta
CREATE INDEX idx_ofertas_cliente      ON lending.ofertas_preaprobadas(id_cliente);
CREATE INDEX idx_solicitudes_cliente  ON lending.solicitudes_prestamo(id_cliente);
CREATE INDEX idx_solicitudes_oferta   ON lending.solicitudes_prestamo(id_oferta);
CREATE INDEX idx_desembolsos_cliente  ON lending.desembolsos(id_cliente);
GO


-- ═══════════════════════════════════════════════════════════════════════════
-- CHANGE TRACKING  ·  requisito del conector de Lakeflow Connect (Sesión 9)
--
-- Change Tracking (CT) registra QUÉ filas cambiaron y por qué operación,
-- pero NO guarda los valores anteriores. Es mucho más liviano que CDC.
--
--   · CT  → "la fila 4711 cambió"          → suficiente para Bronze/Silver
--   · CDC → "la fila 4711 pasó de X a Y"   → necesario si te importa el
--                                             estado intermedio (Sesión 12)
--
-- Lakeflow Connect usa CT o CDC según lo que tenga habilitado la fuente.
-- Habilitar CT es lo mínimo para que el conector gestionado funcione.
-- ═══════════════════════════════════════════════════════════════════════════

-- Nivel base de datos: retención de 7 días con limpieza automática
ALTER DATABASE wizardbank
    SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON);
GO

-- Nivel tabla: TRACK_COLUMNS_UPDATED permite saber qué columnas cambiaron
ALTER TABLE lending.paises               ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);
ALTER TABLE lending.campanias            ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);
ALTER TABLE lending.productos_prestamo   ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);
ALTER TABLE lending.clientes             ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);
ALTER TABLE lending.ofertas_preaprobadas ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);
ALTER TABLE lending.solicitudes_prestamo ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);
ALTER TABLE lending.desembolsos          ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);
ALTER TABLE lending.tipos_cambio         ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);
GO


-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFICACIÓN
-- ═══════════════════════════════════════════════════════════════════════════

SELECT
    t.name                                        AS tabla,
    SUM(p.rows)                                   AS filas,
    MAX(CASE WHEN ct.object_id IS NOT NULL THEN 'ON' ELSE 'OFF' END) AS change_tracking
FROM sys.tables t
JOIN sys.schemas s              ON s.schema_id = t.schema_id
JOIN sys.partitions p           ON p.object_id = t.object_id AND p.index_id IN (0,1)
LEFT JOIN sys.change_tracking_tables ct ON ct.object_id = t.object_id
WHERE s.name = 'lending'
GROUP BY t.name
ORDER BY t.name;
GO
