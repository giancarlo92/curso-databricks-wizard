/*
  WIZARD BANK - Cobranzas local
  Variante local del DDL de la Sesión 10.

  Este archivo se ejecuta contra SQL Server de Docker en la base master. No
  contiene endpoints Azure ni credenciales. El runner local lo aplica antes
  de invocar el generador de datos.
*/

IF SCHEMA_ID(N'cobranzas') IS NULL
    EXEC(N'CREATE SCHEMA cobranzas');
GO

IF OBJECT_ID(N'cobranzas.cuotas', N'U') IS NULL
BEGIN
    CREATE TABLE cobranzas.cuotas (
        id_cuota BIGINT NOT NULL IDENTITY(1,1) PRIMARY KEY,
        numero_credito BIGINT NOT NULL,
        numero_cuota SMALLINT NOT NULL,
        fecha_vencimiento DATE NOT NULL,
        monto_cuota DECIMAL(12,2) NOT NULL,
        monto_capital DECIMAL(12,2) NOT NULL,
        monto_interes DECIMAL(12,2) NOT NULL,
        estado_cuota NVARCHAR(20) NOT NULL,
        fecha_creacion DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        fecha_modificacion DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX ix_cuotas_credito ON cobranzas.cuotas(numero_credito);
    CREATE INDEX ix_cuotas_modificacion ON cobranzas.cuotas(fecha_modificacion);
END;
GO

IF OBJECT_ID(N'cobranzas.pagos', N'U') IS NULL
BEGIN
    CREATE TABLE cobranzas.pagos (
        id_pago BIGINT NOT NULL IDENTITY(1,1) PRIMARY KEY,
        numero_credito BIGINT NOT NULL,
        numero_cuota SMALLINT NOT NULL,
        fecha_pago DATETIME2 NOT NULL,
        monto_pagado DECIMAL(12,2) NOT NULL,
        medio_pago NVARCHAR(30) NOT NULL,
        fecha_creacion DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        fecha_modificacion DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX ix_pagos_credito ON cobranzas.pagos(numero_credito);
    CREATE INDEX ix_pagos_modificacion ON cobranzas.pagos(fecha_modificacion);
END;
GO

IF OBJECT_ID(N'cobranzas.gestiones_cobranza', N'U') IS NULL
BEGIN
    CREATE TABLE cobranzas.gestiones_cobranza (
        id_gestion BIGINT NOT NULL IDENTITY(1,1) PRIMARY KEY,
        numero_credito BIGINT NOT NULL,
        fecha_gestion DATETIME2 NOT NULL,
        tipo_gestion NVARCHAR(20) NOT NULL,
        resultado NVARCHAR(30) NOT NULL,
        dias_mora_al_momento SMALLINT NOT NULL,
        gestor NVARCHAR(60) NOT NULL,
        fecha_creacion DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        fecha_modificacion DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX ix_gestiones_credito ON cobranzas.gestiones_cobranza(numero_credito);
    CREATE INDEX ix_gestiones_modificacion ON cobranzas.gestiones_cobranza(fecha_modificacion);
END;
GO

IF OBJECT_ID(N'cobranzas.ctl_watermarks', N'U') IS NULL
BEGIN
    CREATE TABLE cobranzas.ctl_watermarks (
        tabla NVARCHAR(50) NOT NULL PRIMARY KEY,
        columna_watermark NVARCHAR(50) NOT NULL,
        valor_watermark DATETIME2 NOT NULL,
        fecha_actualizacion DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM cobranzas.ctl_watermarks WHERE tabla = N'cuotas')
    INSERT INTO cobranzas.ctl_watermarks (tabla, columna_watermark, valor_watermark)
    VALUES (N'cuotas', N'fecha_modificacion', '1900-01-01');
IF NOT EXISTS (SELECT 1 FROM cobranzas.ctl_watermarks WHERE tabla = N'pagos')
    INSERT INTO cobranzas.ctl_watermarks (tabla, columna_watermark, valor_watermark)
    VALUES (N'pagos', N'fecha_modificacion', '1900-01-01');
IF NOT EXISTS (SELECT 1 FROM cobranzas.ctl_watermarks WHERE tabla = N'gestiones_cobranza')
    INSERT INTO cobranzas.ctl_watermarks (tabla, columna_watermark, valor_watermark)
    VALUES (N'gestiones_cobranza', N'fecha_modificacion', '1900-01-01');
GO

CREATE OR ALTER PROCEDURE cobranzas.sp_actualizar_watermark
    @tabla NVARCHAR(50),
    @nueva_marca DATETIME2
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE cobranzas.ctl_watermarks
    SET valor_watermark = @nueva_marca,
        fecha_actualizacion = SYSUTCDATETIME()
    WHERE tabla = @tabla AND @nueva_marca IS NOT NULL;
END;
GO
