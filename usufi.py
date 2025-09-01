# ==============================================================
#  Archivo  : usufi.py
#  Autor    : Héctor Ceballos
#  Fecha    : 2025-08-31
#
#  Derechos Reservados © Héctor Ceballos.
#
#  Este código ha sido desarrollado íntegramente por el autor en
#  su domicilio particular y en su tiempo libre. No forma parte de
#  sus funciones ni obligaciones laborales, por lo que constituye
#  una obra personal y autónoma, protegida por la Ley de Propiedad
#  Intelectual de Chile (Ley N° 17.336) y tratados internacionales
#  aplicables.
#
#  Se prohíbe reproducir, distribuir, modificar o utilizar este
#  código, total o parcialmente, sin autorización previa y por
#  escrito del autor.
# ==============================================================

# --- Guardias contra doble ejecución ---
import os, sys, tempfile

# Evita que el flujo se ejecute si el módulo se importa/exec-uta otra vez
if not hasattr(sys.modules[__name__], "_USUFI_RUN_ONCE"):
    _USUFI_RUN_ONCE = True
else:
    sys.exit(0)

# Lock de proceso simple (por si se dispara dos veces al mismo tiempo)
_lock_path = os.path.join(tempfile.gettempdir(), "usufi.run.lock")
try:
    _lock_fd = os.open(_lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
except FileExistsError:
    # Ya hay otra instancia corriendo
    sys.exit(0)



import sqlalchemy
import pandas as pd
import glob
import sqlite3
from datetime import datetime
from datetime import date
import xlrd
import csv
import os
import numpy as np
from pathlib import Path
import pandas as pd
import re

def leer_csv_robusto(f):
	"""Intenta leer un CSV probando múltiples encodings y separadores."""
	encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]  # cp1252/latin-1 típicos en Windows
	seps = [None, ";", ",", "\t", "|"]  # autodetección y separadores comunes
	last_err = None
	for enc in encodings:
		for sep in seps:
			try:
				df = pd.read_csv(
					f,
					sep=sep,              # None => autodetecta con engine='python'
					engine="python",
					encoding=enc,
					on_bad_lines="skip"   # evita caída por filas mal formadas
				)
				print(f"   ✓ leído con encoding={enc}, sep={'auto' if sep is None else repr(sep)}")
				return df
			except Exception as e:
				last_err = e
				continue
	raise last_err if last_err else RuntimeError("No se pudo leer el CSV.")


def crea_base_datos(usufi):
	metadata = sqlalchemy.MetaData()
	engine = sqlalchemy.create_engine('sqlite:///usufi.db', echo=False)
	metadata = sqlalchemy.MetaData()

	OrdenDeCompra = sqlalchemy.Table(
		'usufi',
		metadata,
		sqlalchemy.Column('CodInstitucion', sqlalchemy.String),
		sqlalchemy.Column('RutInstitucion', sqlalchemy.String),
		sqlalchemy.Column('CuentaCorrienteNumero', sqlalchemy.String),
		sqlalchemy.Column('Region', sqlalchemy.String),
		sqlalchemy.Column('CodDepartamentosSENAME', sqlalchemy.String),
		sqlalchemy.Column('DepartamentoSENAME', sqlalchemy.String),
		sqlalchemy.Column('CodProyecto', sqlalchemy.String),
		sqlalchemy.Column('Proyecto', sqlalchemy.String),
		sqlalchemy.Column('RutNumeroProyecto', sqlalchemy.String),
		sqlalchemy.Column('Periodo', sqlalchemy.String),
		sqlalchemy.Column('FechaComprobante', sqlalchemy.String),
		sqlalchemy.Column('NroComprobante', sqlalchemy.String),
		sqlalchemy.Column('Correlativo', sqlalchemy.String),
		sqlalchemy.Column('Monto', sqlalchemy.Integer),
		sqlalchemy.Column('Destino', sqlalchemy.String),
		sqlalchemy.Column('Glosa', sqlalchemy.String),
		sqlalchemy.Column('NumeroCheque', sqlalchemy.String),
		sqlalchemy.Column('MedioDePago', sqlalchemy.String),
		sqlalchemy.Column('IdUsuarioActualizacion', sqlalchemy.String),
		sqlalchemy.Column('Usuario', sqlalchemy.String),
		sqlalchemy.Column('FechaActualizacion', sqlalchemy.String),
		sqlalchemy.Column('CodObjetivo', sqlalchemy.String),
		sqlalchemy.Column('Objetivo', sqlalchemy.String),
		sqlalchemy.Column('CodUso', sqlalchemy.String),
		sqlalchemy.Column('Uso', sqlalchemy.String),
		)

	metadata.create_all(engine)
	usufi.to_sql('usufi', engine, if_exists='replace', index=False)


def limpiar_data(usufi):
	# Palabras clave a borrar si aparecen
	palabras_clave = ["REM", "SUELD", "PAGO", "AJUST", "BONO", "AGUINALDO", "GUARDADORA", "ARRIENDO", "ANTICIPO", "HONORARIO", "PERSONAL", "BANCO", "TRANSFERENCIA", "FUNDACI", "FAE ", "IMPUESTOS", "CREDITO", "ADMINISTRACI", "BOLETA", "TESORERIA", "GENERAL", "SINDICATO", "CANC.", "TELEFONICA", "TGR", "LIQUIDACIONES", "REEMPLA", "SEGURO", "SALUD", "FINIQUITO", "PRESTAMOS", "%", "SII", "CORPORACI", "ADM", "PROFESIONAL", "ACHS", "CAJA", "LTDA", "LIMITADA", "TRABAJADOR", "RELIQUIDACI", "NOMINA", "CHILE", "SODIMAC", "S. A.", "AFP", "CENTR", "BH", "APOYO", "HON ", "AUX", "DIF.LIQ", "FINIQ", "SERVICIO", "ASESORIA", "MANTENCI", "JARDIN", "DESCUENTO", "CANCELA", "DIFERENCI", "RELATOR", "AUTOCUIDAD", "CAMBIO DE CERRADURA", "FACT","REPARACI", "PRESTAMO", "CUOTA","FACT", "CHEQUE", "STAMO", "PAG.", "SERV", "COMPARENDO", "HON.", "COORD", "GAS", "COOP", "ENE", "FEB", "MAR", "ABR", "MAY", "JUN", "JUL", "AGO", "SEP", "OCT", "NOV", "DIC", "RETENCI", "LEY", "CNICO", "HONOR", "TERMINO", "FERIADO", "nan", "PLAN", "HRS", "EVALUA", "MEJOR", "PRM", "CERRO", "EVALUA", "ASEO", "DEV", "PSICO", "TUTOR", "CONFIG", "RENCA", "GUARDA", "INST", "MANIPULA", "INST.", "TRANS", "PROPORCIONAL", "HONR", "REEMP", "BLTA", "FALP", "REPROCESOS", "PREVIRED", "SPE", "PROYECTO", "SINDIDEM", "F9", "FONASA", "IMPOSICION", "PREVIR", "TESOR", "COTIACIONES", "ALDEAS", "LIQUIDACI", "AGUINAL", "RLP", "RESIDENCIA", "INFANTILES", "PROVEEDORES"]

	# Columnas donde aplicar
	columnas_objetivo = ["Destino", "Glosa"]

	# Construir regex con las palabras clave (insensible a may/min)
	patron = re.compile("|".join(map(re.escape, palabras_clave)), flags=re.IGNORECASE)

	# Regex para limpiar prefijos
	prefijo_numerico = r'^\s*(?:\d+(?:[.,]\d+)?\s*)+'    # quita números iniciales (incluye decimales)
	prefijo_no_letra = r'^[^A-Za-zÁÉÍÓÚÑa-záéíóúñ]+'     # si todavía no arranca con letra
	espacios_colapsar = r'\s+'                           # colapsa espacios

	for col in columnas_objetivo:
		if col in usufi.columns:
			usufi[col] = (
				usufi[col]
				.astype(str)
				# Si contiene palabra clave -> vacío (OJO: patron es compilado, no usar case=...)
				.mask(usufi[col].astype(str).str.contains(patron, na=False), "")
				# Si no, limpiar números iniciales y normalizar espacios
				.str.replace(prefijo_numerico, "", regex=True)
				.str.replace(prefijo_no_letra, "", regex=True)
				.str.replace(espacios_colapsar, " ", regex=True)
				.str.strip()
			)

	# Crear la columna con el texto más largo entre "Destino" y "Glosa"
	usufi["LosNombres"] = usufi[["Destino", "Glosa"]].apply(
		lambda x: max(x, key=lambda v: len(str(v)) if pd.notna(v) else 0),
		axis=1
	)

	# Reordenar columnas: insertar justo después de "Glosa"
	cols = list(usufi.columns)
	pos = cols.index("Glosa") + 1  # posición después de Glosa

	# Mover "LosNombres" a esa posición
	cols.insert(pos, cols.pop(cols.index("LosNombres")))
	usufi = usufi[cols]
	#df["El_proyecto"] = df["CodProyecto"].astype(str) + " " + df["Institucion"].astype(str)

	crea_base_datos(usufi)


def tabla_dinamica():
	cnx = sqlite3.connect('usufi.db')
	consulta  = ' \
		SELECT \
			usufi.* \
		FROM \
			usufi \
		WHERE \
			"Region" = "REGIÓN METROPOLITANA DE SANTIAGO" \
			AND "Objetivo" = "GASTOS PERSONAL" \
			AND "Uso" != "IMPOSICIONES" \
			AND "Uso" != "IMPUESTOS 2ª CATEGORÍA" \
			AND "Uso" != "IMPUESTO UNICO" \
	'
	df = pd.read_sql_query(consulta, cnx)

	df["El_proyecto"] = df["CodProyecto"].astype(str) + " " + df["Institucion"].astype(str)

	tabla = pd.pivot_table(
		df,
		index=["Periodo", "LosNombres"],
		columns="El_proyecto",
		values="Monto",
		aggfunc="sum",
		fill_value=0,
		margins=True,
		margins_name="Total",
	)
	tabla.columns = tabla.columns.astype(str)

	# Ordenar columnas dejando Total al final
	cod_cols = [c for c in tabla.columns if c != "Total"]
	cod_cols_sorted = sorted(cod_cols, key=lambda s: s.lower())
	if "Total" in tabla.columns:
		tabla = tabla[cod_cols_sorted + ["Total"]]
	else:
		tabla = tabla[cod_cols_sorted]

	# 👉 'apariciones': cuántos CodProyecto tienen valor distinto de 0 en la fila
	apar = (tabla[cod_cols_sorted] != 0).sum(axis=1).astype(int)
	tabla["apariciones"] = apar

	# Reordenar para poner 'apariciones' inmediatamente después de 'Total'
	cols_final = cod_cols_sorted + (["Total"] if "Total" in tabla.columns else []) + ["apariciones"]
	tabla = tabla.reindex(columns=cols_final)
	tabla = tabla[(tabla["apariciones"] > 1) & (tabla["apariciones"] < 30)]

	today = date.today()
	#filename = today.strftime("%d-%b-%Y") + " - usufi.xlsx"
	filename = today.strftime("usufi " + "%d-%b-%Y") + ".xlsx"

	# Guardar el DataFrame usando context manager
	with pd.ExcelWriter(filename, engine='openpyxl') as writer:
		#df.to_excel(writer, sheet_name='Creado por Héctor Ceballos', index=False)
		tabla.to_excel(writer, sheet_name='Creado por Héctor Ceballos')


class Usufi(object):
	usufi = pd.DataFrame()
	ruta_archivos = os.path.join(os.getcwd(), "*.csv")
	for f in glob.glob(ruta_archivos, recursive=True):
		print('Procesando  : ', f)
		#df = pd.read_csv(f)
		df = leer_csv_robusto(f)
		usufi = pd.concat([usufi, df], ignore_index=True)

	print(usufi.columns)
	limpiar_data(usufi)
	tabla_dinamica()
	print("Listo")
	time.sleep(10)

if __name__ == '__main__':
	Usufi()
