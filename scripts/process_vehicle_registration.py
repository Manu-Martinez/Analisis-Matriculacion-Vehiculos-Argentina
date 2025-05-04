import pandas as pd
import glob
import re
import os
from pathlib import Path

# Definir directorio base del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / 'data'

# Años a procesar
YEARS = [2019, 2020, 2021, 2022, 2023, 2024]

# Columnas a eliminar
COLUMNS_TO_DROP = [
    'registro_seccional_codigo', 'registro_seccional_descripcion',
    'automotor_tipo_codigo', 'automotor_marca_codigo', 'automotor_modelo_codigo',
    'automotor_uso_codigo', 'titular_domicilio_localidad',
    'titular_pais_nacimiento', 'titular_porcentaje_titularidad',
    'titular_domicilio_provincia_id', 'titular_pais_nacimiento_id'
]

# Términos a buscar para filtrar filas
TERMINOS_A_BUSCAR = ["SIN ESPECIFICACION", "AFF", "ARMADO FUERA DE FABRICA", "NO POSEE", "SIN MARCA"]

# Diccionario de tipos de datos
DTYPE_DICT = {
    'titular_anio_nacimiento': 'Int64',
    'automotor_anio_modelo': 'Int64'
}

def check_criteria(row):
    """Verifica si al menos dos de las tres columnas contienen los términos."""
    count = 0
    for col in ['automotor_tipo_descripcion', 'automotor_marca_descripcion', 'automotor_modelo_descripcion']:
        if any(term in str(row[col]).upper() for term in TERMINOS_A_BUSCAR):
            count += 1
    return count >= 2

def remove_nuevo_nueva(text):
    """Elimina 'NUEVO' o 'NUEVA' y limpia espacios."""
    if pd.isna(text):
        return text
    text = str(text).upper()
    text = re.sub(r'\s*\(?(NUEVO|NUEVA)\)?\s*', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def clean_dataframe(df):
    """Aplica todas las limpiezas a un DataFrame."""
    # Eliminar columnas
    df = df.drop(columns=[col for col in COLUMNS_TO_DROP if col in df.columns])

    # Filtrar filas según el criterio
    df = df[~df.apply(check_criteria, axis=1)]

    # Limpiar columna 'automotor_modelo_descripcion'
    df['automotor_modelo_descripcion'] = df['automotor_modelo_descripcion'].apply(remove_nuevo_nueva)

    # Filtrar filas donde 'automotor_marca_descripcion' no sea "SIN MARCA"
    df = df[~df['automotor_marca_descripcion'].str.upper().str.contains('SIN MARCA', na=False)]

    # Convertir columnas a enteros
    for col in ['titular_anio_nacimiento', 'automotor_anio_modelo']:
        if col in df.columns:
            df[col] = df[col].astype('Int64')

    return df

def process_year(year):
    """Procesa todas las tablas de un año y genera un CSV consolidado."""
    input_dir = DATA_DIR / f'inscripciones_{year}'
    output_file = input_dir / f'inscripciones_{year}.csv'

    # Obtener todos los archivos CSV en la carpeta del año
    csv_files = glob.glob(str(input_dir / '*.csv'))

    if not csv_files:
        print(f"No se encontraron archivos CSV en {input_dir}")
        return

    # Lista para almacenar DataFrames
    dfs = []

    # Leer y limpiar cada archivo
    for file in csv_files:
        print(f"Procesando: {file}")
        try:
            df = pd.read_csv(file, low_memory=False, dtype=DTYPE_DICT)
            df_cleaned = clean_dataframe(df)
            dfs.append(df_cleaned)
        except Exception as e:
            print(f"Error al procesar {file}: {e}")

    if dfs:
        # Concatenar todos los DataFrames
        df_completo = pd.concat(dfs, ignore_index=True)
        # Guardar el archivo consolidado
        df_completo.to_csv(output_file, index=False)
        print(f"Archivo consolidado guardado: {output_file}")
    else:
        print(f"No se procesaron archivos para el año {year}")

def main():
    """Procesa todos los años."""
    for year in YEARS:
        print(f"\nProcesando datos del año {year}")
        process_year(year)

if __name__ == "__main__":
    main()