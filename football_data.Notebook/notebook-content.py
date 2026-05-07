# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6144e6dd-f07c-4a3b-ba0f-a0b271621d66",
# META       "default_lakehouse_name": "Lakehouse_futbol",
# META       "default_lakehouse_workspace_id": "27bd691f-29b2-4d3f-8fbd-3546bb0d7270",
# META       "known_lakehouses": [
# META         {
# META           "id": "6144e6dd-f07c-4a3b-ba0f-a0b271621d66"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests  # Esta librería sirve para "llamar" a la API
import pandas as pd # Esta sirve para ver los datos como una tabla de Excel

# 1. Tu llave mágica (pega aquí la que te llegó al correo)
mi_llave = "aqui tu api key"

# 2. La dirección de internet donde están los datos (Liga Española)
url = "https://api.football-data.org/v4/competitions/PD/standings"
cabecera = { 'X-Auth-Token': mi_llave }

# 3. Pedir los datos
respuesta = requests.get(url, headers=cabecera)
datos_sucios = respuesta.json()

# 4. Convertir esos datos en una tabla que podamos entender
# Extraemos solo la parte de la 'tabla de posiciones'
tabla_posiciones = datos_sucios['standings'][0]['table']
df = pd.json_normalize(tabla_posiciones)

# 5. Ver el resultado
print("¡Éxito! Aquí tienes la liga:")
display(df[['position', 'team.name', 'playedGames', 'points']])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_spark = spark.createDataFrame(df)

# Guardamos la tabla con el nombre "tabla_liga_espanola"
df_spark.write.mode("overwrite").format("delta").saveAsTable("tabla_liga_espanola")

print("¡Datos guardados con éxito en el Lakehouse!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Cambiamos la dirección para pedir "goleadores" (scorers)
url_goleadores = "https://api.football-data.org/v4/competitions/PD/scorers"

# 2. Llamamos a la API
resp_goleadores = requests.get(url_goleadores, headers=cabecera)
datos_goleadores = resp_goleadores.json()

# 3. Limpiamos los datos (el nombre del jugador está dentro de 'player' y el equipo en 'team')
df_goleadores = pd.json_normalize(datos_goleadores['scorers'])

# 4. Guardamos en el Lakehouse como una tabla nueva
df_spark_goleadores = spark.createDataFrame(df_goleadores)
df_spark_goleadores.write.mode("overwrite").format("delta").saveAsTable("tabla_goleadores")

print("¡Tabla de goleadores guardada!")
display(df_goleadores.head(5)) # Vemos los 5 primeros

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Pedimos los partidos (matches)
url_partidos = "https://api.football-data.org/v4/competitions/PD/matches"

resp_partidos = requests.get(url_partidos, headers=cabecera)
datos_partidos = resp_partidos.json()

# 2. Esta tabla es más grande, así que la convertimos con cuidado
df_partidos = pd.json_normalize(datos_partidos['matches'])

# 3. Guardamos en el Lakehouse
df_spark_partidos = spark.createDataFrame(df_partidos)
df_spark_partidos.write.mode("overwrite").format("delta").saveAsTable("tabla_partidos")

print("¡Calendario de partidos guardado!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

# Leemos la tabla "bruta"
df_liga = spark.read.table("tabla_liga_espanola")

# Usamos ` ` (acentos graves) para las columnas que tienen puntos
df_liga_limpia = df_liga.select(
    col("`team.id`").alias("id_equipo"),
    col("position").cast("int"),
    col("`team.name`").alias("equipo"), # <-- Aquí estaba el truco
    col("playedGames").cast("int").alias("partidos_jugados"),
    col("won").cast("int").alias("victorias"),
    col("draw").cast("int").alias("empates"),
    col("lost").cast("int").alias("derrotas"),
    col("points").cast("int").alias("puntos"),
    col("goalsFor").cast("int").alias("goles_a_favor"),
    col("goalsAgainst").cast("int").alias("goles_en_contra"),
    col("`team.crest`").alias("url_escudo") # Añadimos el escudo por si lo quieres usar en Power BI
)

# Guardamos la versión limpia
df_liga_limpia.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_liga_posiciones")

print("¡Tabla de posiciones normalizada con éxito!")
display(df_liga_limpia.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
df_goleadores = spark.read.table("tabla_goleadores")

df_goleadores_limpia = df_goleadores.select(
    col("`team.id`").alias("id_equipo"),
    col("`player.id`").alias("id_jugador"),
    col("`player.name`").alias("jugador"),
    col("`player.nationality`").alias("nacionalidad"),
    col("`team.name`").alias("equipo"),
    col("goals").cast("int").alias("goles"),
    col("assists").cast("int").alias("asistencias")
)

df_goleadores_limpia.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_goleadores")
print("Tabla de goleadores normalizada.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import col

df_partidos = spark.read.table("tabla_partidos")

df_partidos_limpia = df_partidos.select(
    col("id").alias("id_partido"),
    col("`homeTeam.id`").alias("id_equipo_local"),    # <--- ¡El DNI del local!
    col("`awayTeam.id`").alias("id_equipo_visitante"), # <--- ¡El DNI del visitante!
    to_timestamp(col("utcDate")).alias("fecha_hora"),
    col("status").alias("estado"),
    col("matchday").cast("int").alias("jornada"),
    col("`homeTeam.name`").alias("equipo_local"),
    col("`awayTeam.name`").alias("equipo_visitante"),
    col("`score.fullTime.home`").cast("int").alias("goles_local"),
    col("`score.fullTime.away`").cast("int").alias("goles_visitante")
)

df_partidos_limpia.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_partidos")
print("Tabla de partidos normalizada.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
