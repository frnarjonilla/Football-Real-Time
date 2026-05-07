# ⚽ Football Data Pipeline: End-to-End con Microsoft Fabric & PySpark

[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-0078D4?style=for-the-badge&logo=microsoft&logoColor=white)](https://www.microsoft.com/en-us/microsoft-fabric)

Este proyecto implementa una arquitectura de datos completa para la extracción, procesamiento y almacenamiento de estadísticas de fútbol profesional. Utiliza **Microsoft Fabric** como ecosistema unificado y **PySpark** para el procesamiento distribuido.

## 🏗️ Arquitectura de Datos (Medallion)

El pipeline sigue el patrón de arquitectura Medallón para garantizar la calidad y trazabilidad del dato:

* **Bronze Layer:** Ingesta de datos crudos (JSON) directamente desde la API REST de `football-data.org` utilizando `requests` y almacenamiento temporal.
* **Silver Layer:** Normalización y limpieza con **PySpark SQL**. Se aplanan estructuras jerárquicas, se tipan los campos (fechas, enteros) y se renombran columnas para facilitar la analítica.
* **Gold/Serving:** Los datos finales se exponen en tablas **Delta** optimizadas para su consumo inmediato desde **Power BI** o herramientas de SQL.

## 🛠️ Stack Tecnológico

* **Ingeniería:** PySpark (Spark SQL), Python.
* **Plataforma:** Microsoft Fabric (Synapse Data Engineering).
* **Almacenamiento:** Lakehouse (Delta Lake format).
* **Fuente:** API Externa (Football-Data.org).

## 📊 Capacidades Técnicas Demostradas

* **Manejo de APIs:** Gestión de autenticación por tokens y manejo de respuestas HTTP/JSON.
* **Transformaciones con Spark:** Uso de `pyspark.sql.functions` para el aplanado de datos anidados y limpieza de esquemas complejos.
* **Evolución de Esquemas:** Implementación de sobreescritura controlada (`overwriteSchema`) para mantener la integridad del Lakehouse.
* **Documentación de Código:** Notebook organizado por celdas lógicas con comentarios técnicos.

## 🚀 Cómo utilizar este proyecto

1.  Clona este repositorio.
2.  Carga el archivo `.ipynb` en tu Workspace de **Microsoft Fabric**.
3.  Obtén una API Key gratuita en [Football-Data.org](https://www.football-data.org/).
4.  Configura la variable `mi_llave` en la primera celda del Notebook.
5.  Ejecuta el pipeline para generar las tablas `silver_partidos` en tu Lakehouse.

---
Desarrollado por Francisco Arjonilla - www.linkedin.com/in/francisco-arjonilla
