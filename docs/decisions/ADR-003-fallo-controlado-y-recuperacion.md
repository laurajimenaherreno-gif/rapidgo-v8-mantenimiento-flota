# ADR-003 - Fallo controlado y recuperación mediante una variable de Airflow

## Contexto / problema

Durante la semana 3 trabajamos en la orquestación del proceso de mantenimiento de flota mediante Apache Airflow. Necesitábamos comprobar que el DAG `rapidgo_v8_control_flota` pudiera detectar fallos en la validación de calidad de los datos y evitar que las tareas posteriores continuaran procesando información cuando existiera un problema.

Para comprobar este comportamiento, realizamos una prueba de fallo controlado. Al activar el mecanismo de prueba, la tarea `calidad_datos` terminó en estado `failed`, mientras que las tres tareas dependientes quedaron en `upstream_failed`. Como resultado, no se generó el reporte. La primera ejecución terminó en estado fallido después de aproximadamente 45 segundos.

 <img width="1005" height="546" alt="image" src="https://github.com/user-attachments/assets/27c5262f-9340-441b-b1f4-e4c742d3ae96" />


## Opciones consideradas

1. **Variable de Airflow (`fallo_controlado`):** utilizar una variable que provoque un error intencional en `calidad_datos` cuando su valor sea `"si"`. Permite activar y desactivar la prueba desde la interfaz de Airflow sin modificar el código del DAG ni alterar los datos.

2. **Alterar un archivo del Data Lake:** modificar intencionalmente un archivo para provocar un error durante la validación. Aunque representa un escenario más cercano a un problema real de los datos, implica alterar información y realizar tareas adicionales para restaurarla.

3. **Modificar directamente el código del DAG:** introducir una condición que provoque el fallo. Esta alternativa requiere modificar y versionar el código únicamente para realizar la prueba.

## Decisión tomada

Elegimos la primera opción porque permite controlar el fallo de manera sencilla, repetible y reversible desde la interfaz de Airflow. Conservamos la configuración de dos reintentos automáticos con un intervalo de espera de 20 segundos. Cuando `fallo_controlado` tiene el valor `"si"`, la tarea `calidad_datos` genera intencionalmente un error. Al cambiarlo a `"no"`, la validación puede ejecutarse normalmente. Para recuperar el flujo, desactivamos la variable y limpiamos la tarea `calidad_datos` en la misma ejecución, permitiendo que Airflow volviera a ejecutarla y continuara con las tareas dependientes.

## Evidencia utilizada

En la segunda ejecución de prueba, `calidad_datos` falló en sus tres intentos y registró el mensaje `FALLO CONTROLADO`. El primer intento se registró a las 00:02:40 y el tercero a las 00:03:23, con una diferencia aproximada de 43 segundos, coherente con los dos intervalos de espera configurados de 20 segundos, considerando el tiempo de ejecución de la tarea.

 <img width="1005" height="540" alt="image" src="https://github.com/user-attachments/assets/b27b309e-2ab6-41ae-90e3-450edfa856c1" />


Después de cambiar la variable a `"no"` y limpiar la tarea, `calidad_datos` terminó correctamente en el intento número 4, con una duración de 2.037 segundos. Finalmente, las cinco tareas del DAG quedaron en estado `success`, completando el flujo y permitiendo la generación del reporte.

 <img width="1005" height="539" alt="image" src="https://github.com/user-attachments/assets/cf4f5f21-8cc5-4f0a-b012-5429d9000dab" />


## Pull Request
https://github.com/laurajimenaherreno-gif/rapidgo-v8-mantenimiento-flota/pull/33

## Consecuencias / trade-offs

Esta decisión permitió comprobar que Airflow controla las dependencias entre tareas y evita que el procesamiento continúe cuando falla una validación crítica. También conseguimos reproducir el escenario de error y recuperar el flujo sin modificar el código ni los datos del Data Lake. Como limitación, los reintentos automáticos no solucionan un error provocado por una variable que permanece activada, ya que la condición de fallo se mantiene. Esto genera tiempos de espera innecesarios y requiere intervención manual para desactivar la variable y recuperar la ejecución. Además, si la variable permanece accidentalmente en `"si"`, las siguientes ejecuciones del DAG también fallarán. Por esta razón, debe verificarse que quede desactivada después de realizar las pruebas.
