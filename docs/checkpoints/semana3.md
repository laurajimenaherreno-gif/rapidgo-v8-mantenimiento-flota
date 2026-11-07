# Checkpoint Semana 3

## 1. Qué quedó funcionando esta semana
Ya tenemos el DAG rapidgo_v8_control_flota corriendo en Airflow, que ahora funciona dentro de docker-compose. Tiene 5 tareas (verificar_fuentes, calidad_datos, resumen_kpis, resumen_alertas y generar_reporte), con reintentos, XCom entre tareas y un reporte que se guarda en el Data Lake. Hicimos una tarea de calidad que frena el reporte si los datos no pasan, probamos un fallo controlado y su recuperación, y ampliamos el CI con el job validar-dag, que prueba el DAG con Airflow.

## 2. Valores reales de la ejecución
El DAG revisó 1248 filas de la capa processed y la calidad quedó aprobada, con 0 filas con nulos, 0 duplicados, 0 temperaturas fuera de rango y 0 km inconsistentes. Con el fallo controlado activo, la ejecución tardó 45 segundos en terminar como Fallido, y al recuperarla calidad_datos terminó bien en el intento 4, en 2.037 segundos.

## 3. Decisión técnica y por qué
Provocamos el fallo con una variable de Airflow llamada fallo_controlado, en vez de dañar datos o cambiar el código, porque se activa y se quita desde la interfaz sin tocar nada más. La decisión completa está en el ADR-003 (docs/decisions/ADR-003-fallo-controlado-y-recuperacion.md).

## 4. Problema encontrado y cómo se resolvió
- La tarea que falló fue calidad_datos, en sus 3 intentos.
- El mensaje real del log fue RuntimeError: FALLO CONTROLADO: la variable fallo_controlado vale 'si' (prueba de recuperacion).
- Las tareas afectadas fueron resumen_kpis, resumen_alertas y generar_reporte, que quedaron en upstream_failed, así que no se generó reporte.
- El cambio que resolvió el problema fue poner la variable en "no" y limpiar la tarea calidad_datos, que terminó bien en el intento 4 y dejó las tareas siguientes en success.

También vimos que los 2 reintentos no ayudan en este caso, porque el error se repite igual en cada intento y la ejecución tardó 45 segundos en marcarse como fallida. Quedó documentado en el ADR-003, en el PR https://github.com/laurajimenaherreno-gif/rapidgo-v8-mantenimiento-flota/pull/33.

## 5. Enlaces
- Issues: #13 (DAG de Airflow), #14 (fallo controlado, recuperación y CI ampliado) y #34 (checkpoint)
- Pull Requests: #30 (DAG de control de la flota), #31 (job validar-dag en CI) y #33 (ADR-003)
- GitHub Actions: pestaña Actions, workflow "CI RapidGo V8" (job validar-dag)
