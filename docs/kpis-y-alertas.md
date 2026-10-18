# KPIs y reglas de alerta - Variante 8: Mantenimiento de flota

## KPIs
| KPI | Definición | Ventana |
|---|---|---|
| Km acumulados | Kilometraje total por vehículo (último valor del odómetro) y suma total de la flota. También los km recorridos en la ventana (máximo - mínimo del kilometraje) | Tumbling de 5 min, por vehículo |
| Fallas por tipo | Cantidad de eventos con código de falla, agrupados por tipo de falla (MOTOR, LUBRICACION, ELECTRICO, COMBUSTIBLE) | Tumbling de 5 min |
| Vehículos en riesgo | Vehículos que cumplen al menos una condición: (a) km desde el último mantenimiento >= 90 % del intervalo, (b) temperatura del motor >= 100 °C, (c) falla de severidad CRITICA en la ventana | Ventana de 5 min |

## Reglas de alerta (umbrales iniciales)
| Alerta | Regla | Umbral inicial |
|---|---|---|
| Mantenimiento vencido | kilometraje - km_ultimo_mantenimiento > intervalo | > 5000 km (ajustable) |
| Parámetro mecánico crítico | Temperatura del motor por encima del límite, o código de falla de severidad CRITICA | temperatura_motor_c > 110 °C, o código P0217 / P0524 |

## Justificación de umbrales
### ¿Por qué 5000 km como intervalo de mantenimiento?

Elegimos 5000 km como intervalo inicial porque es una referencia que se encuentra en planes de mantenimiento de vehículos comercializados en Colombia. Auteco TVS publica revisiones periódicas cada 5000 km para sus motocicletas, y Nissan Colombia también menciona revisiones cada 5000 km o seis meses. Esto no significa que todos los modelos deban revisarse igual, pero nos permite utilizar una base razonable para la simulación de RapidGo y ajustar el intervalo según el fabricante de cada vehículo cuando se implemente con datos reales.

Links de referencia:

https://www.auteco.com.co/blog/post/mantenimiento-por-kilometraje-que-hacer-cada-1-000-3-000-y-6-000-km
https://www.nissan.com.co/newsletter/posventa/mantenimiento-preventivo-nissan.html
### ¿Por qué 110 °C se considera temperatura crítica?

Tomamos los 110 °C como umbral inicial de alerta crítica porque una temperatura de esa magnitud puede indicar un problema importante en el sistema de refrigeración. Como referencia técnica, un taller especializado explica que muchos motores trabajan con temperaturas del refrigerante cercanas a 85–92 °C y que el sobrecalentamiento puede presentarse alrededor de 110–115 °C. Por eso, en RapidGo usamos 110 °C como límite de atención crítica, teniendo presente que el valor exacto debe ajustarse a las especificaciones de cada motor.

Links de referencia:

https://club.autodoc.es/magazin/sobrecalentamiento-del-motor-cuales-son-las-consecuencias
https://www.jeep.com/pe/blog/mundo-automotriz/temperatura-del-motor-potencia-auto.html
### ¿Por qué el riesgo empieza al 90 % del intervalo y a 100 °C?

No esperamos a que el vehículo supere los 5000 km o alcance una temperatura crítica para considerarlo en riesgo. A los 4500 km ya ha consumido el 90 % del intervalo de mantenimiento, por lo que conviene programar una revisión antes de llegar al límite. De forma similar, usamos 100 °C como advertencia temprana para detectar un posible aumento anormal de temperatura antes de alcanzar los 110 °C definidos como umbral crítico. Así podemos distinguir entre prevenir una falla y reaccionar cuando ya existe una condición crítica.

### ¿Por qué una ventana de 5 minutos?

Elegimos una ventana de cinco minutos porque la telemetría simulada genera lecturas frecuentes y necesitamos agruparlas para calcular indicadores útiles, como los kilómetros recorridos, las fallas registradas y los vehículos en riesgo. Una ventana demasiado corta podría producir resultados muy variables por cada lectura, mientras que una demasiado larga retrasaría la detección de una condición problemática. Los cinco minutos son un compromiso inicial entre estabilidad de los indicadores y rapidez de respuesta, que podremos ajustar según el comportamiento del flujo de datos.
