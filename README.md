# Factorial---Business-Logic-Test

## Tabla de contenidos

1. [Introducción](#1-introducción)
2. [Obtención del dataset](#2-obtención-del-dataset)
3. [Tratamiento del dataset](#3-tratamiento-del-dataset)
	- [3.1. Smart Blocking](#31-smart-blocking)
	- [3.2. Fuzzy Matching](#32-fuzzy-matching)
	- [3.3. Post procesado de outliers](#33-post-procesado-de-outliers)
	- [3.4. Unificación de registros](#34-unificación-de-registros)
	- [3.5. Actualización de la base de datos](#35-actualización-de-la-base-de-datos)
4. [Posibles mejoras](#4-posibles-mejoras)

## 1. Introducción

Se presenta como problema la [duplicación de registros con nombres ligeramente similares](https://factorialco.notion.site/Business-Logic-Test-2175e6e051ee80ee9049d72e84c7c3e2) pero relacionados con una empresa, donde cada registro contiene información relevante de dicha empresa (oportunidades, contactos, etc). Se pretende eliminar esa duplicidad de registros y unir la información fragmentada para disponer de únicamente un registro por empresa.

**Tamaño del dataset:** Asumimos un dataset de **3 millones** de registros.

Al tratarse de un volumen tan grande de datos, realizar una comparación "todos con todos" implica un orden de complejidad (O(n$^{2}$)) $\rightarrow$ (O(3M$^{2}$)) $\rightarrow$ (O(9B)) $\rightarrow$ 9 billones de comparaciones.

La manera mas efectiva de tratar datasets de tan grande tamaño, es seguir la filosofía "divide and conquer" para reducir el número de comparaciones.

**Procedimiento resumido:**
1. Obtención del dataset
2. Blocking para agrupar posibles registros duplicados
3. Fuzzy matching dentro de cada bloque para detectar duplicaciones y refinar el resultado del blocking
4. Fuzzy matching entre bloques para unir clústers que se relacionen con la misma empresa
5. Detección y tratamiento de outliers para relacionarlos con algún clúster
6. Unificar datos de registros dentro de un mismo clúster
7. Actualización de los datos

___
## 2. Obtención del dataset

La conexión con la base de datos para la obtención del dataset se realiza con Python, usando la librería `pandas` y `sqlalchemy`, dado que se trabaja con un dataset considerablemente grande (si se tratase de un dataset mas pequeño, se podría usar también `psycopg2`, de manera eficiente).

Para trabajar de forma liviana y óptima, se obtiene únicamente los registros con su nombre e identificador.
- `name`$\rightarrow$ Dato usado para comparar los registros entre sí
- `id`$\rightarrow$ Tratar los datos de forma única (Primary Key)

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://admin:root@factorialhost.amazonaws.com:5432/factorialdb")
df = pd.read_sql_query("SELECT id, name FROM companies WHERE name IS NOT NULL", engine)
```

Para poder visualizar mejor la transformación de los datos durante la ejecución, se toma como dataset de prueba el siguiente:

```python
companies = [
    {"id": 1, "name": "Technova Corp."},
    {"id": 2, "name": "Technova Inc"},
    {"id": 3, "name": "TechNova"},
    {"id": 4, "name": "Techn-ova Ltd"},
    {"id": 5, "name": "Technovva"},
    {"id": 6, "name": "TechNova.io"},
    {"id": 7, "name": "Tech Nova Solutions"},
    {"id": 8, "name": "Tchnva Inc"},
    {"id": 9,  "name": "GreenPower SA"},
    {"id": 10, "name": "Green-Power"},
    {"id": 11, "name": "Green Power Ltd"},
    {"id": 12, "name": "GreenPower"},
    {"id": 13, "name": "GrennPower"},
    {"id": 14, "name": "GreeenPower Group"},
    {"id": 15, "name": "Greenpowr"},
    {"id": 16, "name": "Greenpower.tech"},
    {"id": 17, "name": "GrinPower"},
    {"id": 18, "name": "Datalfex"},
    {"id": 19, "name": "Datalfex Corp."},
    {"id": 20, "name": "DataLfex"},
    {"id": 21, "name": "Dataflex Ltd"},
    {"id": 22, "name": "Datalflex"},
    {"id": 23, "name": "DatalFex.io"},
    {"id": 24, "name": "OmegaLogistics"},
    {"id": 25, "name": "Omega Logistics Inc"},
    {"id": 26, "name": "Omegalogstics"},
    {"id": 27, "name": "OmegaLogixtic"},
    {"id": 28, "name": "Omega Log."},
    {"id": 29, "name": "Omeega Logistics"},
    {"id": 30, "name": "Xenon Corp."},
    {"id": 31, "name": "Xenon Technologies"},
    {"id": 32, "name": "Zenon"},
    {"id": 33, "name": "Zenon Ltd"},
    {"id": 34, "name": "Tech Nova Solutionz"}
]

Expected_clusters = [
	"technova",
	"greenpower",
	"datalfex",
	"omegalogistics",
	"xenon"
]

```

## 3. Tratamiento del dataset

Para detectar registros duplicados, se puede realizar una comparación "todos contra todos". No obstante, dicha comparación implica una elevada complejidad, de modo que resulta más eficiente agrupar los registros en grupos limitados a un tamaño máximo, donde los registros agrupados sean potencialmente duplicaciones de un registro "maestro".

Al limitar estos bloques a un tamaño máximo de 50 registros, estamos reduciendo considerablemente la complejidad del cómputo, limitándola dentro de cada bloque a O(50$^{2}$) $\rightarrow$ 2500 comparaciones por bloque (como máximo). El orden de complejidad total se calcula como `O(n*b)`, donde `n`es el número de bloques y `b` el tamaño promedio del bloque.

**Procedimiento resumido:**
1. Obtención del dataset
2. Blocking para agrupar posibles registros duplicados
3. Fuzzy matching dentro de cada bloque para detectar duplicaciones y refinar el resultado del blocking
4. Fuzzy matching entre bloques para unir clústers que se relacionen con la misma empresa
5. Detección y tratamiento de outliers para relacionarlos con algún clúster
6. Unificar datos de registros dentro de un mismo clúster
7. Actualización de los datos


### 3.1. Smart Blocking

El primer paso para reducir la complejidad es agrupar los registros en bloques con tal de realizar comprobaciones únicamente entre registros del mismo grupo. 

Dado que un mismo dominio puede encontrarse representado de muchas maneras distintas(por ejemplo: `acme.com, acme.io, acme-industries.com`), es necesario definir unas reglas que normalicen los nombres para luego poder agrupar los registros en base al nombre normalizado.

Dichas reglas son las siguientes:
- Eliminación de caracteres especiales y acentos
- Conversión de mayúsculas a minúsculas
- Eliminación de carácteres no alfabéticos
- Eliminación de sufijos comunes en nombres de dominios ("inc", "corp", "corporation", "group", "llc", "ltd","industries", "company", "co", "sa", "sl", "plc", "io", "solutions", "solutionz", "solution", "technologies", "technology", "tech", "systems", "system", "services", "service")

El resultado será la obtención de bloques donde se agrupan potenciales registros duplicados.

``` python
import unicodedata
import re
from collections import defaultdict

COMMON_SUFFIXES = [
"inc", "corp", "corporation", "group", "llc", "ltd","industries", "company", "co", "sa", "sl", "plc", "io", "solutions", "solutionz", "solution", "technologies", "technology", "tech", "systems", "system", "services", "service"
]

def normalize_text(text):
	# Applying normalization rules
	text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
	text = text.lower()
	text = re.sub(r'[-\.]', ' ', text)
	text = re.sub(r'[^a-z\s]', '', text)
	words = text.split()
	clean_words = [words for words in words if words not in COMMON_SUFFIXES]
	
	return ' '.join(clean_words)

core_blocks = defaultdict(list) # Blocks
companies_dict = {}
core_dict = {}

for company in companies:
	company_id = company["id"]
	company_name = company["name"]
	
	normalized_name = normalize_text(company_name)
	companies_dict[company_id] = company_name
	core_dict[company_id] = normalized_name
	
	core_blocks[normalized_name].append(company_id)
```

```python
core_blocks = {
"technova": [1, 2, 3, 6],
"techn ova": [4],
"technovva": [5],
"nova": [7, 34],
"tchnva": [8],
"greenpower": [9, 12, 16],
"green power": [10, 11],
"grennpower": [13],
"greeenpower": [14],
"greenpowr": [15],
"grinpower": [17],
"datalfex": [18, 19, 20, 23],
"dataflex": [21],
"datalflex": [22],
"omegalogistics": [24],
"omega logistics": [25],
"omegalogstics": [26],
"omegalogixtic": [27],
"omega log": [28],
"omeega logistics": [29],
"xenon": [30, 31],
"zenon": [32, 33]
}

companies_dict:  {
  "1": "Technova Corp.",
  "2": "Technova Inc",
  "3": "TechNova",
  "4": "Techn-ova Ltd",
  "5": "Technovva",
  "6": "TechNova.io",
  "7": "Tech Nova Solutions",
  "8": "Tchnva Inc",
  "9": "GreenPower SA",
  "10": "Green-Power",
  "11": "Green Power Ltd",
  "12": "GreenPower",
  "13": "GrennPower",
  "14": "GreeenPower Group",
  "15": "Greenpowr",
  "16": "Greenpower.tech",
  "17": "GrinPower",
  "18": "Datalfex",
  "19": "Datalfex Corp.",
  "20": "DataLfex",
  "21": "Dataflex Ltd",
  "22": "Datalflex",
  "23": "DatalFex.io",
  "24": "OmegaLogistics",
  "25": "Omega Logistics Inc",
  "26": "Omegalogstics",
  "27": "OmegaLogixtic",
  "28": "Omega Log.",
  "29": "Omeega Logistics",
  "30": "Xenon Corp.",
  "31": "Xenon Technologies",
  "32": "Zenon",
  "33": "Zenon Ltd",
  "34": "Tech Nova Solutionz"
}

core_dict:  {
  "1": "technova",
  "2": "technova",
  "3": "technova",
  "4": "techn ova",
  "5": "technovva",
  "6": "technova",
  "7": "nova",
  "8": "tchnva",
  "9": "greenpower",
  "10": "green power",
  "11": "green power",
  "12": "greenpower",
  "13": "grennpower",
  "14": "greeenpower",
  "15": "greenpowr",
  "16": "greenpower",
  "17": "grinpower",
  "18": "datalfex",
  "19": "datalfex",
  "20": "datalfex",
  "21": "dataflex",
  "22": "datalflex",
  "23": "datalfex",
  "24": "omegalogistics",
  "25": "omega logistics",
  "26": "omegalogstics",
  "27": "omegalogixtic",
  "28": "omega log",
  "29": "omeega logistics",
  "30": "xenon",
  "31": "xenon",
  "32": "zenon",
  "33": "zenon",
  "34": "nova"
}
```

### 3.2. Fuzzy Matching

Las técnicas de Fuzzy Matching permiten detectar potenciales registros duplicados dentro de un mismo bloque. 

Hay 2 maneras de aplicar Fuzzy Matching:
1. Fuzzy Matching dentro de bloques
2. Fuzzy Matching entre bloques similares

Al tratarse de un tamaño de datos tan grande, aplicar únicamente **fuzzy solo dentro de bloques** puede llevar a que registros que han sido separados en diferentes bloques nunca sean comparados entre si y, por lo tanto, acaben siendo tratados como empresas distintas en clústers distintos.

Para solventar el problema manteniendo la eficiencia, se usa una combinación de dichas técnicas:
1. Para los bloques de más de 50 registros:
	1. Agrupar los registros en diferentes bloques basándose en la longitud del nombre
	2. Si aún quedan sub-bloques de más de 50 registros, dividirlos en bloques de máximo 50 registros
2. Fuzzy Matching dentro de bloques con threshold del 80%
3. Fuzzy Matching entre bloques (comparando núcleos) con threshold del 60%

Respecto a la similitud entre registros, cabe mencionar que puede darse el caso en el que se generen clústers que realmente pertenezcan a la misma empresa y que, por estar separados en bloques distintos, nunca lleguen a ser comparados. Por ese motivo, además de aplicar Fuzzy Matching dentro de bloques con un límite restrictivo (80%), se aplica Fuzzy Matching entre clústers de una forma mas flexiva (reduciendo el límite a 60%) para garantizar una correcta clasificación.

El resultado será la obtención de bloques más refinados con registros duplicados, además de bloques que no han podido ser agrupados y que deberán de ser tratados como outliers.


``` PYTHON
from collections import defaultdict
from rapidfuzz import fuzz

def divide_big_block(ids, companies_dict, max_size=50):
	# group by name length
	subblocks_by_length = defaultdict(list)
	
	for company_id in ids:
		name = companies_dict[company_id]
		key = len(name) // 10
		subblocks_by_length[key].append(company_id)
	
	result = []
	for subblock in subblocks_by_length.values():
		if len(subblock) > max_size:
			# if still too big, divide group by max size
			result.extend([subblock[i:i+max_size] for i in range(0, len(subblock), max_size)])
		else:
			result.append(subblock)
	return result

def group_by_similarity(company_ids, companies_dict, threshold):
	clusters = []
	processed_companies = set()
	
	for i, company_id in enumerate(company_ids):
		if company_id in processed_companies:
			continue
		new_cluster = [company_id]
		processed_companies.add(company_id)
		
		for j in range(i + 1, len(company_ids)):
			compared_company_id = company_ids[j]
			if compared_company_id in processed_companies:
				continue
		
			company_name = companies_dict[company_id]
			compared_company_name = companies_dict[compared_company_id]
			
			# fuzzy matching
			similarity = fuzz.token_sort_ratio(company_name, compared_company_name)
			
			if similarity >= threshold:
				new_cluster.append(compared_company_id)
				processed_companies.add(compared_company_id)
		clusters.append(new_cluster)
	return clusters

def cross_merge_clusters(clusters, core_dict, core_threshold=60):
    core_list = [(core_dict[c[0]], c) for c in clusters]
    used = set()
    final_clusters = []

    for i, (core1, cluster1) in enumerate(core_list):
        if i in used:
            continue

        fusion = set(cluster1)
        for j, (core2, cluster2) in enumerate(core_list):
            if i == j or j in used:
                continue

            score = fuzz.token_sort_ratio(core1, core2)
            if score >= core_threshold:
                fusion.update(cluster2)
                used.add(j)

        final_clusters.append(list(fusion))
        used.add(i)

    return final_clusters


clusters = []
for normalized_name, company_ids in core_blocks.items():
	if len(company_ids) == 1:
		# single company
		clusters.append([company_ids[0]])
		continue
		
	if len(company_ids) > 50:
		# if block is too big, divide big block
		subblocks = divide_big_block(company_ids, companies_dict, max_size=50)
		for subblock in subblocks:
			if len(subblock) > 1:
			# fuzzy matching
			clusters_subblock = group_by_similarity(subblock, companies_dict, threshold=80)
			clusters.extend(clusters_subblock)
		else:
			clusters.append(subblock)
	else:
		# fuzzy matching
		clusters_block = group_by_similarity(company_ids, companies_dict, threshold=80)
		clusters.extend(clusters_block)
		
clusters = cross_merge_clusters(clusters, core_dict, core_threshold=60)
```

```python
# Clusters after fuzzy matching
clusters = [
    [1, 2, 3, 6],
    [4],
    [5],
    [7, 34],
    [8],
    [9, 12, 16],
    [10, 11],
    [13],
    [14],
    [15],
    [17],
    [18, 19, 20, 23],
    [21],
    [22],
    [24],
    [25],
    [26],
    [27],
    [28],
    [29],
    [30, 31],
    [32, 33]
]
```

```python
# Clusters after cross merging
clusters = [
    [1, 2, 3, 34, 5, 6, 7, 8],
    [4],
    [9, 10, 11, 12, 13, 14, 15, 16, 17],
    [18, 19, 20, 21, 22, 23],
    [24, 25, 26, 27, 29],
    [28],
    [32, 33, 30, 31]
]
```

### 3.3. Post procesado de outliers

Aquellos registros que después de aplicar Fuzzy Matching no han podido ser agrupados quedan aislados en bloques y deben ser tratados como outliers.

Para tratar dichos registros, se vuelve aplicar Fuzzy Matching, comparando cada outlier con algún centroide de cada clúster generado anteriormente.

En este punto, un threshold más flexivo (60%) facilita la asignación de los outliers a algún clúster.

El resultado obtenido serán los clústers finales con los outliers recuperados, teniendo así todos los registros agrupados por empresa a la cual hacen referencia.

``` python
from rapidfuzz import fuzz

def find_outliers(clusters):
    outliers = []
    for cluster in clusters:
        if len(cluster) == 1:
            company_id = cluster[0]
            outliers.append(company_id)
    return outliers

def recover_outliers(outliers, clusters, companies_dict, recovery_threshold=60):
    new_clusters = []

    for outlier_id in outliers:
        outlier_name = companies_dict[outlier_id]
        best_similarity = 0
        best_cluster = None

        for cluster in clusters:
            if len(cluster) == 1:
                continue

            similarities = []
            for company_id in cluster:
                similarity = fuzz.token_sort_ratio(outlier_name, companies_dict[company_id])
                similarities.append(similarity)

            max_similarity_cluster = max(similarities)

            if max_similarity_cluster > best_similarity and max_similarity_cluster >= recovery_threshold:
                if len(cluster) <= 3:
                    if max_similarity_cluster >= recovery_threshold:
                        best_similarity = max_similarity_cluster
                        best_cluster = cluster
                else:
                    similar_companies = sum(1 for s in similarities if s >= recovery_threshold)
                    if similar_companies >= 2:
                        best_similarity = max_similarity_cluster
                        best_cluster = cluster

        if best_cluster:
            best_cluster.append(outlier_id)
            print(f"Recovered: ID {outlier_id} ({outlier_name}) -> Cluster with similarity {best_similarity:.1f}%")
        else:
            new_clusters.append([outlier_id])
            print(f"Kept as outlier: ID {outlier_id} ({outlier_name})")

    clusters = [c for c in clusters if len(c) > 1]
    clusters.extend(new_clusters)

    return clusters


outliers = find_outliers(clusters)

if (outliers):
	final_clusters = recover_outliers(outliers, clusters, companies, recovery_threshold=60)
```

```python
final_clusters = [
    [1, 2, 3, 34, 5, 6, 7, 8, 4],
    [9, 10, 11, 12, 13, 14, 15, 16, 17],
    [18, 19, 20, 21, 22, 23],
    [24, 25, 26, 27, 29, 28],
    [32, 33, 30, 31]
]
```

### 3.4. Unificación de registros

Para unificar los datos de los registros agrupados en clústers, es necesario definir un registro maestro, el cual almacenará los datos del resto de registros.

El criterio que se ha escogido para escoger un registro maestro es el registro con identificador más antiguo (el valor mas pequeño), ya que es altamente probable de que se trate del registro original y no sea una duplicación (creado cuando ya existía un registro para la misma empresa).

Dado que el dataset con el que se trabaja únicamente contiene nombres e identificadores, basta con construir un diccionario que siga la siguiente estructura:
- `key`$\rightarrow$ id del registro duplicado
- `value`$\rightarrow$ id del registro maestro

```python
def get_company_id_redirects(final_clusters):
    redirects = {}
    for cluster in final_clusters:
        master_id = min(cluster)
        for cid in cluster:
            if cid != master_id:
                redirects[cid] = master_id
    return redirects

redirects = get_company_id_redirects(final_clusters)
```

``` python
redirects = {
	2: 1,
	3: 1,
	34: 1,
	5: 1,
	# ...
	10: 9,
	11: 9,
	12: 9,
	# ...
}
```

### 3.5. Actualización de la base de datos

```python
import pandas as pd
from sqlalchemy import create_engine

############################################################################
# DB Connection code
engine = create_engine("postgresql://admin:root@factorialhost.amazonaws.com:5432/factorialdb")
df = pd.read_sql_query("SELECT id, name FROM companies WHERE name IS NOT NULL", engine)
############################################################################


############################################################################
# Blocking + Fuzzy Matching + Outliners Post-Processing + Data Unification
############################################################################


############################################################################
# DB Update
for duplicated_id, master_id in redirects.items():
    print(f"UPDATE contacts SET company_id = {master_id} WHERE company_id = {duplicated_id};")
    print(f"UPDATE deals SET company_id = {master_id} WHERE company_id = {duplicated_id};")
############################################################################
```

Algunas de las consultas SQL generadas:
```sql
UPDATE contacts SET company_id = 1 WHERE company_id = 2;
UPDATE deals SET company_id = 1 WHERE company_id = 2;
UPDATE contacts SET company_id = 1 WHERE company_id = 2;
UPDATE deals SET company_id = 1 WHERE company_id = 2;
UPDATE contacts SET company_id = 9 WHERE company_id = 10;
UPDATE deals SET company_id = 9 WHERE company_id = 10;
UPDATE contacts SET company_id = 9 WHERE company_id = 11;
UPDATE deals SET company_id = 9 WHERE company_id = 11;
```

___
## 4. Posibles mejoras

El uso de tecnologías como Spark podría ayudar a aumentar la velocidad de procesamiento, ya que permitiría ejecutar en paralelo los algoritmos de Fuzzy Matching.

También se podría ejecutar en paralelo parte de código usando librerías de Python como `multiprocessing`o `joblib`, aunque son más básicas.
