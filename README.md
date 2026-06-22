# Progetto Meteo ed Energia

![Architettura del progetto](docs/images/struttura.png)

Pipeline dati su Databricks per collegare condizioni meteorologiche e produzione rinnovabile in Italia.

Il progetto **Meteo ed Energia** raccoglie, trasforma e analizza dati meteorologici e dati energetici per studiare il rapporto tra meteo e produzione rinnovabile.
La pipeline integra dati provenienti da **Open-Meteo** per la parte meteorologica e dati provenienti da **ENTSO-E** per la parte energetica, usando un’architettura **Medallion** su **Databricks**, **Unity Catalog** e **Delta Lake**.

Il risultato finale è una tabella Gold unica, `gold.dati_studio`, usata per analisi e visualizzazioni su temperatura, umidità, vento, precipitazioni, produzione solare, produzione eolica e produzione idrica.

---

## Obiettivo del progetto

L’obiettivo principale è costruire una pipeline dati completa, ordinata e automatizzabile che permetta di:

* raccogliere dati meteo storici e live tramite API Open-Meteo;
* raccogliere dati energetici storici e giornalieri tramite API ENTSO-E;
* consolidare i dati in tabelle Delta organizzate per layer;
* aggregare i dati meteo ed energetici per macroarea, data e ora;
* creare una tabella finale unica per analisi e visualizzazioni;
* realizzare casi studio sul rapporto tra meteo e produzione rinnovabile.

Il progetto è pensato per mostrare un flusso end-to-end: dalla raccolta dei dati grezzi fino alla costruzione di dataset analitici e grafici esplorativi.

---

## Tecnologie utilizzate

* **Databricks**
* **Unity Catalog**
* **Delta Lake**
* **PySpark**
* **Python**
* **Pandas**
* **Plotly**
* **Open-Meteo API**
* **ENTSO-E API**
* **Databricks Jobs**
* **Databricks Secrets**

---

## Architettura generale

Il progetto usa una struttura **Medallion Architecture** composta da quattro aree principali:

| Layer      | Ruolo                                                              |
| ---------- | ------------------------------------------------------------------ |
| `metadati` | Tabelle statiche di supporto, come città monitorate e zone ENTSO-E |
| `bronze`   | Dati grezzi o quasi grezzi provenienti dalle API                   |
| `silver`   | Dati puliti, consolidati o aggregati per l’uso operativo           |
| `gold`     | Dataset finali per analisi, visualizzazioni e casi studio          |

Il catalogo principale è:

```text
progetto_meteo
```

Gli schemi usati sono:

```text
progetto_meteo.metadati
progetto_meteo.bronze
progetto_meteo.silver
progetto_meteo.gold
```

---

## Tabelle principali

| Layer    | Tabella                     | Descrizione                                           |
| -------- | --------------------------- | ----------------------------------------------------- |
| Metadati | `metadati.citta_monitorate` | Città e province operative usate per Open-Meteo       |
| Metadati | `metadati.zone_entsoe`      | Zone ENTSO-E italiane e macroarea associata           |
| Bronze   | `bronze.meteo_storico`      | Storico meteo scaricato da Open-Meteo                 |
| Bronze   | `bronze.meteo_streaming`    | Dati meteo live raccolti ogni 15 minuti               |
| Bronze   | `bronze.dati_entsoe`        | Dati energetici ENTSO-E scaricati via API             |
| Silver   | `silver.dati_aggiornati`    | Dati meteo consolidati a livello città, data e ora    |
| Silver   | `silver.energy_block`       | Produzione rinnovabile aggregata per area, data e ora |
| Gold     | `gold.dati_aggregati`       | Dati meteo aggregati per macroarea                    |
| Gold     | `gold.dati_studio`          | Dataset finale meteo + energia                        |

---

## Fonti dati

### Open-Meteo

Open-Meteo viene usato per raccogliere dati meteorologici storici e live.

Le principali variabili usate sono:

* temperatura a 2 metri dal suolo;
* umidità relativa;
* precipitazioni;
* velocità del vento;
* temperatura massima giornaliera;
* temperatura minima giornaliera.

Nel progetto i nomi finali sono in italiano:

| Campo Open-Meteo              | Campo progetto    |
| ----------------------------- | ----------------- |
| `temperature_2m` storico      | `Temp_Oraria`     |
| `temperature_2m` live/current | `Temp_Istantanea` |
| `relative_humidity_2m`        | `Umidita`         |
| `wind_speed_10m`              | `Velocita_Vento`  |
| `precipitation`               | `Precipitazioni`  |
| `temperature_2m_max`          | `Temp_Max`        |
| `temperature_2m_min`          | `Temp_Min`        |

### ENTSO-E

ENTSO-E viene usato per raccogliere dati di produzione elettrica da fonti rinnovabili.

Il progetto usa i dati **Actual Generation per Production Type**.

Tipi di produzione considerati:

| Codice | Tipo ENTSO-E                   | Aggregazione finale |
| ------ | ------------------------------ | ------------------- |
| `B16`  | Solar                          | `Solare`            |
| `B11`  | Hydro Run-of-river and pondage | `Idrico`            |
| `B12`  | Hydro Water Reservoir          | `Idrico`            |
| `B18`  | Wind Offshore                  | `Eolico`            |
| `B19`  | Wind Onshore                   | `Eolico`            |

Gli orari ENTSO-E arrivano in UTC e vengono convertiti in `Europe/Rome`, così i dati energetici restano allineati ai dati Open-Meteo.

---

## Metadati territoriali

### Città monitorate

La tabella `metadati.citta_monitorate` contiene le città e province operative usate per interrogare Open-Meteo.

Schema:

```text
citta
regione
area
latitudine
longitudine
```

Le macroaree finali del progetto sono:

* `Nord`
* `Centro`
* `Sud`
* `Isole`

Per la provincia **Barletta-Andria-Trani** viene usata una sola riga tecnica chiamata `BAT`, invece di inserire tre righe separate per Barletta, Andria e Trani.

Coordinate operative BAT:

```text
latitudine: 41.2748
longitudine: 16.3298
```

### Zone ENTSO-E

La tabella `metadati.zone_entsoe` contiene le zone energetiche italiane usate per interrogare l’API ENTSO-E.

| Zona          | Macroarea |
| ------------- | --------- |
| `IT-NORTH`    | Nord      |
| `IT-CNORTH`   | Centro    |
| `IT-CSOUTH`   | Centro    |
| `IT-SOUTH`    | Sud       |
| `IT-CALABRIA` | Sud       |
| `IT-SICILY`   | Isole     |
| `IT-SARDINIA` | Isole     |
| `IT-SACODC`   | Isole     |
| `IT-SACOAC`   | Isole     |

I codici ENTSO-E presenti nel progetto non sono credenziali: sono codici dominio necessari per interrogare l’API.

---

## Pipeline Open-Meteo

La parte meteorologica è composta da quattro notebook principali.

### `Bootstrapper.ipynb`

Scarica lo storico meteo da Open-Meteo dal `01/01/2024` fino al giorno precedente al lancio.

Inoltre recupera anche la giornata corrente fino all’ultima ora completa, così si evita un buco tra mezzanotte e l’orario di avvio del progetto.

Scrive nella tabella:

```text
progetto_meteo.bronze.meteo_storico
```

### `Clonatore.ipynb`

Inizializza la Silver meteo copiando i dati da:

```text
bronze.meteo_storico
```

a:

```text
silver.dati_aggiornati
```

Questo notebook serve nella fase iniziale, subito dopo il bootstrap meteo.

### `LiveForecast.ipynb`

Raccoglie dati meteo live tramite Forecast API di Open-Meteo.

Il notebook viene eseguito periodicamente ogni 15 minuti e salva ogni acquisizione in:

```text
bronze.meteo_streaming
```

In questa tabella la temperatura corrente viene salvata come:

```text
Temp_Istantanea
```

perché rappresenta il valore del momento e non ancora una media oraria.

### `Patcher.ipynb`

Consolida ogni giorno i dati live del giorno appena concluso.

Legge:

```text
bronze.meteo_streaming
```

raggruppa per città, data e ora, e scrive il risultato in:

```text
silver.dati_aggiornati
```

Durante questa fase:

```text
Temp_Istantanea -> Temp_Oraria
```

Il notebook evita duplicazioni controllando se il giorno è già presente in Silver.

---

## Pipeline ENTSO-E

La parte energetica è composta da quattro notebook principali.

### `Bootstrap_entsoe_API.ipynb`

Scarica lo storico ENTSO-E via API dal `01/01/2024` fino alla mezzanotte del giorno di lancio, con fine esclusiva.

Scrive nella tabella:

```text
progetto_meteo.bronze.dati_entsoe
```

Il token ENTSO-E viene letto tramite Databricks Secrets e non è scritto nel codice.

### `Clonatore_Energy_Block.ipynb`

Inizializza la Silver energia leggendo tutta la Bronze ENTSO-E e aggregando i dati per:

```text
Area
Data
Ora
```

Il risultato viene salvato in:

```text
silver.energy_block
```

Questa tabella contiene:

```text
Solare
Idrico
Eolico
```

### `Streaming_entsoe_giornaliero.ipynb`

Scarica ogni giorno solo il giorno appena concluso da ENTSO-E.

Aggiorna `bronze.dati_entsoe` con overwrite mirato tramite `replaceWhere`.

La logica è:

* non ricostruire tutta la Bronze;
* non fare delete manuale;
* aggiornare solo il giorno interessato.

### `Patcher_Energy_Block.ipynb`

Aggrega il giorno appena scaricato da ENTSO-E e aggiorna solo quel giorno in:

```text
silver.energy_block
```

Anche qui viene usato `replaceWhere`, così la Silver energia non viene ricostruita interamente ogni giorno.

---

## Pipeline Gold

La parte Gold costruisce i dataset finali usati per analisi e visualizzazioni.

### `Gold_Dati_Aggregati.ipynb`

Legge la Silver meteo:

```text
silver.dati_aggiornati
```

e aggrega i dati da livello città a livello macroarea.

La tabella finale è:

```text
gold.dati_aggregati
```

Contiene:

```text
Area
Data
Ora
Temperatura
Umidita
Velocita_Vento
Precipitazioni
```

### `Gold_Dati_Studio.ipynb`

Unisce:

```text
gold.dati_aggregati
silver.energy_block
```

tramite join su:

```text
Area
Data
Ora
```

Il risultato finale è:

```text
gold.dati_studio
```

Questa è la tabella principale usata nei casi studio.

Colonne finali:

```text
Area
Data
Ora
Temperatura
Umidita
Velocita_Vento
Precipitazioni
Solare
Idrico
Eolico
```

Il join segue la copertura meteo. Se una riga meteo non trova energia corrispondente, i valori energetici vengono riempiti a `0.0`.

---

## Jobs Databricks

Il progetto è pensato per essere eseguito tramite Databricks Jobs.

### `JOB_BOOTSTRAP_INIZIALE`

Job manuale, da eseguire una sola volta.

Ramo meteo:

```text
Bootstrapper -> Clonatore
```

Ramo energia:

```text
Bootstrap_entsoe_API -> Clonatore_Energy_Block
```

Serve a inizializzare Bronze e Silver di base.

### `JOB_LIVE_OPENMETEO`

Job schedulato ogni 15 minuti.

Contiene:

```text
LiveForecast
```

Cron:

```text
0 0/15 * * * ?
```

Timezone:

```text
Europe/Rome
```

### `JOB_AGGIORNAMENTO_GIORNALIERO`

Job schedulato ogni giorno alle 00:10.

Ramo meteo:

```text
Patcher -> Gold_Dati_Aggregati
```

Ramo energia:

```text
Streaming_entsoe_giornaliero -> Patcher_Energy_Block
```

Step finale:

```text
Gold_Dati_Studio
```

Cron:

```text
0 10 0 * * ?
```

Timezone:

```text
Europe/Rome
```

Il notebook `tester.ipynb` non è inserito nei Jobs automatici. Serve solo come controllo manuale.

---

## Casi studio e visualizzazioni

I casi studio leggono la tabella finale:

```text
progetto_meteo.gold.dati_studio
```

Le visualizzazioni sono realizzate direttamente in Databricks usando:

```python
spark.sql(query).toPandas()
displayHTML(fig.to_html(...))
```

Non servono:

```text
.env
hostname
http_path
token
databricks-sql-connector
```

### Studio 1 - Mix rinnovabile mensile

Analizza la produzione mensile rinnovabile in Italia.

Include:

* produzione mensile Solare + Eolico;
* produzione mensile Solare + Eolico + Idrico.

Serve a leggere la stagionalità della produzione rinnovabile e a osservare come cambia il contributo delle fonti nel corso dell’anno.

### Studio 2 - Confronto tra macroaree

Confronta Nord, Centro, Sud e Isole.

Include:

* produzione rinnovabile annua per macroarea;
* composizione percentuale del mix rinnovabile per macroarea.

Serve a evidenziare differenze territoriali e modelli diversi di produzione.

### Studio 3 - Rischio mensile di bassa produzione rinnovabile

Calcola la percentuale di giorni in cui la produzione rinnovabile giornaliera è sotto il primo quartile della macroarea.

Il risultato è una heatmap per area e mese.

Serve a individuare periodi e territori in cui la produzione rinnovabile è più spesso debole rispetto al comportamento tipico dell’area.

### Studio 4 - Relazione tra vento medio e produzione eolica

Confronta vento medio mensile e produzione eolica mensile per macroarea.

Calcola anche una correlazione tra le due grandezze.

Serve a leggere quanto la produzione eolica segue l’andamento del vento nelle diverse macroaree.

### Studio 5 - Profilo orario della produzione solare

Analizza la produzione solare media per macroarea, mese e ora del giorno.

Mostra una heatmap per ogni macroarea.

Mantiene una soglia grafica a `300 MW`: sotto questa soglia il colore resta blu scuro, sopra passa alla scala giallo/arancio/rosso.

Serve a osservare le fasce orarie e stagionali in cui il solare diventa più rilevante.

---

## Colori standard dei grafici

| Fonte  | Colore    |
| ------ | --------- |
| Solare | `#FFC000` |
| Eolico | `#40DD3B` |
| Idrico | `#00B0F0` |
| Media  | `#FF0000` |

---

## Sicurezza e credenziali

Il progetto è pensato per essere GitHub-ready.

Le credenziali non devono essere salvate nei notebook o nei file versionati.

Il token ENTSO-E viene letto tramite Databricks Secrets:

```python
token_entsoe = dbutils.secrets.get(
    scope="progetto-meteo",
    key="entsoe-token"
)
```

Regole principali:

* non committare token;
* non committare file `.env` reali;
* non inserire credenziali nei notebook;
* usare Databricks Secrets per valori sensibili;
* usare eventualmente `.env.example` solo come template senza valori reali.

---

## Struttura consigliata del repository

Esempio di struttura GitHub:

```text
Meteo_Project/
│
├── README.md
├── .gitignore
│
├── notebooks/
│   ├── setup/
│   │   ├── Setup_Struttura.ipynb
│   │   ├── Setup_Città.ipynb
│   │   └── Setup_Zone_entsoe.ipynb
│   │
│   ├── open_meteo/
│   │   ├── Bootstrapper.ipynb
│   │   ├── Clonatore.ipynb
│   │   ├── LiveForecast.ipynb
│   │   └── Patcher.ipynb
│   │
│   ├── entsoe/
│   │   ├── Bootstrap_entsoe_API.ipynb
│   │   ├── Clonatore_Energy_Block.ipynb
│   │   ├── Streaming_entsoe_giornaliero.ipynb
│   │   └── Patcher_Energy_Block.ipynb
│   │
│   ├── gold/
│   │   ├── Gold_Dati_Aggregati.ipynb
│   │   └── Gold_Dati_Studio.ipynb
│   │
│   └── studi/
│       ├── Studio_1_Mix_Rinnovabile_Mensile.ipynb
│       ├── Studio_2_Confronto_Macroaree.ipynb
│       ├── Studio_3_Rischio_Bassa_Produzione.ipynb
│       ├── Studio_4_Vento_Eolico.ipynb
│       └── Studio_5_Profilo_Solare_Orario.ipynb
│
└── docs/
    └── eventuali_immagini_o_note.md
```

---

## Ordine di esecuzione

### Prima esecuzione

1. `Setup_Struttura.ipynb`
2. `Setup_Città.ipynb`
3. `Setup_Zone_entsoe.ipynb`
4. `Bootstrapper.ipynb`
5. `Clonatore.ipynb`
6. `Bootstrap_entsoe_API.ipynb`
7. `Clonatore_Energy_Block.ipynb`
8. `Gold_Dati_Aggregati.ipynb`
9. `Gold_Dati_Studio.ipynb`

### Esecuzione live Open-Meteo

```text
LiveForecast.ipynb
```

Ogni 15 minuti.

### Esecuzione giornaliera

1. `Patcher.ipynb`
2. `Streaming_entsoe_giornaliero.ipynb`
3. `Patcher_Energy_Block.ipynb`
4. `Gold_Dati_Aggregati.ipynb`
5. `Gold_Dati_Studio.ipynb`

---

## Output finale

La tabella finale del progetto è:

```text
progetto_meteo.gold.dati_studio
```

Questa tabella unisce meteo ed energia e può essere usata per:

* analisi esplorative;
* visualizzazioni;
* confronto tra macroaree;
* studio della stagionalità;
* analisi del rapporto tra meteo e rinnovabili;
* possibili sviluppi futuri in ambito machine learning.

---

## Controlli manuali

Il progetto può includere un notebook `tester.ipynb` per controlli manuali.

Esempi di controlli utili:

* conteggio righe per tabella;
* conteggio città monitorate;
* conteggio zone ENTSO-E;
* intervallo temporale coperto;
* righe per anno;
* valori nulli nella Gold finale;
* duplicati su `Area`, `Data`, `Ora`;
* prime e ultime righe disponibili.

Il tester non fa parte dei Jobs automatici.

---

## Limitazioni note

Alcuni disallineamenti possono comparire in casi specifici, ad esempio nei giorni di cambio ora legale o in presenza di dati mancanti dalle API.

Gli zeri energetici nella tabella finale possono derivare da vera produzione nulla oppure da assenza di corrispondenza nel join tra meteo ed energia.

Il progetto lavora a livello di macroarea nella parte finale, quindi non misura il dettaglio energetico di singoli impianti o singole città.

---

## Possibili sviluppi futuri

Possibili evoluzioni del progetto:

* aggiunta di dashboard interattive;
* aggiunta di controlli qualità automatici;
* creazione di viste SQL dedicate;
* esportazione controllata dei risultati;
* analisi comparative tra anni diversi;
* modelli predittivi su produzione rinnovabile;
* studio di anomalie meteo-energia;
* miglioramento della documentazione tecnica dei Jobs;
* integrazione con workflow CI/CD per repository GitHub.

---

## Stato del progetto

Il progetto è funzionante nella sua architettura principale.

La fase attuale riguarda:

* pulizia dei notebook;
* miglioramento delle descrizioni;
* organizzazione del repository;
* preparazione GitHub-ready;
* documentazione tramite README.

---

## License

This project is released under the MIT License.  
See the [LICENSE](LICENSE) file for details.
