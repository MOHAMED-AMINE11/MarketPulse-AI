1\. Architecture haute-niveau

1.1 Schéma logique

┌─────────────────────────────────────────────────────────────────────────────┐

│                           SOURCES EXTERNES                                   │

├─────────────────────────────────────────────────────────────────────────────┤

│  • Yahoo Finance API (Prix OHLC, Volume)                                    │

│  • New York Times API (Articles financiers)                                 │

└────────────────┬────────────────────────────────────────────────────────────┘

&nbsp;                │

&nbsp;                ▼

┌─────────────────────────────────────────────────────────────────────────────┐

│                      COUCHE INGESTION (Producers)                            │

├─────────────────────────────────────────────────────────────────────────────┤

│  • Python Producers (Schedulés via Cron/Airflow)                            │

│  • Polling périodique des APIs                                              │

│  • Sérialisation JSON                                                       │

└────────────────┬────────────────────────────────────────────────────────────┘

&nbsp;                │

&nbsp;                ▼

┌─────────────────────────────────────────────────────────────────────────────┐

│                        APACHE KAFKA (Message Broker)                         │

├─────────────────────────────────────────────────────────────────────────────┤

│  Topic: prices\_raw      │  Topic: news\_raw                                  │

│  • Partitions: 3        │  • Partitions: 3                                  │

│  • Replication: 1       │  • Replication: 1                                 │

│  • Retention: 7 jours   │  • Retention: 7 jours                             │

└────────────────┬────────────────────────────────────────────────────────────┘

&nbsp;                │

&nbsp;                ▼

┌─────────────────────────────────────────────────────────────────────────────┐

│                    APACHE SPARK (Processing Layer)                           │

├─────────────────────────────────────────────────────────────────────────────┤

│  • Spark Structured Streaming (Micro-batch 30s)                             │

│  • Spark Batch Jobs (Nightly aggregations)                                  │

│  • PySpark + Spark SQL + Spark MLlib                                        │

└────────────────┬────────────────────────────────────────────────────────────┘

&nbsp;                │

&nbsp;                ▼

┌─────────────────────────────────────────────────────────────────────────────┐

│                    DATA LAKE (HDFS-like / MinIO / Local)                     │

├─────────────────────────────────────────────────────────────────────────────┤

│  🥉 BRONZE (Raw)          │  🥈 SILVER (Cleaned)    │  🥇 GOLD (Enriched)   │

│  • Format: Parquet        │  • Format: Parquet      │  • Format: Parquet    │

│  • Partitioning: date     │  • Partitioning: date   │  • Partitioning: date │

│  • Schema: original       │  • Schema: normalized   │  • Schema: aggregated │

│  • Deduplication: non     │  • Deduplication: oui   │  • ML enrichment      │

└────────────────┬────────────────────────────────────────────────────────────┘

&nbsp;                │

&nbsp;                ▼

┌─────────────────────────────────────────────────────────────────────────────┐

│                      SPARK ML (Sentiment Analysis)                           │

├─────────────────────────────────────────────────────────────────────────────┤

│  • Entraînement: Modèle de classification de sentiment                      │

│  • Features: TF-IDF, N-grams                                                │

│  • Output: Score sentiment \[-1, 1]                                          │

│  • Sauvegarde: MLflow / Parquet Model                                       │

└────────────────┬────────────────────────────────────────────────────────────┘

&nbsp;                │

&nbsp;                ▼

┌─────────────────────────────────────────────────────────────────────────────┐

│                         ELK STACK (Analytics \& Viz)                          │

├─────────────────────────────────────────────────────────────────────────────┤

│  Logstash           →    Elasticsearch        →    Kibana                   │

│  • Input: Parquet   │    • Index: financial   │    • Dashboards             │

│  • Filter: JSON     │    • Mapping dynamique  │    • Time series            │

│  • Output: ES       │    • Aggregations       │    • Sentiment trends       │

└─────────────────────────────────────────────────────────────────────────────┘
1.2 Flux de données (vue d'ensemble)

Phase 1 - Ingestion

Les producers Python interrogent les APIs externes toutes les 5-15 minutes et publient les données brutes dans Kafka.

Phase 2 - Streaming (Near Real-Time)

Spark Structured Streaming consomme les topics Kafka et écrit en continu dans la zone Bronze du Data Lake.

Phase 3 - Transformation (Batch)

Des jobs Spark Batch nettoyent, dédupliquent et normalisent les données Bronze → Silver (1x par heure).

Phase 4 - Enrichissement ML

Le modèle de sentiment analyse les articles news et enrichit les données Silver → Gold (1x par jour).

Phase 5 - Indexation \& Visualisation

Logstash lit les fichiers Parquet Gold, les transforme en JSON et les indexe dans Elasticsearch pour visualisation Kibana.

2. Description détaillée des composants

2.1 Apache Kafka

Rôle

Kafka agit comme un bus de messagerie distribué, garantissant la durabilité, l'ordre et la résilience des flux de données financières.

Topics

TopicDescriptionPartitionsReplicationRetentionprices\_rawPrix OHLC, volumes317 joursnews\_rawArticles financiers317 jours

Partitionnement



Clé de partition : symbol (ticker financier, ex: AAPL, TSLA)

Objectif : Garantir que toutes les données d'un même symbole arrivent dans la même partition (ordre préservé)



Configuration recommandée



log.retention.hours=168 (7 jours)

compression.type=gzip

min.insync.replicas=1 (dev), 2 (prod)

2.2 Apache Spark

Rôle

Moteur de traitement distribué pour le streaming, le batch et le machine learning.

Modes d'exécution

A. Spark Structured Streaming (Near Real-Time)



Consommation Kafka en micro-batches (30 secondes)

Écriture continue dans Bronze

Checkpoint pour la résilience



B. Spark Batch (Scheduled)



Jobs PySpark déclenchés par cron/Airflow

Transformations SQL complexes

Agrégations temporelles (OHLC resampling)



Configuration cluster



1 Master + 2 Workers (minimum)

Memory par worker : 4 GB

Cores par worker : 2

Driver memory : 2 GB





2.3 Data Lake (Architecture Médaillons)

Philosophie

Architecture en trois couches inspirée de Databricks Lakehouse : Bronze → Silver → Gold.

🥉 BRONZE (Raw Zone)



Contenu : Données brutes telles que reçues de Kafka

Format : Parquet (compression Snappy)

Schema : Dynamique, minimal cleaning

Partitionnement : ingestion\_date=YYYY-MM-DD

Exemple : /data\_lake/bronze/prices/ingestion\_date=2025-12-02/part-00000.parquet



🥈 SILVER (Cleaned Zone)



Contenu : Données nettoyées, dédupliquées, typées

Format : Parquet (compression Snappy)

Schema : Normalisé avec contraintes

Partitionnement : ingestion\_date=YYYY-MM-DD

Transformations :



Suppression des doublons

Validation des types (float, timestamp)

Gestion des valeurs nulles

Normalisation des symboles (uppercase)







🥇 GOLD (Business Zone)



Contenu : Données enrichies, agrégées, prêtes pour l'analytics

Format : Parquet (compression Snappy)

Schema : Optimisé pour les requêtes

Partitionnement : ingestion\_date=YYYY-MM-DD

Enrichissements :



Métriques calculées (RSI, MACD, moving averages)

Scores de sentiment (ML)

Agrégations temporelles (daily, weekly, monthly)









2.4 Spark ML (Sentiment Analysis)

Objectif

Analyser le sentiment des articles financiers pour détecter des signaux bullish/bearish.

Pipeline ML



Feature Engineering



Tokenization (mots)

StopWords removal (en, fr)

TF-IDF vectorization

N-grams (bigrams, trigrams)





Modèle



Algorithme : Logistic Regression / Random Forest

Labels : positive, neutral, negative

Output : Score numérique \[-1, 1]





Entraînement



Dataset : Articles labellisés manuellement ou via API

Cross-validation : 80/20

Métriques : Accuracy, F1-score, Confusion matrix





Inférence



Batch job quotidien sur Silver

Enrichissement des news avec sentiment\_score

Écriture dans Gold







Sauvegarde du modèle



Format : MLflow / Parquet Model

Path : /ml/models/sentiment\_v1/





2.5 ELK Stack

Logstash

Rôle : ETL entre Data Lake et Elasticsearch

Input



Plugin : file ou jdbc (Spark Thrift)

Format : Parquet → JSON intermédiaire



Filter



JSON parsing

Date parsing (@timestamp)

Field renaming



Output



Destination : Elasticsearch

Index pattern : financial-prices-%{+YYYY.MM.dd}, financial-news-%{+YYYY.MM.dd}



Elasticsearch

Rôle : Base de données NoSQL optimisée pour la recherche et l'agrégation

Configuration



Index templates avec mappings

Time-based indices (rotation quotidienne)

Retention policy : 90 jours



Mappings clés

{

&nbsp; "symbol": { "type": "keyword" },

&nbsp; "price": { "type": "float" },

&nbsp; "volume": { "type": "long" },

&nbsp; "timestamp": { "type": "date" },

&nbsp; "sentiment\_score": { "type": "float" }

}

Kibana

Rôle : Interface de visualisation interactive

Dashboards



Prix \& Volume : Line charts, candlesticks

Sentiment Trends : Heatmaps par symbole

News Explorer : Full-text search

Alertes : Watcher (anomalies de prix, sentiment extrême)





3\. Conventions techniques

3.1 Naming conventions

Topics Kafka



Pattern : {domain}\_{layer}

Exemples : prices\_raw, news\_raw, alerts\_enriched



Data Lake directories



Pattern : /data\_lake/{layer}/{domain}/ingestion\_date={YYYY-MM-DD}/

Exemples :



/data\_lake/bronze/prices/ingestion\_date=2025-12-02/

/data\_lake/silver/news/ingestion\_date=2025-12-02/

/data\_lake/gold/aggregated/ingestion\_date=2025-12-02/







Elasticsearch indices



Pattern : financial-{domain}-{YYYY.MM.dd}

Exemples : financial-prices-2025.12.02, financial-news-2025.12.02



3.2 Formats \& Sérialisation

Étape	    	    FormatInput            FormatOutput		     Compression

API → Kafka		JSON			JSON			None

Kafka → Bronze		JSON			Parquet			Snappy

Bronze → Silver		Parquet			Parquet			Snappy

Silver → Gold		Parquet			Parquet			Snappy

Gold → Elasticsearch	Parquet			JSON			None



3.3 Partitionnement Parquet

Stratégie de partitionnement



Colonne : ingestion\_date (DATE)

Format : YYYY-MM-DD

Granularité : Quotidienne



Avantages



Partition pruning pour les requêtes temporelles

Facilite la suppression des anciennes données (GDPR, retention)

Optimise les scans pour les dashboards Kibana



Exemple de requête Spark SQL

SELECT symbol, AVG(close) as avg\_price

FROM bronze.prices

WHERE ingestion\_date BETWEEN '2025-11-01' AND '2025-11-30'

GROUP BY symbol



3.4 Schémas de données

Schema Bronze - Prices

{

&nbsp; "symbol": "string",

&nbsp; "timestamp": "timestamp",

&nbsp; "open": "double",

&nbsp; "high": "double",

&nbsp; "low": "double",

&nbsp; "close": "double",

&nbsp; "volume": "long",

&nbsp; "ingestion\_date": "date",

&nbsp; "ingestion\_timestamp": "timestamp"

}



Schema Silver - Prices

{

&nbsp; "symbol": "string",

&nbsp; "timestamp": "timestamp",

&nbsp; "open": "double",

&nbsp; "high": "double",

&nbsp; "low": "double",

&nbsp; "close": "double",

&nbsp; "volume": "long",

&nbsp; "is\_valid": "boolean",

&nbsp; "ingestion\_date": "date"

}

Schema Gold - Enriched Prices

{

&nbsp; "symbol": "string",

&nbsp; "timestamp": "timestamp",

&nbsp; "close": "double",

&nbsp; "volume": "long",

&nbsp; "ma\_7": "double",

&nbsp; "ma\_30": "double",

&nbsp; "rsi\_14": "double",

&nbsp; "sentiment\_score": "double",

&nbsp; "ingestion\_date": "date"

}

```



---



\## 4. Workflow global



\### 4.1 Pipeline détaillé

```

┌──────────────────────────────────────────────────────────────┐

│ PHASE 1 : INGESTION (Temps réel)                             │

├──────────────────────────────────────────────────────────────┤

│ 1. Python Producer démarre (cron : \*/5 \* \* \* \*)             │

│ 2. Appel Yahoo Finance API / NYT API                         │

│ 3. Sérialisation JSON                                        │

│ 4. Publish to Kafka (topics: prices\_raw, news\_raw)          │

│ 5. Kafka commit offset                                       │

└──────────────────────────────────────────────────────────────┘

&nbsp;                            ▼

┌──────────────────────────────────────────────────────────────┐

│ PHASE 2 : STREAMING → BRONZE (Near Real-Time)                │

├──────────────────────────────────────────────────────────────┤

│ 1. Spark Structured Streaming subscribe to Kafka            │

│ 2. Micro-batch every 30 seconds                              │

│ 3. Basic schema inference                                    │

│ 4. Write to Bronze as Parquet (append mode)                 │

│ 5. Checkpoint saved to /checkpoints/bronze/                 │

└──────────────────────────────────────────────────────────────┘

&nbsp;                            ▼

┌──────────────────────────────────────────────────────────────┐

│ PHASE 3 : BATCH → SILVER (Hourly)                            │

├──────────────────────────────────────────────────────────────┤

│ 1. Spark Batch job triggered (cron : 0 \* \* \* \*)             │

│ 2. Read Bronze Parquet (last hour)                          │

│ 3. Data quality checks (nulls, types, ranges)               │

│ 4. Deduplication (dropDuplicates on \[symbol, timestamp])    │

│ 5. Schema normalization                                      │

│ 6. Write to Silver as Parquet (overwrite mode)              │

└──────────────────────────────────────────────────────────────┘

&nbsp;                            ▼

┌──────────────────────────────────────────────────────────────┐

│ PHASE 4 : ML ENRICHMENT → GOLD (Daily)                       │

├──────────────────────────────────────────────────────────────┤

│ 1. Spark ML job triggered (cron : 0 2 \* \* \*)                │

│ 2. Read Silver news (last 24h)                              │

│ 3. Load trained sentiment model                             │

│ 4. Apply model.transform() → sentiment\_score                │

│ 5. Join with Silver prices on symbol                        │

│ 6. Calculate technical indicators (MA, RSI)                 │

│ 7. Write to Gold as Parquet (overwrite mode)                │

└──────────────────────────────────────────────────────────────┘

&nbsp;                            ▼

┌──────────────────────────────────────────────────────────────┐

│ PHASE 5 : INDEXATION → ELASTICSEARCH (Daily)                 │

├──────────────────────────────────────────────────────────────┤

│ 1. Logstash pipeline starts (cron : 0 3 \* \* \*)              │

│ 2. Read Gold Parquet (last 24h)                             │

│ 3. Convert Parquet → JSON                                   │

│ 4. Apply filters (date parsing, field mapping)              │

│ 5. Bulk insert to Elasticsearch                             │

│ 6. Refresh index                                             │

└──────────────────────────────────────────────────────────────┘

&nbsp;                            ▼

┌──────────────────────────────────────────────────────────────┐

│ PHASE 6 : VISUALISATION (Interactive)                        │

├──────────────────────────────────────────────────────────────┤

│ 1. Kibana dashboards refresh (auto)                         │

│ 2. Users query via Discover / Visualize / Dashboard         │

│ 3. Alertes Watcher (anomalies, sentiment spikes)            │

└──────────────────────────────────────────────────────────────┘

```



\### 4.2 Scheduling



| Job | Fréquence | Outil | Trigger |

|-----|-----------|-------|---------|

| API Ingestion | Toutes les 5 min | Cron / Airflow | `\*/5 \* \* \* \*` |

| Streaming Bronze | Continu | Spark Streaming | - |

| Batch Silver | Toutes les heures | Airflow | `0 \* \* \* \*` |

| ML Gold | Quotidien (2h du matin) | Airflow | `0 2 \* \* \*` |

| Logstash Export | Quotidien (3h du matin) | Cron | `0 3 \* \* \*` |



---



\## 5. Structure du repository Git

```

finance-data-lake/

│

├── docs/                              # Documentation technique

│   ├── architecture.md                # Ce document

│   ├── api\_specs.md                   # Specs des APIs (Yahoo, NYT)

│   ├── data\_dictionary.md             # Description des colonnes

│   └── runbooks/                      # Procédures opérationnelles

│       ├── deployment.md

│       ├── monitoring.md

│       └── troubleshooting.md

│

├── infra/                             # Infrastructure as Code

│   ├── docker/

│   │   ├── docker-compose.yml         # Orchestration des services

│   │   └── .env.example               # Variables d'environnement

│   ├── kafka/

│   │   ├── create\_topics.sh           # Script de création des topics

│   │   └── kafka.properties           # Configuration Kafka

│   └── spark/

│       ├── spark-defaults.conf        # Config Spark

│       └── log4j.properties           # Logging

│

├── ingestion/                         # Producers Kafka

│   ├── requirements.txt               # Dépendances Python

│   ├── config/

│   │   └── apis\_config.yaml           # Credentials APIs

│   ├── producers/

│   │   ├── yahoo\_finance\_producer.py

│   │   └── nyt\_news\_producer.py

│   └── tests/

│       └── test\_producers.py

│

├── spark-jobs/                        # Jobs PySpark

│   ├── requirements.txt

│   ├── streaming/

│   │   └── bronze\_streaming.py        # Kafka → Bronze

│   ├── batch/

│   │   ├── silver\_cleaning.py         # Bronze → Silver

│   │   └── gold\_enrichment.py         # Silver → Gold

│   ├── utils/

│   │   ├── spark\_session.py           # Factory Spark

│   │   └── data\_quality.py            # Fonctions de validation

│   └── tests/

│       └── test\_transformations.py

│

├── ml/                                # Machine Learning

│   ├── requirements.txt

│   ├── notebooks/

│   │   └── sentiment\_exploration.ipynb

│   ├── training/

│   │   ├── train\_sentiment\_model.py

│   │   └── evaluate\_model.py

│   ├── inference/

│   │   └── apply\_sentiment.py

│   ├── models/                        # Modèles sauvegardés

│   └── data/

│       └── labeled\_news.csv           # Dataset d'entraînement

│

├── elk/                               # ELK Stack configuration

│   ├── logstash/

│   │   ├── pipelines/

│   │   │   ├── prices.conf

│   │   │   └── news.conf

│   │   └── templates/

│   ├── elasticsearch/

│   │   ├── index\_templates/

│   │   │   ├── prices\_template.json

│   │   │   └── news\_template.json

│   │   └── ilm\_policies/              # Index Lifecycle Management

│   └── kibana/

│       ├── dashboards/

│       │   └── financial\_overview.ndjson

│       └── visualizations/

│

├── data\_lake/                         # Stockage local (dev)

│   ├── bronze/

│   │   ├── prices/

│   │   └── news/

│   ├── silver/

│   │   ├── prices/

│   │   └── news/

│   └── gold/

│       └── enriched/

│

├── monitoring/                        # Observabilité

│   ├── prometheus.yml

│   ├── grafana/

│   │   └── dashboards/

│   └── alerts/

│

├── scripts/                           # Scripts utilitaires

│   ├── setup.sh                       # Setup initial

│   ├── start\_all.sh                   # Démarrage complet

│   └── cleanup.sh                     # Nettoyage

│

├── tests/                             # Tests d'intégration

│   ├── integration/

│   └── e2e/

│

├── .gitignore

├── .env.example

├── LICENSE

└── README.md                          # Documentation principale



Rôle de chaque dossier

Dossier					Responsabilité

docs					/Documentation d'architecture, spécifications techniques, runbooks

infra					/Infrastructure Docker, configs Kafka/Spark, scripts IaC

ingestion				/Producers Python pour Yahoo Finance \& NYT API → Kafka

spark-jobs				/Jobs PySpark (streaming + batch) pour Bronze/Silver/Gold

ml					/Pipeline ML (training, inference, notebooks, modèles)

elk					/Configurations Logstash, mappings Elasticsearch, dashboards Kibana

data\_lake				/Stockage Parquet local (dev) - médaillons Bronze/Silver/Gold

monitoring				/Prometheus, Grafana, alertes (observabilité)

scripts					/Scripts bash pour setup, démarrage, cleanup

tests					/Tests unitaires, intégration, end-to-end



