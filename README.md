\# 📊 MarketPulse-AI




\## 🎯 Objectif



Plateforme Big Data pour l'ingestion, le traitement et l'analyse de données financières en temps réel, combinant streaming, machine learning et visualisation interactive.



---



\## 🏗️ Architecture



\*\*Sources\*\* → \*\*Kafka\*\* → \*\*Spark\*\* → \*\*Data Lake (Bronze/Silver/Gold)\*\* → \*\*Spark ML\*\* → \*\*ELK Stack\*\*



\### Flux de données



1\. \*\*Ingestion\*\* : APIs Yahoo Finance \& New York Times → Kafka topics

2\. \*\*Streaming\*\* : Spark Structured Streaming → Bronze (Parquet)

3\. \*\*Batch\*\* : Spark jobs → Silver (cleaning) → Gold (enrichment + ML)

4\. \*\*Indexation\*\* : Logstash → Elasticsearch

5\. \*\*Visualisation\*\* : Kibana dashboards interactifs



---



\## 🛠️ Stack technique



| Composant | Technologie | Version |

|-----------|-------------|---------|

| \*\*Message Broker\*\* | Apache Kafka | 3.5 |

| \*\*Processing\*\* | Apache Spark | 3.5 |

| \*\*Storage\*\* | Parquet (Data Lake) | - |

| \*\*Machine Learning\*\* | Spark MLlib | 3.5 |

| \*\*Search \& Analytics\*\* | Elasticsearch | 8.11 |

| \*\*ETL\*\* | Logstash | 8.11 |

| \*\*Visualization\*\* | Kibana | 8.11 |

| \*\*Notebooks\*\* | Apache Zeppelin | 0.11 |

| \*\*Orchestration\*\* | Docker Compose | 2.20+ |



---



\## 🚀 Quick Start



\### Prérequis



\- Docker ≥ 24.0 + Docker Compose v2

\- 16 GB RAM minimum

\- 50 GB disque disponible

\- Ports 2181, 9092, 8080-8083, 9200, 5601 libres



\### Démarrage



```bash

\# 1. Cloner le repository

git clone https://github.com/your-org/finance-data-lake.git

cd finance-data-lake



\# 2. Configurer les variables d'environnement

cp .env.example .env

\# Éditer .env avec vos clés API



\# 3. Démarrer l'infrastructure

docker-compose up -d



\# 4. Vérifier le statut

docker-compose ps



\# 5. Créer les topics Kafka

./scripts/setup.sh

```



\### Accès aux interfaces



| Service | URL | Description |

|---------|-----|-------------|

| \*\*Spark Master\*\* | http://localhost:8080 | Cluster Spark |

| \*\*Kafka\*\* | localhost:9092 | Broker Kafka |

| \*\*Zeppelin\*\* | http://localhost:8083 | Notebooks |

| \*\*Kibana\*\* | http://localhost:5601 | Dashboards |

| \*\*Elasticsearch\*\* | http://localhost:9200 | API ES |



---



\## 📂 Structure du projet



```

finance-data-lake/

├── docs/              # Documentation

├── infra/             # Docker + configs

├── ingestion/         # Producers Kafka

├── spark-jobs/        # Jobs PySpark

├── ml/                # Machine Learning

├── elk/               # ELK Stack configs

├── data\_lake/         # Stockage Parquet

└── scripts/           # Utilitaires

```



Voir \[docs/architecture.md](docs/architecture.md) pour les détails.



---



\## 📊 Data Lake (Médaillons)



\- \*\*🥉 Bronze\*\* : Données brutes de Kafka

\- \*\*🥈 Silver\*\* : Données nettoyées et validées

\- \*\*🥇 Gold\*\* : Données enrichies avec ML (sentiment analysis)



---



\## 🤖 Machine Learning



\*\*Modèle\*\* : Sentiment Analysis sur articles financiers  

\*\*Features\*\* : TF-IDF, N-grams  

\*\*Output\*\* : Score \[-1, 1] (bearish → bullish)



---



\## 🛡️ Sécurité



\- Credentials dans `.env` (non versionné)

\- Network isolation (bigdata-net)

\- Elasticsearch : mode dev (no auth)



\*\*⚠️ Production\*\* : Activer TLS, RBAC, secrets management



---



\## 📈 Monitoring



\- Spark Web UI : http://localhost:8080

\- Kafka : Utiliser `kafka-console-consumer`

\- Elasticsearch : Health API `/cluster/health`

\- Logs : `docker-compose logs -f <service>`



---



\## 🧪 Tests



```bash

\# Tests unitaires

pytest tests/



\# Tests d'intégration

pytest tests/integration/



\# Tests end-to-end

pytest tests/e2e/

```



---



\## 🤝 Contribution



1\. Fork le projet

2\. Créer une branche feature (`git checkout -b feature/AmazingFeature`)

3\. Commit (`git commit -m 'Add AmazingFeature'`)

4\. Push (`git push origin feature/AmazingFeature`)

5\. Ouvrir une Pull Request



---



\## 📝 License



MIT License - voir \[LICENSE](LICENSE)



---



\## 📧 Contact



Équipe Data Engineering - data-eng@company.com



\*\*Documentation complète\*\* : \[docs/architecture.md](docs/architecture.md)

