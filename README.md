# Kafka POC

POC en ligne de commande d'un producteur et d'un consommateur Kafka. Un broker Kafka mono-nœud
(mode KRaft, sans Zookeeper) tourne dans Docker ; deux applications Java autonomes s'y connectent
depuis l'hôte pour envoyer et recevoir des messages sur le topic `poc-topic`.

## Prérequis

- Docker (avec Docker Compose)
- JDK (le wrapper Gradle se charge du reste)

## 1. Démarrer le broker Kafka

```bat
docker compose up -d
```

Le broker écoute sur `localhost:9092`. Pour l'arrêter :

```bat
docker compose down
```

## 2. Lancer le producteur

```bat
gradlew.bat producer
```

Tape une ligne de texte puis Entrée pour l'envoyer sur le topic `poc-topic`. Tape `exit` pour quitter.

## 3. Lancer le service REST (persistance SQLite)

Dans un autre terminal, avant de lancer le consommateur :

```bat
gradlew.bat restApi
```

Démarre un serveur HTTP sur `localhost:8090` avec deux endpoints :

- `POST /consumer` — body JSON `{"message": "..."}`, insère le message dans la table `t_consumer`
  d'une base SQLite (`data/consumer.db`, créée automatiquement).
- `GET /consumer` — renvoie la liste de tous les messages stockés, au format JSON.

## 4. Lancer le consommateur

Dans un autre terminal :

```bat
gradlew.bat consumer
```

Le consommateur s'abonne à `poc-topic` (groupe `poc-consumer-group`), affiche en continu
partition/offset/valeur de chaque message reçu, et appelle `POST http://localhost:8090/consumer`
pour chaque message afin de le persister dans SQLite. `Ctrl+C` pour arrêter.

> Le broker et le service REST doivent être démarrés avant le consommateur : celui-ci se connecte à
> `localhost:9092` (Kafka) et `localhost:8090` (REST) sans retry/backoff au-delà de celui de leurs
> clients respectifs.

## Consulter les messages stockés

```bat
curl http://localhost:8090/consumer
```

## Autres commandes utiles

```bat
gradlew.bat build     REM compile + lance les tests
gradlew.bat test      REM lance tous les tests
```
