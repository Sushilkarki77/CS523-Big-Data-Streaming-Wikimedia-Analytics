# Phase 0 — Docker stack inventory (completed)

For the full build plan (Phases 1–7), see **[implementation-plan.md](./implementation-plan.md)**.

This inventory reflects containers detected on the developer machine during Phase 0 verification. Service URLs use **localhost** because Kafka/Zookeeper/Postgres and Hadoop ports are published to the host. Adjust hosts if your environment differs.

## Docker network

| Property | Value |
|----------|--------|
| Compose-style network name | `cs523bdt-devenvsetupforstudents_default` |
| Purpose | `kafka-server` and `cs523bdt-lab` share this network so the lab container can reach Kafka at hostname **`kafka-server`**. |

## Running containers

| Container | Image | Host ports (published) | Role |
|-----------|--------|-------------------------|------|
| `kafka-server` | `mmukadam/kafka:v7.4.0` | **9092** | Kafka broker |
| `zookeeper-server` | `mmukadam/zookeeper:v3.8` | **2181** | ZooKeeper for Kafka |
| `cs523bdt-lab` | `mmukadam/cs523bdt-lab:v4.0` | **4040**, **8088**, **9870**, **10000**, **16010** | Hadoop/Hive/HBase/Spark tooling (see below) |
| `hive-metastore-db` | `mmukadam/postgres:v16` | **5432** | PostgreSQL backing Hive metastore |

## Kafka — verified

| Check | Result |
|-------|--------|
| Broker CLI (`kafka-topics`, inside `kafka-server`) | Works (`kafka-topics` is on `PATH` as **`kafka-topics`**, not `kafka-topics.sh`) |
| Topic created for health check | `phase0-healthcheck` (3 partitions, RF=1) |
| Produce / consume (inside broker container) | **OK** |

### Bootstrap addresses

| Client location | Bootstrap servers | Notes |
|-----------------|-------------------|--------|
| Inside Docker (e.g. Spark driver in `cs523bdt-lab`) | `kafka-server:9092` | Matches `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka-server:9092`. |
| On Windows host (Python/Java producer) | Prefer **`kafka-server:9092`** after adding a hosts entry (see below). Using only `localhost:9092` may fail after metadata refresh because advertised hostname is `kafka-server`. |

**Hosts file workaround (Windows):** add:

```text
127.0.0.1 kafka-server
```

Then use **`kafka-server:9092`** from host clients.

## Hadoop stack inside `cs523bdt-lab`

Versions from container environment:

| Component | Version |
|-----------|---------|
| Hadoop | 3.2.1 |
| Hive | 3.1.2 |
| Spark | 3.1.2 |
| HBase | 2.2.0 |
| Kafka (bundled in lab image) | 3.4.0 |

### Processes observed during Phase 0

Only **HDFS** and **YARN** were running (`NameNode`, `DataNode`, `SecondaryNameNode`, `ResourceManager`, `NodeManager`). **HiveServer2**, **HBase**, and **Spark** were **not** listening yet — start them when you reach those project phases (per course/lab instructions).

### Useful URLs from the host (when daemons are up)

| Port | Typical service | Verified during Phase 0 |
|------|-----------------|-------------------------|
| **9870** | HDFS NameNode UI | HTTP **200** |
| **8088** | YARN ResourceManager UI | HTTP **302** (redirect — UI reachable) |
| **4040** | Spark application UI | Only when a Spark app binds UI (not up until you run Spark) |
| **10000** | HiveServer2 JDBC endpoint | Start HiveServer2 first |
| **16010** | HBase Master UI | Start HBase first |

Inside the lab container, **HDFS NameNode RPC** listens on **`127.0.0.1:9000`** only — clients **inside** the lab container should use **`hdfs://localhost:9000`** (or the URI from `core-site.xml`). Confirm with:

```bash
docker exec cs523bdt-lab bash -c 'grep -E "fs.defaultFS|dfs.namenode.http-address" $HADOOP_CONF_DIR/core-site.xml $HADOOP_CONF_DIR/hdfs-site.xml 2>/dev/null'
```

## Hive metastore database (PostgreSQL)

| Setting | Value |
|---------|--------|
| Host (from Windows host) | `localhost` |
| Port | **5432** |
| Database | `hive_metastore` |
| User | `hive` |
| Password | `hivepassword` |

Use these only for troubleshooting metastore connectivity — normal Hive workflows go through Hive/HDFS.

## Phase 0 exit criteria (this environment)

- [x] Container roles and published ports documented  
- [x] Kafka topic list / create / produce / consume verified inside `kafka-server`  
- [x] `KAFKA_ADVERTISED_LISTENERS` noted for host-side producers  
- [x] HDFS/YARN partially verified via HTTP; Hive/HBase/Spark noted as not yet started  
- [x] Hive Postgres metastore credentials captured  

## Next implementation phase

Pick **one** sink early (**HBase** vs **Hive**) and confirm required daemons start cleanly in `cs523bdt-lab`, then implement **Part 1** (producer → Kafka).

## Optional cleanup

To remove the Phase 0 test topic:

```bash
docker exec kafka-server kafka-topics --bootstrap-server localhost:9092 --delete --topic phase0-healthcheck
```
