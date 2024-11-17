![kafi logo](pics/kafi.jpg)

*Kafi*[^1] is a Python library for anybody working with Kafka (or any solution based on the Kafka API). It is your *Swiss army knife for Kafka*. It has already been presented at [Current 2023](https://www.confluent.io/events/current/2023/kash-py-how-to-make-your-data-scientists-love-real-time-1/) and [Current 2024](https://current.confluent.io/2024-sessions/your-swiss-army-knife-for-kafka-based-applications) (you can find the Jupyter notebook [here](https://github.com/xdgrulez/cur24)).

Kafi supports two main modes:
* Real Kafka
  * Kafka API via [confluent_kafka](https://github.com/confluentinc/confluent-kafka-python)
  * Kafka REST Proxy API
* Emulated Kafka
  * local file system
  * S3
  * Azure Blob Storage

Emulated Kafka is e.g. useful for debugging, as there is need to run an additional Kafka cluster. It can also be used to download snapshots of Kafka topics or doing backups.

Kafi also supports the Schema Registry API, including full support for Avro, Protobuf and JSONSchema.

# Installation

Kafi is on PyPI. Hence:

```
pip install kafi
```

# Configuration

Kafi is configured using YAML files. As an example, here is a YAML file for a local real Kafka installation, including Schema Registry:

```
kafka:
  bootstrap.servers: localhost:9092

schema_registry:
  schema.registry.url: http://localhost:8081
```

And this is a YAML file for a local emulated Kafka in the /tmp-directory:

```
local:
  root.dir: /tmp
```

Kafi is looking for these YAML files in:
1. the local directory (`.`) or the directory set in `KAFI_HOME` (if set)
2. the `configs/<storage type>/<storage>` sub-directory of 1 (`.` or `KAFI_HOME`).

Within Kafi, you can refer to these files by their name without the `.yml` or `.yaml` suffix, e.g. `local` for `local.yaml`.

You can also use environment variables within the YAML files, e.g.:
```
kafka:
  bootstrap.servers: ${KAFI_KAFKA_SERVER}
  security.protocol: SASL_SSL
  sasl.mechanisms: PLAIN
  sasl.username: ${KAFI_KAFKA_USERNAME}
  sasl.password: ${KAFI_KAFKA_PASSWORD}
  
schema_registry:
  schema.registry.url: ${KAFI_SCHEMA_REGISTRY_URL}
  basic.auth.credentials.source: USER_INFO
  basic.auth.user.info: ${KAFI_SCHEMA_REGISTRY_USER_INFO}
```

We provide example YAML files in this GitHub repository under `configs`, e.g. for Confluent Cloud and Redpanda (real Kafka), and local disk, S3 and Azure Blob Storage (emulated Kafka, files).

# Use Cases

What can Kafi be for you?

## An Alternative to the Existing CLI Tools

I initially started development on Kafi because I was not a big fan of the existing Kafka CLI tools. Hence, one way Kafi can help you is to act as an alternative to these tools, e.g. those from the Apache Kafka distribution.

To get started, just enter your Python interpreter, import Kafi and create a `Cluster` object (e.g. pointing to your local Kafka cluster):

```
from kafi.kafi import *
c = Cluster("local")
```

### Create Topics

Now you can create topics with a shell-inspired command:

```
c.touch("my_topic")
```

instead of:

```
kafka-topics --bootstrap-server localhost:9092 --topic my_topic --create
```

### List Topics

Or list topics:

```
c.ls()
```

instead of:

```
kafka-topics --bootstrap-server localhost:9092 --list
```

### Produce Messages

Produce messages:

```
p = c.producer("my_topic")
p.produce({"bla": 123}, key="123")
p.close()
```

instead of:

```
kafka-console-producer --bootstrap-server localhost:9092 --topic my_topic --property "parse.key=true" --property "key.separator=:"
> 123:{"bla": 123}
```

### Consume Messages

And consume them:

```
c.cat("my_topic")
```

instead of:

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic my_topic --from-beginning
```

### Search Messages

```
c.grep("my_topic", ".*bla.*")
```

instead of:

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic my_topic --from-beginning | grep bla
```

## A Debug Tool

Kafi can help you debugging and fixing bugs on Kafka. Here are some examples.

### Check for Missing Magic Byte

A typical reoccurring problem is that at the beginning of their development, producers forget to use a proper serializer and the first bunch of messages on dev are not e.g. JSONSchema-serialized. This is how you can find the first N messages in a topic that do not start with the *magic byte* 0:

```
c.filter("my_topic", type="bytes", filter_function=lambda x: x["value"][0] != 0)
```

### Delete Records

Kafi supports all of the not-too-specific AdminClient methods of [confluent_kafka](https://github.com/confluentinc/confluent-kafka-python), so you can use it to do (and automate) all kinds of configuration tasks. For example deleting the first 100 messages of a topic:

```
c.delete_records({"my_topic": {0: 100}})
```

...and then to get the watermarks of a topic:

```
c.watermarks("my_topic")
```

etc.

### Collect all Schemas Used in a Topic

Kafi has full support for the Schema Registry API. Hence, you can list, create, delete and update subjects/schemas etc.

The following Kafi code snippet collects the list of schema IDs used in a topic and prints out the corresponding schemas retrieved from the Schema Registry:

```
def collect_ids(acc, x):
  id = int.from_bytes(x["value"][1:5], "big")
  acc.add(id)
  return acc

(ids, _) = c.foldl("my_topic", collect_ids, set(), type="bytes")

for id in ids:
  print(c.sr.get_schema(id))
```

## A Simple Non-stateful Stream Processor

You can also use Kafi as a simple non-stateful stream processing tool.


### Copy Topics

You can use Kafi to just copy topics[^2]:

```
c.cp("my_topic", c, "my_copied_topic")
```

### Map

In the example below, we use a *single message transform*. In our `map_function`, we add 42 the "bla" fields or all messages from the input topic `my_topic` and write the processed messages to the output topic `my_mapped_topic`:

```
def plus_42(x):
  x["value"]["bla"] += 42
  return x

c.cp("my_topic", c, "my_mapped_topic", map_function=plus_42)
```

### FlatMap

You can also use Kafi for filtering (or exploding) using its `flatmap` functionality. In the example below, we only keep those messages from the input topic `my_topic` where "bla" equals 4711. Only those messages are written to the output topic `my_flatmapped_topic`:

```
def filter_out_4711(x):
  if x["value"]["bla"] == 4711:
    return [x]
  else:
    return []

c.cp("my_topic", c, "my_flatmapped_topic", flatmap_function=filter_out_4711)
```

### A Simple MirrorMaker

The input and output topics can be on any cluster - i.e., you can easily do simple stream processing *across clusters*. In a sense, Kafi thus allows you to easily spin up your own simple MirrorMaker (below, `c1` is the source cluster, and `c2` the target):

```
c1.cp("my_topic_on_cluster1", c2, "my_topic_on_cluster2")
```

## A Backup Tool

You can also use Kafi as a backup tool - using its built-in "Kafka emulation".

### Backing up a Topic to Local Disk

In the example, the source (``cluster``) is a real Kafka cluster and the target (``localfs``) is Kafi's Kafka emulation on your local file system. Kafi's Kafka emulation keeps all the Kafka metadata (keys, values, headers, timestamps) such that you can later easily restore the backed-up topics without losing data. We set the type to "bytes" to have a 1:1 carbon copy of the data in our backup (no deserialization/serialization).

```
cluster.cp("my_topic", localfs, "my_topic_backup", type="bytes")
```

### Restoring a Backed-up Topic to Kafka

Below, we bring back the backed-up data to Kafka:

```
localfs.cp("my_topic_backup", cluster, "my_topic", type="bytes")
```

### Backing up a Topic to S3

Works exactly in the same way, you just need to configure `s3` correctly beforehand:

```
cluster.cp("my_topic", s3, "my_topic_backup", type="bytes")
```

## A Bridge from Kafka to Files

If you are e.g. a data scientist, Kafi can play the role of a bridge between Kafka and files for you. Based on Pandas, it allows you to e.g. transform Kafka topics into all kinds of file formats:
* CSV
* Feather
* JSON
* ORC
* Parquet
* Excel
* XML

### Get a Snapshot of a Topic as an Excel File

This is as simple as:

```
c.kafka_to_file("my_topic", l, "my_topic.xlsx")
```

### Get a Snapshot of a Topic as a Parquet File

Similar:

```
c.kafka_to_file("my_topic", l, "my_topic.parquet")
```

### Bring a Parquet File back to Kafka

The other way round:

```
l.file_to_kafka("my_topic.parquet", c, "my_topic")
```

---

[^1]: "Kafi" stands for "(Ka)fka and (fi)les". And, "Kafi" is the Swiss word for a coffee or a coffee place. *Kafi* is the successor of [kash.py](https://github.com/xdgrulez/kash.py) which is the successor of [streampunk](https://github.com/xdgrulez/streampunk).

[^2] Please note that you need to set the `consume_timeout` to `-1` on the source cluster for Kafi to always wait for new messages: `c.consume_timeout(-1)`.
