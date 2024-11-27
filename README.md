![kafi logo](pics/kafi.jpg)

*Kafi*[^1] is a Python library for anybody working with Kafka (or any solution based on the Kafka API). It is your *Swiss army knife for Kafka*. It has already been presented at [Current 2023](https://www.confluent.io/events/current/2023/kash-py-how-to-make-your-data-scientists-love-real-time-1/) and [Current 2024](https://current.confluent.io/2024-sessions/your-swiss-army-knife-for-kafka-based-applications) (you can find the Jupyter notebook [here](https://github.com/xdgrulez/cur24)).

Kafi supports two main modes:
* Real Kafka
  * Kafka API via [confluent_kafka](https://github.com/confluentinc/confluent-kafka-python)
  * Kafka REST Proxy API
* Emulated Kafka/files
  * local file system
  * S3
  * Azure Blob Storage

Emulated Kafka is e.g. useful for debugging, as there is need to run an additional Kafka cluster. It can also be used to download snapshots of Kafka topics or to do backups.

Kafi also fully supports the Schema Registry API, including full support for Avro, Protobuf and JSONSchema.

Kafi is fun to use either in the interactive Python interpreter (acting a bit like a shell), or inside your Python (micro-)service code, and - it's the ideal tool for Kafka in your Jupyter notebooks :-)

This "README" is split into a basic part:
* [Installation](#installation)
* [Basic Configuration](#basic-configuration)
* [Use Cases](#use-cases)

...and a more detailed part:
* [Full Configuration](#full-configuration)
* [More on Producing Messages](#more-on-producing-messages)
* [More on Consuming Messages](#more-on-consuming-messages)

# Installation

Kafi is on PyPI. Hence:

```
pip install kafi
```

# Basic Configuration

Kafi is configured using YAML files. As an example, here is a YAML file for a local Kafka installation, including Schema Registry:

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
2. the `configs/<storage type>/<storage config>` sub-directory of 1 (`.` or `KAFI_HOME`). Here, `storage_type` is either `azblobs`, `clusters`, `locals`, `restproxies` or `s3s` and `storage_config` is your configuration file (in Kafi, a connection to one of its back-ends is called *storage*) 

Within Kafi, you can refer to these files by their name without the `.yml` or `.yaml` suffix, e.g. `local` for `local.yaml`.

You can also use environment variables in the YAML files, e.g.:
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

We provide example YAML files in this GitHub repository under `configs`:
* Real Kafka
  * Kafka API:
    * Local Kafka installation: `clusters/local.yaml`
    * Confluent Cloud: `clusters/ccloud.yaml`
    * Redpanda: `clusters/redpanda.yaml`
  * Kafka REST Proxy API:
    * Local Kafka/REST Proxy installation: `restproxies/local.yaml`
* Emulated Kafka/files
  * local file system: `locals/local.yaml`
  * S3: `s3s/local.yaml`
  * Azure Blob Storage: `azureblobs/local.yaml`

More details on configuring Kafi can be found [here](#full-configuration).

# Use Cases

What can Kafi be for you?

## An Alternative to the Existing CLI Tools

I initially started development on Kafi because I was not a big fan of the existing Kafka CLI tools. Hence, one way Kafi can help you is to act as an alternative to these tools, e.g. those from the Apache Kafka distribution. Just have a look.

To get started, just enter your Python interpreter, import Kafi and create a `Cluster` object (e.g. pointing to your local Kafka cluster):

```
from kafi.kafi import *
c = Cluster("local")
```

### Create Topics

Now you can create topics with a shell-inspired command:

```
c.touch("topic_json")
```

instead of:

```
kafka-topics --bootstrap-server localhost:9092 --topic topic_json --create
```

### List Topics

You can list topics:

```
c.ls()
```

instead of:

```
kafka-topics --bootstrap-server localhost:9092 --list
```

### Produce Messages

Produce messages (pure JSON without schema):

```
p = c.producer("topic_json")
p.produce({"bla": 123}, key="123")
p.produce({"bla": 456}, key="456")
p.produce({"bla": 789}, key="789")
p.close()
```

instead of:

```
kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic topic_json \
  --property parse.key=true \
  --property key.separator=':'

123:{"bla": 123}
456:{"bla": 456}
789:{"bla": 789}
```

### Consume Messages

And consume them:

```
c.cat("topic_json")

[{'topic': 'topic_json', 'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1732660705555), 'key': '123', 'value': {'bla': 123}}, {'topic': 'snacks_json', 'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1732660710565), 'key': '456', 'value': {'bla': 456}}, {'topic': 'snacks_json', 'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1732660714166), 'key': '789', 'value': {'bla': 789}}]
```

instead of:

```
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic topic_json \
  --from-beginning

{"bla": 123}
{"bla": 456}
{"bla": 789}
^CProcessed a total of 3 messages
```

### Produce Messages Using a Schema

#### Avro

Producing messages with a schema is as effortless as possible with Kafi. Here is a simple example using an Avro schema:

```
t = "topic_avro"
s = """
{
    "type": "record",
    "name": "myrecord",
    "fields": [
        {
            "name": "bla",
            "type": "int"
        }
    ]
}
"""
p = c.producer(t, value_type="avro", value_schema=s)
p.produce({"bla": 123}, key="123")
p.produce({"bla": 456}, key="456")
p.produce({"bla": 789}, key="789")
p.close()
```

instead of:

```
kafka-avro-console-producer \
  --broker-list localhost:9092 \
  --topic topic_avro \
  --property schema.registry.url=http://localhost:8081 \
  --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"bla","type":"int"}]}' \
  --property parse.key=true \
  --property key.separator=':'

123:{"bla": 123}
456:{"bla": 456}
789:{"bla": 789}
```


#### Protobuf

```
t = "topic_protobuf"
s = """
message value {
    required int32 bla = 1;
}
"""
p = c.producer(t, value_type="protobuf", value_schema=s)
p.produce({"bla": 123}, key="123")
p.produce({"bla": 456}, key="456")
p.produce({"bla": 789}, key="789")
p.close()
```

instead of:

```
kafka-protobuf-console-producer \
  --broker-list localhost:9092 \
  --topic topic_protobuf \
  --property schema.registry.url=http://localhost:8081 \
  --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property value.schema='message value { required int32 bla = 1; }' \
  --property parse.key=true \
  --property key.separator=':'

123:{"bla": 123}
456:{"bla": 456}
789:{"bla": 789}
```

#### JSONSchema

```
t = "topic_jsonschema"
s = """
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "title": "myrecord",
    "properties": {
      "bla": {
        "type": "integer"
      }
    },
    "required": ["bla"],
    "additionalProperties": false
  }
"""
p = c.producer(t, value_type="jsonschema", value_schema=s)
p.produce({"bla": 123}, key="123")
p.produce({"bla": 456}, key="456")
p.produce({"bla": 789}, key="789")
p.close()
```

instead of:

```
kafka-json-schema-console-producer \
  --broker-list localhost:9092 \
  --topic topic_protobuf \
  --property schema.registry.url=http://localhost:8081 \
  --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property value.schema='{ "$schema": "http://json-schema.org/draft-07/schema#", "type": "object", "title": "myrecord", "properties": { "bla": { "type": "integer" } }, "required": ["bla"], "additionalProperties": false }' \
  --property parse.key=true \
  --property key.separator=':'

123:{"bla": 123}
456:{"bla": 456}
789:{"bla": 789}
```

### Search Messages

```
c.grep("topic_avro", ".*456.*", value_type="avro")

([{'topic': 'topic_avro', 'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1732666578986), 'key': '456', 'value': {'bla': 456}}], 1, 3)
```

instead of:

```
kafka-avro-console-consumer \
  --bootstrap-server localhost:9092 \
  --property schema.registry.url=http://localhost:8081 \
  --topic topic_avro \
  --from-beginning \
  | grep 456

{"bla":456}
^CProcessed a total of 3 message
```

### Supported Serialization/Deserialization Types

The supported types are:
* `bytes`: Pure bytes
* `str`: String (Default for keys)
* `json`: Pure JSON (Default for values)
* `avro`: Avro (requires Schema Registry)
* `protobuf` or `pb`: Protobuf (requires Schema Registry)
* `jsonschema` or `json_sr`: JSONSchema (requires Schema Registry)

You can specify the serialization/deserialization types as follows:
* `key_type`/`key_schema`/`key_schema_id`: Type/schema/schema ID for the key
* `value_type`/`value_schema`/`value_schema_id`: Type/schema/schema ID for the value
* `type`: Same type for both the key and the value

## A Convenient Tool for Schema Registry Administration

You can also use Kafi to directly interact with the Schema Registry API. Here are some examples.

### Get Subjects

```
c.sr.get_subjects()

['topic_avro-value', 'topic_jsonschema-value', 'topic_protobuf-value']
```

### Delete a Subject

First soft-delete:

```
c.sr.delete_subject("topic_avro-value")

[1]
```

Then list the subjects again:

```
c.sr.get_subjects()

['topic_jsonschema-value', 'topic_protobuf-value']
```

List also the soft-deleted subjects:

```
c.sr.get_subjects(deleted=True)

['topic_avro-value', 'topic_jsonschema-value', 'topic_protobuf-value']
```

Then hard-delete the subject:

```
c.sr.delete_subject("topic_avro-value", permanent=True)

[1]
```

And check whether it is really gone:

```
c.sr.get_subjects(deleted=True)

['topic_jsonschema-value', 'topic_protobuf-value']
```

### Get the Latest Version of a Subject

```
c.sr.get_latest_version("topic_jsonschema-value")

{'schema_id': 3, 'schema': {'schema_str': '{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","title":"myrecord","properties":{"bla":{"type":"integer"}},"required":["bla"],"additionalProperties":false}', 'schema_type': 'JSON'}, 'subject': 'topic_jsonschema-value', 'version': 1}
```

etc.

## A Simple Non-stateful Stream Processor

You can also use Kafi as a simple non-stateful stream processing tool.

### Copy Topics

You can use Kafi to just copy topics[^2]:

```
c.cp("topic_json", c, "topic_json_copy")

(3, 3)
```

Of course you can also use schemas here, e.g. you could convert a Protobuf topic to a pure JSON topic:

```
c.cp("topic_protobuf", c, "topic_avro_json_copy", source_value_type="protobuf")

(3, 3)
```

...or copy a pure JSON topic to an Avro topic:

```
s = """
{
    "type": "record",
    "name": "myrecord",
    "fields": [
        {
            "name": "bla",
            "type": "int"
        }
    ]
}
"""
c.cp("topic_json", c, "topic_json_avro_copy", target_value_type="avro", target_value_schema=s)

(3, 3)
```

### Map

In the example below, we use a *single message transform*. In our `map_function`, we add 42 the "bla" fields or all messages from the input topic `topic_json` and write the processed messages to the output topic `topic_json_mapped`:

```
def plus_42(x):
  x["value"]["bla"] += 42
  return x

c.cp("topic_json", c, "topic_json_mapped", map_function=plus_42)

(3, 3)
```

...and look at the result:

```
c.cat("topic_json_mapped")

[{'topic': 'topic_json_mapped', 'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1732668466442), 'key': '123', 'value': {'bla': 165}}, {'topic': 'topic_json_mapped', 'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1732668466442), 'key': '456', 'value': {'bla': 498}}, {'topic': 'topic_json_mapped', 'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1732668466442), 'key': '789', 'value': {'bla': 831}}]
```

Of course, all that also works seamlessly with schemas, for example:

```
c.cp("topic_protobuf", c, "topic_protobuf_json_mapped", map_function=plus_42, source_value_type="protobuf")

(3, 3)
```

### FlatMap

You can also use Kafi for filtering (or exploding) using its `flatmap` functionality. In the example below, we only keep those messages from the input topic `topic_json` where "bla" equals 4711. Only those messages are written to the output topic `topic_json_flatmapped`:

```
def filter_out_456(x):
  if x["value"]["bla"] == 456:
    return [x]
  else:
    return []

c.cp("topic_json", c, "topic_json_flatmapped", flatmap_function=filter_out_456)

(3, 1)
```

### A Simple MirrorMaker

The input and output topics can be on any cluster - i.e., you can easily do simple stream processing *across clusters*. In a sense, Kafi thus allows you to easily spin up your own simple MirrorMaker (below, `c1` is the source cluster, and `c2` the target):

```
c1 = Cluster("cluster1")
c2 = Cluster("cluster2")
c1.cp("my_topic_on_cluster1", c2, "my_topic_on_cluster2")
```

### How to Set the Serialization/Deserialization Types for Stream Processing

This works analogously to setting the serialization/deserialization types above - you just add the prefixes `source_` and `target_`:
* `source_key_type`/`source_key_schema`/`source_key_schema_id`: Type/schema/schema ID for the key of the source topic
* `source_value_type`/`source_value_schema`/`source_value_schema_id`: Type/schema/schema ID for the value of the source topic
* `source_type`: Same type for both the key and the value of the source topic

...and analogously for `target_`.

## A Backup Tool

You can also use Kafi as a backup tool - using its built-in "Kafka emulation".

### Backing up a Topic to Local Disk

In the example, the source (``cluster``) is a real Kafka cluster and the target (``localfs``) is Kafi's Kafka emulation on your local file system. Kafi's Kafka emulation keeps all the Kafka metadata (keys, values, headers, timestamps) such that you can later easily restore the backed-up topics without losing data. We set the type to "bytes" to have a 1:1 carbon copy of the data in our backup (no deserialization/serialization).

```
cluster = Cluster("cluster")
localfs = Local("local")
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

If you are e.g. a data scientist, Kafi can play the role of a bridge between Kafka and files for you. Based on Pandas, it allows you to e.g. transform Kafka topics into Pandas dataframes and vice versa, and similarly for all kinds of file formats:
* CSV
* Feather
* JSON
* ORC
* Parquet
* Excel
* XML

### Get a Snapshot of a Topic as a Pandas Dataframe

This is as simple as:

```
df = c.topic_to_df("topic_protobuf", value_type="protobuf")
df

   bla
0  123
1  456
2  789
```

### Write a Pandas Dataframe to a Kafka Topic

The other way round:

```
c.df_to_topic(df, "topic_json_from_df")
c.cat("topic_json_from_df)

[{'topic': 'topic_json_from_df', 'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1732669665739), 'key': None, 'value': {'bla': 123}}, {'topic': 'topic_json_from_df', 'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1732669666743), 'key': None, 'value': {'bla': 456}}, {'topic': 'topic_json_from_df', 'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1732669666744), 'key': None, 'value': {'bla': 789}}]
```

### Get a Snapshot of a Topic as an Excel File

This is as simple as:

```
l = Local("local")
c.topic_to_file("topic_json", l, "topic_json.xlsx")
```

### Get a Snapshot of a Topic as a Parquet File

Similar:

```
l = Local("local")
c.topic_to_file("topic_json", l, "topic_json.parquet")
```

### Bring a Parquet File back to Kafka

The other way round:

```
l = Local("local")
l.file_to_topic("topic_json.parquet", c, "topic_json_from_parquet")
```

## A Powerful Debug Tool

Because Kafi is just a Python library integrated into the Python ecosystem, it can be a powerful tool for debugging and fixing bugs - for developers and Kafka administrators alike. Here are some examples.

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

# Full Configuration

In Kafi one configuration file corresponds to a "connection" to a so-called *storage* (Kafka API, Kafka REST Proxy API, Local file system, S3 and Azure Blob Storage). Each storage has one section that only makes sense for itself:

* Kafka API: `kafka`
* Kafka REST Proxy API: `rest_proxy`
* Local file system: `local`
* S3: `s3`
* Azure Blob Storage: `azure_blob`

In addition, the storages can all have one or two of the following sections:
* `schema_registry` (Schema Registry configuration)
* `kafi` (additional configuration items)

Please also have a look at the example YAML files in the GitHub repo for further illustration.

## General

The following configuration items are shared across all *storages* (defaults in brackets):

* `schema_registry`
  * `schema.registry.url`
  * `basic.auth.credentials.source`
  * `basic.auth.user.info`

* `kafi`
  * `progress.num.messages` (`1000`)
  * `consume.batch.size` (`1000`)
  * `produce.batch.size` (`1000`)
  * `verbose` (`1` if run in the interactive Python interpreter, `0` if not)
  * `auto.offset.reset` (`earliest`)
  * `consumer.group.prefix` (`""`)
  * `enable.auto.commit` (`false`)
  * `commit.after.processing` (`true`)
  * `key.type` (`str`)
  * `value.type` (`json`)

## Real Kafka

### Kafka API

* `kafka`
  * `bootstrap.servers`
  * `security.protocol`
  * `sasl.mechanisms`
  * `sasl.username`
  * `sasl.password`
  * `log_level`(`3` if run in the interactive Python interpreter, `6` if not))
  * etc. [librdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)

* `kafi`
  * `flush.timeout` (`-1.0`)
  * `retention.ms` (`604800000`)
  * `consume.timeout` (`5.0`)
  * `session.timeout.ms` (`45000`)
  * `block.num.retries` (`10`)
  * `block.interval` (`0.5`)

### Kafka REST Proxy API

* `rest_proxy`:
  * `rest.proxy.url`
  * `basic.auth.user.info`

* `kafi`:
  * `fetch.min.bytes` (`-1`)
  * `consumer.request.timeout.ms` (`1000`)
  * `consume.num.attempts` (`3`)
  * `requests.num.retries` (`10`)

## Kafka Emulation/files

### Local File System

* `local`:
  * `root.dir` (`.`)

### S3

* `s3`:
  * `endpoint`
  * `access.key`
  * `secret.key`
  * `bucket.name` (`test`)
  * `root.dir` (`""`)

### Azure Blob Storage

* `azure_blob`:
  * `connection.string`
  * `container.name` (`test`)
  * `root.dir` (`""`)

# More on Producing Messages

To streamline its syntax, Kafi employs a number of defaults/assumptions. All of them can of course be overridden.

Look at the following code from above:

```
p = c.producer("topic_json")
p.produce({"bla": 123}, key="123")
p.produce({"bla": 456}, key="456")
p.produce({"bla": 789}, key="789")
p.close()
```

Kafi uses the following defaults/assumptions here. First, for setting up the producer object:
* The maximum batch size for producing is set to the corresponding value `produce.batch.size` in the `kafi` section of the configuration file, e.g. `1000` in `clusters/local.yaml`.
* The flush timeout for `flush` calls to the Kafka API is set to the corresponding value `flush.timeout` in the `kafi` section of the configuration file, e.g. `-1.0` in `clusters/local.yaml`.
* The default key type is set to the corresponding value `key.type` in the `kafi` section of the configuration file, e.g. `str` in `clusters/local.yaml`. It can also be overridden with the `key_type` kwargs parameter.
* The default value type is set to the corresponding value `value.type` in the `kafi` section of the configuration file, e.g. `json` in `clusters/local.yaml`. It can also be overridden with the `value_type` kwargs parameter.
* No delivery callback function is called. This can be overridden with the `delivery_function` kwargs parameter.

Then, for each individual `produce` call:
* there are no headers (you can add headers using the `headers` kwargs parameter).
* the target partition is any (=-1) (you can set the target partition explicitly using the `partition` kwargs parameter).
* the timestamp is set automatically using the `CURRENT_TIME` setting (=0) (you can set the timestamp to a specfic value using the `timestamp` kwargs parameter).
* after producing each message, the producer does not call `flush` from the Kafka API (you can control this behavior using the `flush` kwargs parameter).

The call could also be written out as follows, assuming the values from the example configuration file `clusters/local.yaml`:

```
c.produce_batch_size(1000)
c.flush_timeout(-1.0)
c.key_type("str")
c.value_type("json")
p = c.producer("topic_json", delivery_function=None)
p.produce({"bla": 123}, key="123", headers=None, partition=-1, timestamp=0, flush=False)
p.produce({"bla": 456}, key="456", headers=None, partition=-1, timestamp=0, flush=False)
p.produce({"bla": 789}, key="789", headers=None, partition=-1, timestamp=0, flush=False)
p.close()
```

# More on Consuming Messages

For consuming messages, Kafi also makes use of a number of defaults/assumptions.

To illustrate this, look at the following call:

```
c.cat("topic_json")
```

Kafi uses the following defaults/assumptions here. For one, `cat` hides the implicit creation of a consumer object. Then, for setting up the consumer:
* It implicitly creates a new consumer group based on the current timestamp.
* `auto.offset.reset` is set to the corresponding value `auto.offset.reset` in the `kafi` section of the configuration file, e.g. `earliest` in `clusters/local.yaml`.
* `session.timeout.ms` is set to the corresponding value `session.timeout.ms` in the `kafi` section of the configuration file, e.g. `45000` milliseconds in `clusters/local.yaml`.
* `enable.auto.commit` is set to the corresponding value `enable.auto.commit` in the `kafi` section of the configuration file, e.g. `false` in `clusters/local.yaml`.
* The consume timeout is set to the corresponding value `consume.timeout` in the `kafi` section of the configuration file, e.g. `1.0` (for 1 second) in `clusters/local.yaml`. If you set this to -1, Kafi will "wait forever", as in a typical neverending consumer loop.
* The consumer group prefix is set to the corresponding value `consumer.group.prefix` in the `kafi` section of the configuration file, e.g. `""` in `clusters/local.yaml`.
* the maximum batch size for consuming is set to the corresponding value `consume.batch.size` in the `kafi` section of the configuration file, e.g. `1000` in `clusters/local.yaml`.
* The default key type is set to the corresponding value `key.type` in the `kafi` section of the configuration file, e.g. `str` in `clusters/local.yaml`. It can also be overridden with the `key_type` kwargs parameter.
* The default value type is set to the corresponding value `value.type` in the `kafi` section of the configuration file, e.g. `json` in `clusters/local.yaml`. It can also be overridden with the `value_type` kwargs parameter.

And for the `consume` calls:
* It attempts to read infinitely many messages (parameter `n=-1`)

The call could also be written out as follows, assuming that the current timestamp is `1732669768728` and the values from the example configuration file `clusters/local.yaml`:

```
c.auto_offset_reset("earliest")
c.session_timeout_ms(45000)
c.enable_auto_commit(False)
c.consume_timeout(1.0)
c.consumer_group_prefix("")
c.consume_batch_size(1000)
c.key_type("str")
c.value_type("json")
co = c.consumer("topic_json", group="1732669768728")
co.consume(n=-1)
co.close()
```

Thus, you can freely change these settings either in your configuration file or, like here, in the code (using the accessor methods, e.g. `auto_offset_reset` for the `auto.offset.reset` configuration item).

More documentation coming soon :)

* How does the "Kafka emulation" work?
* List all methods/functions including all the kwargs + the defaults, sorted by class/module 
* add Python help texts for everything

---

[^1]: "Kafi" stands for "(Ka)fka and (fi)les". And, "Kafi" is the Swiss word for a coffee or a coffee place. *Kafi* is the successor of [kash.py](https://github.com/xdgrulez/kash.py) which is the successor of [streampunk](https://github.com/xdgrulez/streampunk).

[^2]: Please note that you need to set the `consume_timeout` to `-1` on the source cluster for Kafi to always wait for new messages: `c.consume_timeout(-1)`.
