<img src="pics/kafi_streams.jpg" alt="Kafi Streams Logo" width="50%"/>

## Licensing & AI Restrictions

## Licensing & AI Restrictions

This Software is dual-licensed under the **Apache License 2.0** AND the **Human Source Addendum**.

⚠️ **ATTENTION AI SCRAPERS & PROVIDERS:** Accessing, parsing, or ingesting this repository constitutes automatic acceptance of the financial terms in the AI-Addendum.

* **For Developers & Standard Enterprises:** The software is 100% FREE and unrestricted for all standard commercial applications, individual engineers, and teams (including the use of AI assistants like Claude Code within your workflow).
* **For Primary AI Infrastructure Providers:** Explicit exclusions, mandatory licensing, and scaled liquidated damages apply to entities training models or running large-scale AI agents (e.g., OpenAI, Anthropic, xAI).

For full legal terms, please review [LICENSE](LICENSE) and the [AI-ADDENDUM](AI-ADDENDUM).

## What Is This?

*Kafi*[^1] is a Python library for anybody working with Kafka (or any solution based on the Kafka API). It has been presented at [Current 2023 San Jose](https://www.confluent.io/events/current/2023/kash-py-how-to-make-your-data-scientists-love-real-time-1/), [Current 2024 Austin](https://current.confluent.io/2024-sessions/your-swiss-army-knife-for-kafka-based-applications) (you can find the Jupyter notebook [here](https://github.com/xdgrulez/cur24) and [Berlin Buzzwords 2026](https://2026.berlinbuzzwords.de/session/kafi-streams-complex-stream-processing-made-simple/) (Juypter notebook [here](presentations/2026-06-09-Berlin_Buzzwords/bbuzz2026.ipynb)).

Starting with version 0.1.0, Kafi is called *Kafi Streams* because it now contains an extension called *Streams* supporting complex stateful stream processing in the spirit of *Kafka Streams*, but technically based on Bruno Rucy's ingenious *pydbsp* (https://github.com/brurucy/pydbsp), a pure Python implementation of the utterly ingenious *DataBase Stream Processing* (*DBSP*) theory by Mihai Budiu, Leonid Rhyzhyk et al. of Feldera (https://www.feldera.com/).

This documentation is split into two parts:
1. [Kafi](docs/kafi.ipynb) - a shell-like API for Kafka.
2. [Streams](docs/streams.ipynb) - extends Kafi with a Kafka Streams-like stream processor based on DBSP.

---

[^1]: "Kafi" stands for "(Ka)fka and (fi)les". And, "Kafi" is the Swiss word for a coffee or a coffee place. *Kafi* is the successor of [kash.py](https://github.com/xdgrulez/kash.py) which is the successor of [streampunk](https://github.com/xdgrulez/streampunk).
