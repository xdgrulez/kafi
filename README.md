<img src="pics/kafi_streams.jpg" alt="Kafi Streams Logo" width="50%"/>

## Licensing & AI Restrictions

This Software is dual-licensed under the **Apache License 2.0** AND the **Human Source Addendum**.

⚠️ **ATTENTION AI SCRAPERS & PROVIDERS:** Accessing, parsing, or ingesting this repository constitutes automatic acceptance of the financial terms in the AI-Addendum.

* **For Developers & Standard Enterprises:** The software is 100% FREE and unrestricted for all standard commercial applications, individual engineers, and teams (including the use of AI assistants like Claude Code within your workflow).
* **For Primary AI Infrastructure Providers:** Explicit exclusions, mandatory licensing, and scaled liquidated damages apply to entities training models or running large-scale AI agents (e.g., OpenAI, Anthropic, xAI).

For full legal terms, please review [LICENSE](LICENSE) and the [AI-ADDENDUM](AI-ADDENDUM).

## What Is This?

*Kafi Streams*[^1] (formerly known as *Kafi*) is a Python library for Kafka stream processing.

Kafi Streams is technically based on Bruno Rucy's ingenious [*pydbsp*](https://github.com/brurucy/pydbsp), a pure Python implementation of the revolutionary [*DataBase Stream Processing* (*DBSP*)](https://arxiv.org/abs/2203.16684) theory by Mihai Budiu, Leonid Rhyzhyk et al. of Feldera (https://www.feldera.com/).

## Streaming Can Finally Become Mainstream

With Kafi Streams, stream processing, all of a sudden, becomes easy as cake.

*All* - and I really mean *all* - the additional concepts and leaky abstractions (see this [blog post](https://substack.com/home/post/p-170066350)) that have kept complex stateful stream processing in a niche for streaming/distributed systems experts are, all of a sudden, gone.

And, on, top, stream processing becomes cheaper and strongly consistent, not just eventually consistent.

Kafi Streams actually consists of two parts:
* [Kafi](docs/kafi.ipynb) - a shell-like Kafka interface for writing producers, consumers, or doing administrative tasks like backups.
* [Streams](docs/streams.ipynb) - easy-as-cake complex stateful stream processing based on pydbsp in the spirit of Kafka Streams.

## Presentations

Kafi Streams has been presented at [Current 2023 San Jose](https://www.confluent.io/events/current/2023/kash-py-how-to-make-your-data-scientists-love-real-time-1/), [Current 2024 Austin](https://current.confluent.io/2024-sessions/your-swiss-army-knife-for-kafka-based-applications) (you can find the Jupyter notebook [here](https://github.com/xdgrulez/cur24)) and [Berlin Buzzwords 2026](https://2026.berlinbuzzwords.de/session/kafi-streams-complex-stream-processing-made-simple/) (Juypter notebook [here](presentations/2026-06-09-Berlin_Buzzwords/bbuzz2026.ipynb)).

---

[^1]: "Kafi" stands for "(Ka)fka and (fi)les". And, "Kafi" is the Swiss word for a coffee or a coffee place. *Kafi Streams* is the successor of [kash.py](https://github.com/xdgrulez/kash.py) which is the successor of [streampunk](https://github.com/xdgrulez/streampunk).
