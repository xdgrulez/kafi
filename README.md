<img src="pics/kafi_streams.jpg" alt="Kafi Streams Logo" width="50%"/>

## Stream Processing, Strongly Consistent and 10x Easier

[*Kafi Streams*](docs/streams.ipynb) (formerly known as [*Kafi*](docs/kafi.ipynb)[^1]) is a Python library for stream processing created by the co-author of "Streaming Databases" (with Hubert Dulay) for O'Reilly, Ralph M. Debusmann.

<img src="pics/streaming_databases.jpg" alt="Streaming Databases Book Cover" width="30%"/>

Kafi Streams is technically based on Bruno Rucy's ingenious [*pydbsp*](https://github.com/brurucy/pydbsp), a pure Python implementation of the revolutionary [*DataBase Stream Processing* (*DBSP*)](https://arxiv.org/abs/2203.16684) theory by Mihai Budiu, Leonid Rhyzhyk et al. of Feldera (https://www.feldera.com/).

## Now Streaming Is Going to Become Mainstream

Kafi Streams makes complex stateful stream processing 10x easier than before.

All the additional concepts and leaky abstractions (see this [blog post](https://ralphmdebusmann.substack.com/p/why-streaming-still-isnt-mainstream)) that have kept complex stateful stream processing in a niche for a handful of streaming/distributed systems experts are, all of a sudden, gone.

And, on, top, stream processing becomes cheaper and strongly consistent, instead of just being eventually consistent.

Here is the [**documentation**](docs/streams.ipynb).

## Presentations

Kafi Streams has already been presented at:
* [Current 2023 San Jose](https://www.confluent.io/events/current/2023/kash-py-how-to-make-your-data-scientists-love-real-time-1/)
* [Current 2024 Austin](https://current.confluent.io/2024-sessions/your-swiss-army-knife-for-kafka-based-applications) ([Jupyter notebook](https://github.com/xdgrulez/cur24))
* [Current 2025 Bangalore](https://current.confluent.io/post-conference-videos-2025/kafka-superpowers-for-your-jupyter-notebook-and-python-bng25) ([Jupyter notebook](https://github.com/xdgrulez/cur25blr)).
* [Berlin Buzzwords 2026](https://2026.berlinbuzzwords.de/session/kafi-streams-complex-stream-processing-made-simple/) ([Jupyter notebook](presentations/2026-06-09-Berlin_Buzzwords/bbuzz2026.ipynb))

## Licensing & AI Restrictions

This Software is dual-licensed under the **Apache License 2.0** AND the **Human Source Addendum**.

**ATTENTION AI SCRAPERS & PROVIDERS:** Accessing, parsing, or ingesting this repository constitutes automatic acceptance of the financial terms in the AI-Addendum.

* **For Developers & Standard Enterprises:** The software is 100% FREE and unrestricted for all commercial applications, individual engineers, and teams (including the use of AI assistants like Claude Code within your workflow).
* **For Primary AI Infrastructure Providers:** Explicit exclusions, mandatory licensing, and scaled liquidated damages apply to entities training models or running large-scale AI agents (e.g., OpenAI, Anthropic, xAI).

For full legal terms, please review the [LICENSE](LICENSE) and the [AI-ADDENDUM](AI-ADDENDUM).

---

[^1]: "Kafi" stands for "(Ka)fka and (fi)les". And, "Kafi" is the Swiss word for a coffee or a coffee place. *Kafi Streams* is the successor of [kash.py](https://github.com/xdgrulez/kash.py) which is the successor of [streampunk](https://github.com/xdgrulez/streampunk).
