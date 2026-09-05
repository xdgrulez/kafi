import lancedb
from mcp.server.fastmcp import FastMCP
from kafi.streams.streams import Streams
from kafi.kafka.cluster.cluster import Cluster
from sentence_transformers import SentenceTransformer

# 1. In-Process Stores & Embedding Model setup
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
db = lancedb.connect("./lancedb_data")
# LanceDB Schema / Table init
table = db.create_table(
    "customer_context",
    data=[{"id": "init", "vector": embedding_model.encode("init").tolist(), "text": "init", "customer_id": "0"}],
    mode="overwrite"
)

# 2. Sink Function: Streaming Incremental Embedding & LanceDB Upsert (Zero-Drift)
def lancedb_upsert_sink(record):
    val = record["value"]
    doc_id = f"order_{val['order_id']}"
    text_repr = f"Order #{val['order_id']} for Customer {val['name']} (ID: {val['customer_id']}) status: {val['status']}, amount: {val['amount']} EUR"
    
    vector = embedding_model.encode(text_repr).tolist()
    
    # In-Memory Zero-Drift Upsert: Ersetzt sofort veraltete Vektoren mit demselben Primary Key
    table.merge_insert("id") \
         .when_matched_update_all() \
         .when_not_matched_insert_all() \
         .execute([{"id": doc_id, "vector": vector, "text": text_repr, "customer_id": val["customer_id"]}])

# 3. Kafi Streams Topology Setup (gemäss deinem DSL Pattern)
c = Cluster({"kafka": {"bootstrap.servers": "localhost:9092"}})

orders_tn = (
    Streams.source(c, "orders")
    .map(lambda r: {"order_id": r["value"]["order_id"], "customer_id": r["value"]["customer_id"], "status": r["value"]["status"], "amount": r["value"]["amount"]})
    .distinct()
)

customers_tn = (
    Streams.source(c, "customers")
    .map(lambda r: {"id": r["value"]["id"], "name": r["value"]["name"]})
    .distinct()
)

# Join Orders x Customers & Sink via sink_fun direkt in LanceDB
sink_tn = (
    orders_tn
    .join(
        customers_tn,
        lambda l: l["customer_id"],
        lambda r: r["id"],
        lambda l, r: {"value": {**l, "name": r["name"]}}
    )
    .sink_fun(lancedb_upsert_sink)
)

topology = Streams.build(sink_tn)
stop_streams = Streams.start_streams(topology)

# 4. FastMCP Server Setup (Stellt den Zero-Drift State für Agenten bereit)
mcp = FastMCP("Zero-Drift-Agentic-Memory")

@mcp.tool()
def search_realtime_context(customer_id: str, query: str) -> str:
    """Sucht garantiert frischen Real-Time Kontext für einen Kunden aus LanceDB."""
    query_vector = embedding_model.encode(query).tolist()
    results = table.search(query_vector) \
                   .where(f"customer_id = '{customer_id}'") \
                   .limit(3) \
                   .to_list()
    if not results:
        return "Keine Daten für diesen Kunden gefunden."
    return "\n".join([r["text"] for r in results])

if __name__ == "__main__":
    # Startet den MCP Server (SSE / Stdio Mode für Claude Desktop / Cursor / Agents)
    mcp.run()

#

@mcp.tool()
def search_customer_context(customer_id: str, query: str) -> str:
    """Sucht semantisch im garantiert frischen Real-Time Kontext eines Kunden."""
    query_vector = embedding_model.encode(query).tolist()
    
    # Hybrid Search: Exakter Key-Filter + Vektor-Ähnlichkeit
    results = table.search(query_vector) \
                   .where(f"customer_id = '{customer_id}'") \
                   .limit(3) \
                   .to_list()
    
    if not results:
        return f"Keine relevanten Ereignisse für Kunde {customer_id} gefunden."
    
    # Formatierung für den Agenten
    context_output = [f"--- GEFUNDENER ECHTZEIT-KONTEXT FÜR KUNDE {customer_id} ---"]
    for r in results:
        context_output.append(f"• Event-ID: {r['id']}")
        context_output.append(f"  Inhalt: {r['text']}")
        context_output.append(f"  Relevanz-Score: {round(r['_distance'], 3)}")
        context_output.append("")
        
    return "\n".join(context_output)