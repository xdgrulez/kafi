import datetime, json, os, random, sys, time

if os.path.basename(os.getcwd()) == "kafi":
    sys.path.insert(1, ".")
else:
    sys.path.insert(1, "../..")

from kafi.kafi import *

c = Cluster("local")
c.retouch("transactions", partitions=1)
p = c.producer("transactions")
random.seed(42)
for id_int in range(0, 1000000):
  row_str = json.dumps({
        "id": id_int,
        "from_account": random.randint(0, 9),
        "to_account": random.randint(0, 9),
        "amount": 1,
        "ts": datetime.datetime.now().isoformat(sep=" ", timespec="milliseconds")
    })
  print(row_str)
  p.produce(row_str, key=str(id_int))
  if id_int % 10000 == 0:
    p.flush()
#   time.sleep(0.01)
p.close()
