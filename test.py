from kafi.kafi import *
c = Cluster("local")
a = AzureBlob("local")
c.to_file("scored_protobuf", a, "scored.xlsx", type="protobuf")