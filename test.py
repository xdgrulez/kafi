from kafi.kafi import *

c = Cluster("local")
l = Local("local")
l.rm("uje1")
c.cp("uje", l, "uje1")
