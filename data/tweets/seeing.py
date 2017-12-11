from __future__ import print_function
import ast, json
from pprint import *

fin = open("BillGates_2017-09-17 00-05-15.191513.txt","r")
l = json.loads(fin.read())
print(len(l))
pprint(len(l[0]))
pprint(l[0][0])
for x in l:
    print(len(x))
