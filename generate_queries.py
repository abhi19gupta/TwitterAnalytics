from __future__ import print_function
from pprint import *
import jinja2 as jj

from neo4j.v1 import GraphDatabase, basic_auth
from datetime import datetime
import json
import os
import time


match_s = """MATCH {% for aa in act_att %}({{aa[0]}} {{aa[1]}}),{% endfor %}"""
where_s = """ WHERE {% for rel in rels %} ({{rel[0]}}) -[:{{rel[1]}}]-> ({{rel[2]}}) AND {% endfor %}"""
return_s  = """RETURN {% for po in prop_vars %} {{po[0]}}({{po[1]}}), {% endfor %}"""
total_s = """
{% for p in parts %}{{p}}
{% endfor %}
"""

# x_template = jj.Template(x)
# x_code = x_template.render(act_att=[("u1 :USER","{id:12}"),("u2 :USER","{id:15}")])
# print(x_code)
def generate_simple_query(actors,attributes,relations,return_values):
    actors = [x[0]+" :"+x[1] for x in actors]
    attribs = [", ".join(["{"+x[i][0]+":"+str(x[i][1])+"}" for i in range(len(x))]) for x in attributes]
    act_att = zip(actors,attribs)

    m_template = jj.Template(match_s)
    m_code = m_template.render(act_att=act_att)
    w_template = jj.Template(where_s)
    w_code = w_template.render(rels=relations)
    r_template = jj.Template(return_s)
    r_code = r_template.render(prop_vars=return_values)

    m_code = m_code.rstrip().rstrip(",")
    w_code = w_code.rstrip().rstrip("AND")
    r_code = r_code.rstrip().rstrip(",")

    print(m_code)
    print(w_code)
    print(r_code)

    tot_template = jj.Template(total_s)
    tot_code = tot_template.render(parts=[m_code,w_code,r_code])
    print(tot_code)

generate_simple_query(actors=[("u1","USER"),("u2","USER"),("x","USER")],attributes=[[("id",101311381)],[("id",44196397)],[]],relations=[("x","FOLLOWS","u1"),("x","FOLLOWS","u2")],return_values=[("count","x")])

