from __future__ import print_function
from pprint import *
import jinja2 as jj

from neo4j.v1 import GraphDatabase, basic_auth
from datetime import datetime
import json
import os
import time

# driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))
# session = driver.session()


##OLD CODE : will clean up later
# match_s = """MATCH {% for aa in act_att %}({{aa[0]}} {{aa[1]}}),{% endfor %}"""
# where_s = """WHERE {% for rel in rels %} ({{rel[0]}}) -[:{{rel[1]}}]-> ({{rel[2]}}) AND {% endfor %}"""
# return_s  = """RETURN {% for po in prop_vars %} {{po[0]}}({{po[1]}}), {% endfor %}"""
# total_s = """
# {% for p in parts %}{{p}}
# {% endfor %}
# """

# x_template = jj.Template(x)
# x_code = x_template.render(act_att=[("u1 :USER","{id:12}"),("u2 :USER","{id:15}")])
# print(x_code)
# def generate_simple_query(actors,attributes,relations,return_values):
#     actors = [x[0]+" :"+x[1] for x in actors]
#     attribs = [", ".join(["{"+x[i][0]+":"+str(x[i][1])+"}" for i in range(len(x))]) for x in attributes]
#     act_att = zip(actors,attribs)

#     m_template = jj.Template(match_s)
#     m_code = m_template.render(act_att=act_att)
#     w_template = jj.Template(where_s)
#     w_code = w_template.render(rels=relations)
#     r_template = jj.Template(return_s)
#     r_code = r_template.render(prop_vars=return_values)

#     m_code = m_code.rstrip().rstrip(",")
#     w_code = w_code.rstrip().rstrip("AND")
#     r_code = r_code.rstrip().rstrip(",")

#     print(m_code)
#     print(w_code)
#     print(r_code)

#     tot_template = jj.Template(total_s)
#     tot_code = tot_template.render(parts=[m_code,w_code,r_code])
#     return tot_code

# sq = generate_simple_query(actors=[("u1","USER"),("u2","USER"),("x","USER")],attributes=[[("id",101311381)],[("id",44196397)],[]],relations=[("x","FOLLOWS","u1"),("x","FOLLOWS","u2")],return_values=[("count","x")])
# result = session.run(sq,{})
# for r in result:
#     print(r)
# session.close()
###########################################################################################################

match_frames = """{% for i in num_frames %} (run:RUN) -[:HAS_FRAME]-> (frame{{i}}:FRAME),{% endfor %}"""
where_frames = """WHERE {% for ft in frame_times %} frame{{ft[0]}}.end_t >= {{ft[1]}} AND frame{{ft[0]}}.start_t <= {{ft[2]}} AND{% endfor %}"""
match_events = """{% for e in events %}(frame{{e[0]}}) -[:{{e[1]}}]-> (event{{e[0]}} :{{e[2]}}), {% endfor %}"""
match_actors = """{% for e in events %}(event{{e[0]}}) -[:{{e[1]}}]-> ({{e[2]}}), {% endfor %}"""
match_actors2 = """{% for r in rels %}({{r[0]}}) -[:{{r[1]}}]-> ({{r[2]}}), {% endfor %}"""
tweet_attribs = """{% for tp in tweet_properties %}({{tp[0]}}) -[:{{tp[1]}}]-> ({{tp[2]}}), {% endfor %}"""
returns  = """RETURN {% for po in prop_vars %} {{po[0]}}({{po[1]}}), {% endfor %}"""
total_query = """
{% for p in parts %}{{p}}
{% endfor %}
"""

encode = {"TWEETED":("HAS_TWEET","TWEET_EVENT"),"STARTED_FOLLOWING":("HAS_FOLLOW","FOLLOW_EVENT")}
encode_left = {"TWEETED":"TE_USER","STARTED_FOLLOWING":"FE_FOLLOWER"}
encode_right = {"TWEETED":"TE_TWEET","STARTED_FOLLOWING":"FE_FOLLOWED"}

def generate_node(var,type,props):
    pt = "{{var}} {{type}} {% raw %}{{% endraw %}{% for av in props %}{{av[0]}}:{{av[1]}}, {% endfor %}"
    t = jj.Template(pt)
    c = t.render(props=props,var=var,type=type)
    c = c.strip().strip(",")
    return c+"}"
order = ["STARTED_FOLLOWING","TWEETED","FOLLOWS"]
def create_query(actors,attributes,relations, return_values):
    relations.sort(key=lambda x:order.index(x[1])) ##required because we write the ma_c first and then ma_c2.
    types = {}
    properties = {}
    for x in actors:
        types[x[0]] = ":"+x[1]
    for i in range(len(actors)):
        properties[actors[i][0]] = attributes[i]

    frame_times = []
    match_events_l = []
    match_actors_l = []
    match_actors_l2 = []
    already_described = []
    num_frames = 0
    for x in relations: ## a relation is(actor1,rel,actor2,time1,time2)
        if(x[3]!="" and x[4]!=""):
            num_frames+=1
            frame_times.append((num_frames,x[3],x[4]))
            e_enc = encode[x[1]]
            match_events_l.append((num_frames,e_enc[0],e_enc[1]))
            enc_left = encode_left[x[1]]
            enc_right = encode_right[x[1]]
            if(x[1]=="TWEETED"):
                if(x[2] in already_described):
                    match_actors_l.append((num_frames,enc_left,generate_node(x[2],"",[])))
                else:
                    match_actors_l.append((num_frames,enc_right,generate_node(x[2],types[x[2]],[])))
                    already_described.append(x[2])
                if(x[0] in already_described):
                    match_actors_l.append((num_frames,enc_left,generate_node(x[0],"",[])))
                else:
                    match_actors_l.append((num_frames,enc_left,generate_node(x[0],types[x[0]],properties[x[0]])))
                    already_described.append(x[0])
            elif x[1]=="STARTED_FOLLOWING":
                if(x[0] in already_described):
                    match_actors_l.append((num_frames,enc_left,generate_node(x[0],"",[])))
                else:
                    match_actors_l.append((num_frames,enc_left,generate_node(x[0],types[x[0]],properties[x[0]])))
                    already_described.append(x[0])
                if(x[2] in already_described):
                    match_actors_l.append((num_frames,enc_right,generate_node(x[2],"",[])))
                else:
                    match_actors_l.append((num_frames,enc_right,generate_node(x[2],types[x[2]],properties[x[2]])))
                    already_described.append(x[2])

        elif(x[3]=="" and x[4]==""):
            if(x[0] in already_described):
                left = generate_node(x[0],"",[])
            else:
                left = generate_node(x[0],types[x[0]],properties[x[0]])
                already_described.append(x[0])
            if(x[2] in already_described):
                right = generate_node(x[2],"",[])
            else:
                right = generate_node(x[2],types[x[2]],properties[x[2]])
                already_described.append(x[2])
            match_actors_l2.append((left,x[1],right))

    tweet_properties = []
    pprint(properties)
    for a in actors:
        if(a[1]=="TWEET"):
            for att in properties[a[0]]:
                if(att[0]=="hashtag"):
                    if(a[0] in already_described):
                        right = generate_node(a[0],"",[])
                    else:
                        right = generate_node(a[0],types[a[0]],[])
                        already_described.append(a[0])
                    tweet_properties.append((right,"HAS_HASHTAG",":HASHTAG {text:'"+att[1]+"'}"))
                elif(att[0]=="url"):
                    if(a[0] in already_described):
                        right = generate_node(a[0],"",[])
                    else:
                        right = generate_node(a[0],types[a[0]],[])
                        already_described.append(a[0])
                    tweet_properties.append((right,"HAS_URL",":URL {expanded_url:'"+att[1]+"'}"))
                elif(att[0]=="retweet_of"):
                    tweet_properties.append((a[0],"RETWEET_OF",att[1]))
                elif(att[0]=="reply_of"):
                    tweet_properties.append((a[0],"REPLY_OF",att[1]))
                elif(att[0]=="quoted"):
                    tweet_properties.append((a[0],"QUOTED",att[1]))
                elif(att[0]=="has_mention"):
                    tweet_properties.append((a[0],"HAS_MENTION",att[1]))

    ##get the individual codes
    pprint(frame_times)
    pprint(match_events_l)
    pprint(match_actors_l)
    pprint(match_actors_l2)
    pprint(tweet_properties)

    mf = jj.Template(match_frames)
    mf_c = mf.render(num_frames=[i+1 for i in range(num_frames)])

    wf = jj.Template(where_frames)
    wf_c = wf.render(frame_times=frame_times)

    me = jj.Template(match_events)
    me_c = me.render(events=match_events_l)

    ma = jj.Template(match_actors)
    ma_c = ma.render(events=match_actors_l)

    ma2 = jj.Template(match_actors2)
    ma_c2 = ma2.render(rels=match_actors_l2)

    ta = jj.Template(tweet_attribs)
    ta_c = ta.render(tweet_properties=tweet_properties)

    # r = jj.Template(returns)
    # r_c = r.render(prop_vars=return_values)
    ##final cleaning
    mf_c = mf_c.strip().strip(",")
    wf_c = wf_c.strip().strip("AND")
    me_c = me_c.strip().strip(",")
    ma_c = ma_c.replace("  {}","").replace(" {}","").strip().strip(",")
    ma_c2 = ma_c2.replace("  {}","").replace(" {}","").strip().strip(",")
    ta_c = ta_c.replace("  {}","").replace(" {}","").strip().strip(",")
    # r_c = r_c.strip().strip(",")

    print(mf_c)
    print(wf_c)
    print(me_c)
    print(ma_c)
    print(ma_c2)
    print(ta_c)

    ##get the complete query
    match1 = "MATCH "+mf_c
    where1 = wf_c
    match2 = "MATCH "+", ".join([x for x in [me_c,ma_c,ma_c2,ta_c] if x.strip()!=""])
    # ret = r_c
    tot_template = jj.Template(total_query)
    parts = []
    if(match1.strip()!="MATCH"):
        parts.append(match1)
    if(where1.strip()!="WHERE"):
        parts.append(where1)
    parts.append(match2)
    # parts.append(ret)
    parts.append("RETURN "+return_values)
    tot_code = tot_template.render(parts=parts)
    print(tot_code)
    return tot_code

# actors=[("u1","USER"),("u2","USER"),("x","USER"),("t","TWEET")]
# attributes=[[("id","101311381")],[("id","44196397")],[],[("hashtag","BLEEDBLUE")]]
# relations=[("x","FOLLOWS","u1",None,None),("x","STARTED_FOLLOWING","u2","10","12"),("u2","TWEETED","t","15","17")]

# actors = [('x', 'USER'), ('u1', 'USER')] + [('t1', 'TWEET'), ('t2', 'TWEET')]
# attributes = [[('id', '12')], [('id', '24')]]+[[('hashtag', 'BLUERISING')], [('retweet_of', 't1'), ('has_mention', 'u1')]]
# relations = [('x', 'FOLLOWS', 'u1', '', ''), ('x', 'TWEETED', 't2', '24', '48')]
# return_values=[("count","x")]
# create_query(actors,attributes,relations,return_values)
