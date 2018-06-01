from __future__ import print_function
from pprint import *
import jinja2 as jj
from neo4j.v1 import GraphDatabase, basic_auth
from datetime import datetime
import json
import os
import time

###########################################################################################################

order = ["STARTED_FOLLOWING","TWEETED","FOLLOWS"]
match_frames = """{% for i in num_frames %} (run:RUN) -[:HAS_FRAME]-> (frame{{i}}:FRAME),{% endfor %}"""
where_frames = """WHERE {% for ft in frame_times %} frame{{ft[0]}}.end_t >= {{ft[1]}} AND frame{{ft[0]}}.start_t <= {{ft[2]}} AND{% endfor %}"""
match_events = """{% for e in events %}(frame{{e[0]}}) -[:{{e[1]}}]-> (event{{e[0]}} :{{e[2]}}), {% endfor %}"""
match_actors = """{% for e in events %}(event{{e[0]}}) -[:{{e[1]}}]-> ({{e[2]}}), {% endfor %}"""
match_actors2 = """{% for r in rels %}({{r[0]}}) -[:{{r[1]}}]-> ({{r[2]}}), {% endfor %}"""
tweet_attribs = """{% for tp in tweet_properties %}({{tp[0]}}) -[:{{tp[1]}}]-> ({{tp[2]}}), {% endfor %}"""
match_actors_solo = """{% for a in actors %}({{a}}), {% endfor %}"""
returns  = """RETURN {{ret_values}}"""
unwinds = """{%  for uw in unwinds %}UNWIND {% raw %}{{% endraw %}{{uw}}{% raw %}}{% endraw %} AS {{uw}}_value
{% endfor %}"""
total_query = """
{% for p in parts %}{{p}}
{% endfor %}
"""

encode = {"TWEETED":("HAS_TWEET","TWEET_EVENT"),"STARTED_FOLLOWING":("HAS_FOLLOW","FOLLOW_EVENT")}
encode_left = {"TWEETED":"TE_USER","STARTED_FOLLOWING":"FE_FOLLOWER"}
encode_right = {"TWEETED":"TE_TWEET","STARTED_FOLLOWING":"FE_FOLLOWED"}

class CreateQuery:
    def __init__(self):
        self.depth = lambda L: isinstance(L, list) and max(map(self.depth, L))+1
        self.types = {}
        self.properties = {}
        self.already_described = []

    def generate_node(self,var,type,props):
        pt = "{{var}} {{type}} {% raw %}{{% endraw %}{% for av in props %}{{av[0]}}:{{av[1]}}, {% endfor %}"
        t = jj.Template(pt)
        c = t.render(props=props,var=var,type=type)
        c = c.strip().strip(",")
        ## the replace are required to remove these {} braces. though, syntactically correct in cypher but not so pleasant to read in the generated query.
        return ((c+"}").replace(" {}","").replace("  {}","").replace("{}","")).replace(" )",")")

    def conditional_create(self,entity):
            print(">>>> ",entity,self.already_described)
            if(entity in self.already_described):
                return self.generate_node(entity,"",[])
            else:
                if(self.types[entity]==":TWEET"):
                    node = self.generate_node(entity,self.types[entity],[])
                elif(self.types[entity]==":USER"):
                    node = self.generate_node(entity,self.types[entity],self.properties[entity])
                self.already_described.append(entity)
                print(node)
                return node


    def create_query(self,actors,attributes,relations, return_values):
        print("We are generating the query ")
        print("--------------------------------")
        print(actors, attributes, relations)
        print("--------------------------------")
        relations.sort(key=lambda x:order.index(x[1])) ##required because we write the ma_c first and then ma_c2.
        self.types = {}
        self.properties = {}
        unwind_list = []
        for x in actors:
            self.types[x[0]] = ":"+x[1]
        for i in range(len(actors)):
            entity_name = actors[i][0]
            self.properties[entity_name] = []
            for prop in attributes[i]:
                prop_value = prop[1]
                if(prop_value[0]=="{" and prop_value[-1]=="}"):
                    unwind_list.append(prop_value[1:-1])
                    self.properties[entity_name].append((prop[0],prop_value[1:-1]+"_value"))
                else:
                    self.properties[entity_name].append(prop)
        pprint(self.types)
        pprint(self.properties)
        self.already_described = [] ##list of already specified users, if encountered again, just use the variable name and don't specify again

        frame_times = []
        match_events_l = []
        match_actors_l = []
        match_actors_l2 = []
        num_frames = 0
        for x in relations: ## a relation is(actor1,rel,actor2,time1,time2)
            if(x[3]!="" and x[4]!=""):
                num_frames+=1
                frame_times.append((num_frames,x[3],x[4]))
                e_enc = encode[x[1]]
                match_events_l.append((num_frames,e_enc[0],e_enc[1]))
                enc_left = encode_left[x[1]]
                enc_right = encode_right[x[1]]
                match_actors_l.append( (num_frames,enc_left,self.conditional_create(x[0])) )
                match_actors_l.append( (num_frames,enc_right,self.conditional_create(x[2])) )


            elif(x[3]=="" and x[4]==""):
                match_actors_l2.append( (self.conditional_create(x[0]),x[1],self.conditional_create(x[2])) )

        tweet_properties = []
        for a in actors:
            if(a[1]=="TWEET"):
                for att in self.properties[a[0]]:
                    if(att[0]=="hashtag"):
                        if (att[1] in [x+"_value" for x in unwind_list]):
                            tweet_properties.append((self.conditional_create(a[0]),"HAS_HASHTAG",":HASHTAG {text:"+att[1]+"}"))
                        else:
                            tweet_properties.append((self.conditional_create(a[0]),"HAS_HASHTAG",":HASHTAG {text:'"+att[1]+"'}"))
                    elif(att[0]=="url"):
                        if (att[1] in [x+"_value" for x in unwind_list]):
                            tweet_properties.append((self.conditional_create(a[0]),"HAS_URL",":URL {expanded_url:"+att[1]+"}"))
                        else:
                            tweet_properties.append((self.conditional_create(a[0]),"HAS_URL",":URL {expanded_url:'"+att[1]+"'}"))
                    elif(att[0]=="retweet_of"):
                        tweet_properties.append((self.conditional_create(a[0]),"RETWEET_OF",self.conditional_create(att[1])))
                    elif(att[0]=="reply_of"):
                        tweet_properties.append((conditional_create(a[0]),"REPLY_OF",conditional_create(att[1])))
                    elif(att[0]=="quoted"):
                        tweet_properties.append((conditional_create(a[0]),"QUOTED",conditional_create(att[1])))
                    elif(att[0]=="has_mention"):
                        tweet_properties.append((self.conditional_create(a[0]),"HAS_MENTION",self.conditional_create(att[1])))
        match_actors_solo_l = []
        for a in actors:
            if(a[0] not in self.already_described):
                match_actors_solo_l.append(self.conditional_create(a[0]))

        ##get the individual codes
        # print("The different lists are ")
        # print("frame times :")
        # pprint(frame_times)
        # print("match_events_l: ")
        # pprint(match_events_l)
        # print("match_actors_l and l2: ")
        # pprint(match_actors_l)
        # pprint(match_actors_l2)
        # print("tweet_properties: ")
        # pprint(tweet_properties)

        uw = jj.Template(unwinds)
        uw_c = uw.render(unwinds = unwind_list)

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

        mas = jj.Template(match_actors_solo)
        mas_c = mas.render(actors=match_actors_solo_l)

        r = jj.Template(returns)
        r_c = r.render(ret_values=return_values)
        ##final cleaning
        uw_c = uw_c.strip()
        mf_c = mf_c.strip().strip(",")
        wf_c = wf_c.strip().strip("AND")
        me_c = me_c.strip().strip(",")
        ma_c = ma_c.strip().strip(",")
        ma_c2 = ma_c2.strip().strip(",")
        ta_c = ta_c.strip().strip(",")
        mas_c = mas_c.strip().strip(",")
        r_c = r_c.strip().strip(",")

        print(uw_c)
        print(mf_c)
        print(wf_c)
        print(me_c)
        print(ma_c)
        print(ma_c2)
        print(ta_c)
        print(mas_c)

        ##get the complete query
        unwind = uw_c
        match1 = "MATCH "+mf_c
        where1 = wf_c
        match2 = "MATCH "+", ".join([x for x in [me_c,ma_c,ma_c2,ta_c,mas_c] if x.strip()!=""])
        ret = r_c
        tot_template = jj.Template(total_query)
        parts = []
        parts.append(unwind)
        if(match1.strip()!="MATCH"):
            parts.append(match1)
        if(where1.strip()!="WHERE"):
            parts.append(where1)
        parts.append(match2)
        parts.append(ret)
        tot_code = tot_template.render(parts=parts)
        print(tot_code)
        # arguments = {}
        # for x in unwind_list:
        #     arguments[x[0]+"_list"] = x[1]
        print("Query generation ended ")
        print("--------------------------------")
        temp = [x.split(",") for x in return_values.split(", ")]
        return_dict = {"code":tot_code,"inputs":unwind_list,"outputs":[x for subl in temp for x in subl]}
        return(return_dict)

if __name__ == '__main__':

    actors=[("u","USER"),("t","TWEET"),("t1","TWEET")]
    attributes=[[],[("hashtag","{hash}")],[]]
    relations=[("u","TWEETED","t","",""),("u","TWEETED","t1","","")]

    # actors = [('x', 'USER'), ('u1', 'USER')] + [('t1', 'TWEET'), ('t2', 'TWEET')]
    # attributes = [[('id', '12')], [('id', '24')]]+[[('hashtag', 'BLUERISING')], [('retweet_of', 't1'), ('has_mention', 'u1')]]
    # relations = [('x', 'FOLLOWS', 'u1', '', ''), ('x', 'TWEETED', 't2', '24', '48')]
    cq = CreateQuery();
    return_values="u.id,count(t1)"
    ret_dict = cq.create_query(actors,attributes,relations,return_values)
    pprint(ret_dict,width=150)


    ##TODO
    # compostion of realationships or attribute properties like all tweets(t) which are retweets of t1 or quoted t2

"""
1) Alerts using the neo4j API - not much to do.
2) Customizability of post processing functions. - discuss.
3) Merge the neo4j web interface to the dashboard.
"""