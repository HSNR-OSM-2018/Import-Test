#! /usr/bin/env/ python

import sys, io, codecs, re
from neo4j.v1 import GraphDatabase

class LayerStruct(object):
    def __init__(self, tag, id):
		self.tag = tag
		self.id = id

data_path = "/home/route/import/data/elfenhang_all.xml"
encoding = "UTF-8"

p_attributes = re.compile('\s?(?P<attr>\w+=(?:\"|\')[^\"\']+(?:\"|\'))', re.IGNORECASE)
p_tags = re.compile(r'(?:<(?P<tag>\w+)(?P<attrs>(?:\s+id=\"(?P<id>[^\"]*)\")?(?:\s+\w+=\"[^\"]*\")*)?)?(?:<?(?P<end>\/)?\w*>)', re.IGNORECASE)
driver = GraphDatabase.driver("bolt://192.168.72.102:7687", auth=("neo4j", "neo"))

layers = [LayerStruct('', 0)] * 10
layer_current = 0

nodes = []
edges = []
alphabet = "a b c d e f g h i j k l m n o p q r s t u v w x y z".split(' ')

#print len(alphabet)

records_per_transaction = 50

done = 0

def add_friends(tx, name, friend_name):
    tx.run("MERGE (a:Person {name: $name}) "
           "MERGE (a)-[:KNOWS]->(friend:Person {name: $friend_name})",
           name=name, friend_name=friend_name)

def del_all(tx):
    tx.run("MATCH (n) DETACH DELETE n")

def execute_query(tx):
	if done % records_per_transaction == 0:
		#print "\n".join(nodes) + "\n"
		if len(nodes) > 0:
			tx.run("\n".join(nodes))
		if len(edges) > 0:
			#print " WITH 1 as x ".join(edges) + "\n"
			tx.run(" WITH 1 as x ".join(edges))
		#q = "\n".join(nodes) +  "\n" + "\n".join(edges)
		#if len(edges) > 0:
		#q = q + 
		#print q
		#print "\n"
		#tx.run(q)
		del nodes[:]
		del edges[:]
	if done % 1000 == 0:
		print "done: %d" % done

def add_tag(tx, tag, id, layer, attrs):
	#print "done mod rpt = %d" % (done % records_per_transaction)
	#var_name = alphabet[done % records_per_transaction]
	
	if layer > 1:
		#query.append("MERGE (a:%s {%s}) MERGE (parent {id:%s}) CREATE (a)-[:CHILD_OF]->(parent)" % (tag, attrs, layers[layer].id))
		edges.append("MATCH (p:%s {id:'%s'}) WITH p CREATE (:%s{%s})-[:CHILD_OF]->(p)" % (layers[layer - 1].tag, layers[layer - 1].id, tag, attrs))
		#print "MATCH (p {id:'%s'}) WITH p CREATE (:%s{%s})-[:CHILD_OF]->(p)" % (layers[layer - 1].id, tag, attrs)
	else:
		nodes.append("CREATE (:%s {%s})" % (tag, attrs))
		#print "CREATE (:%s {%s})" % (tag, attrs)
	execute_query(tx)
	
if __name__ == "__main__":
	with io.open(data_path, mode='r', buffering=-1, encoding=encoding, errors=None, newline=None, closefd=True) as f:
		with driver.session() as session:
			session.write_transaction(del_all)
			for line in f:
				done = done + 1
				
				if line == "":
					continue
				s = codecs.encode(line, encoding)
			
				tags = p_tags.findall(s)
				#print s
				#print tags
				#print "\n"
				if tags:
					for tag in tags:
						
						# tag[0] => tag, [1] => attrs, [2] => id, [3] => end
						if tag[0] != '':
							attrs = p_attributes.findall(s)
							attrs = ", ".join(attrs).replace('"', '\'').replace('=', ':')

							#print "done: %d,\tlayer: %d,\ttag: %s,\tend: %s" % (done, layer_current, tag[0], tag[3])
							#print s

							if tag[2] != '':
								layers[layer_current].tag = tag[0]
								layers[layer_current].id = tag[2]

							session.write_transaction(add_tag, tag[0], tag[2], layer_current, attrs)

							if tag[3] == '':
								layer_current = layer_current + 1
						else:
							if tag[3] != '':
								#print "decrease layer"
								layer_current = layer_current - 1
				#if stop_in == 0:
				#	break
			session.write_transaction(execute_query)
