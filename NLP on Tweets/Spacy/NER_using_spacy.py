from __future__ import print_function
from pprint import *
import tabulate
import random
import time
import re
import codecs
## Spacy imports
import json
import pathlib
import spacy
from spacy.pipeline import EntityRecognizer
from spacy.gold import GoldParse
from spacy.tokens import Doc
# from spacy.tagger import Tagger
## NLTK imports
from nltk.tokenize.moses import MosesDetokenizer

def get_data(file):
	fin = open(file,"r")
	tokens_l = []; tags_l  =[]
	tl = []; tgl = []
	for line in fin:
		if(line.strip()==""):
			tokens_l.append(tl);tags_l.append(tgl)
			tl = [];tgl = []
		else:
			l = [x.strip() for x in line.split()]
			tl.append(l[0]); tgl.append(l[1])
	return tokens_l, tags_l

def train_ner(nlp, train_data, entity_types):
	detokenizer = MosesDetokenizer()
	## Add new words to vocab.
	for tokens, _ in train_data:
		# doc = nlp.make_doc(raw_text)
		for word in tokens:
			_ = nlp.vocab[word]

	# ## Train NER.
	# ner = EntityRecognizer(nlp.vocab, entity_types=entity_types)
	# for itn in range(5):
	# 	random.shuffle(train_data)
	# 	for tokens, entity_offsets in train_data:
	# 		doc = nlp.make_doc(detokenizer.detokenize(tokens, return_str=True))
	# 		gold = GoldParse(doc,entities=entity_offsets)
	# 		ner.update([doc], [gold])
	# return ner
	if 'ner' not in nlp.pipe_names:
		ner = nlp.create_pipe('ner')
		nlp.add_pipe(ner, last=True)
	# otherwise, get it so we can add labels
	else:
		ner = nlp.get_pipe('ner')

	# add labels
	for _, annotations in train_data:
		for ent in annotations:
			ner.add_label(ent)
	other_pipes = [pipe for pipe in nlp.pipe_names if pipe != 'ner']

	optimizer = nlp.begin_training()
	for itn in range(100):
		random.shuffle(train_data)
		for raw_text, entity_offsets in train_data:
			doc = Doc(nlp.vocab,words = raw_text)
			gold = GoldParse(doc, entities=entity_offsets)
			nlp.update([doc], [gold], drop=0.5, sgd=optimizer)

def predict_list_spacy(nlp,data,testing,model_dir=None):
	nlp = spacy.load('en_default', parser=False, entity=False, add_vectors=False)
	if nlp.tagger is None:
		print("---------> Didn't find a model, performance will be not upto mark.")
		nlp.tagger = Tagger(nlp.vocab, features=Tagger.feature_templates)

	print("testing on indices",testing)
	train_d = [data[i] for i in indices if i not in testing]
	test_d = [data[i] for i in testing]
	print("Training and testing on corpus of size",len(train_d),len(test_d)); t1 = time.time()
	ner = train_ner(nlp, train_d, global_labels)
	print("training completed in ",time.time()-t1)

	offer_strings = [x[0] for x in test_d]
	preds = []
	for s in offer_strings:
		p = []
		doc = nlp.make_doc(s)
		nlp.tagger(doc)
		ner(doc)
		for word in doc:
			# print(word.text,word.ent_type_)
			p.append((word.text,word.ent_type_))
		preds.append(p)

	if model_dir is not None:
		save_model(ner, model_dir)
	return(preds)

tokens,tags = get_data("train3.txt")
print(len(tokens),len(tags))
pprint(tokens[:4])
pprint(set([x for ll in tags for x in ll]))
nlp = spacy.load("en")
ner_tagger = train_ner(nlp,list(zip(tokens,tags)),['B-PER', 'I-ORG', 'O', 'I-LOC', 'I-PER', 'B-ORG', 'B-LOC'])