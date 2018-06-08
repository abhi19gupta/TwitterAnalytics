from __future__ import unicode_literals, print_function

import plac
from pathlib import Path
import random
import spacy
from spacy.tokens import Doc
from spacy.gold import GoldParse
# training data
TRAIN_DATA = [
	(['Who','is', 'Shaka','Khan','?'],["O","O","B-PER","I-PER","O"]),
]

def main(model=None, output_dir=None, n_iter=100):
	if model is not None:
		nlp = spacy.load(model)  # load existing spaCy model
		print("Loaded model '%s'" % model)
	else:
		nlp = spacy.blank('en')  # create blank Language class
		print("Created blank 'en' model")

    #add to vocab
	# for tokens, _ in TRAIN_DATA:
	# 	for word in tokens:
	# 		_ = nlp.vocab[word]

	if 'ner' not in nlp.pipe_names:
		ner = nlp.create_pipe('ner')
		nlp.add_pipe(ner, last=True)
	else:
		ner = nlp.get_pipe('ner')

	# add labels
	for _, annotations in TRAIN_DATA:
		for ent in annotations:
			ner.add_label(ent)

	# get names of other pipes to disable them during training
	other_pipes = [pipe for pipe in nlp.pipe_names if pipe != 'ner']
	with nlp.disable_pipes(*other_pipes):  # only train NER
		optimizer = nlp.begin_training()
		for itn in range(n_iter):
			random.shuffle(TRAIN_DATA)
			losses = {}
			for tokens, entities in TRAIN_DATA:
				doc = Doc(nlp.vocab,words = tokens)
				gold = GoldParse(doc, entities=entities)
				nlp.update([doc], [gold], drop=0.5, sgd=optimizer,losses=losses)
		print(losses)

	# test the trained model
	# for text, _ in TRAIN_DATA:
	#	 doc = nlp(text)
	#	 print('Entities', [(ent.text, ent.label_) for ent in doc.ents])
	#	 print('Tokens', [(t.text, t.ent_type_, t.ent_iob) for t in doc])

	# # save model to output directory
	# if output_dir is not None:
	#	 output_dir = Path(output_dir)
	#	 if not output_dir.exists():
	#		 output_dir.mkdir()
	#	 nlp.to_disk(output_dir)
	#	 print("Saved model to", output_dir)

	#	 # test the saved model
	#	 print("Loading from", output_dir)
	#	 nlp2 = spacy.load(output_dir)
	#	 for text, _ in TRAIN_DATA:
	#		 doc = nlp2(text)
	#		 print('Entities', [(ent.text, ent.label_) for ent in doc.ents])
	#		 print('Tokens', [(t.text, t.ent_type_, t.ent_iob) for t in doc])


# if __name__ == '__main__':
#	 plac.call(main)
main()
	# Expected output:
	# Entities [('Shaka Khan', 'PERSON')]
	# Tokens [('Who', '', 2), ('is', '', 2), ('Shaka', 'PERSON', 3),
	# ('Khan', 'PERSON', 1), ('?', '', 2)]
	# Entities [('London', 'LOC'), ('Berlin', 'LOC')]
	# Tokens [('I', '', 2), ('like', '', 2), ('London', 'LOC', 3),
# ('and', '', 2), ('Berlin', 'LOC', 3), ('.', '', 2)]