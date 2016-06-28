import json

def recover_sentence(tokens):
  sentence = ""
  for word_obj in tokens:
    sentence += word_obj['before'] + word_obj['originalText'] + word_obj['after']
  return sentence

def recover_sentences(sentences):
  all_sentences = []
  for sentence_obj in sentences:
    all_sentences.append(recover_sentence(sentence_obj['tokens']))
  return all_sentences

def recover_coref(corefs):
  all_corefs = {}
  for entity in corefs:
    if entity not in all_corefs:
      all_corefs[entity] = []
    for matching_obj in corefs[entity]:
      all_corefs[entity].append("Sentence: " + str(matching_obj['sentNum']).encode('utf-8') + " - Text : " + matching_obj['text'])
  return all_corefs

def write_output(info, data):
  g = open('output/' + info['id'] + ".html", 'w')
  
  g.write("<h1>" + info['time'] + "</h1>\n");
  sentences = recover_sentences(data['sentences'])
  for i, x in enumerate(sentences):
    g.write("<p><b>Sentence " + str(i+1) + "</b><p>\n")
    g.write("<p>" + x.encode('utf-8') + "</p>")
  corefs = recover_coref(data['corefs'])
 
  for entity, references in sorted(corefs.iteritems()):
    g.write("<p><b>Entity " + str(entity) + "</b>\n")
    for reference in references:
      g.write(reference.encode('utf-8') + "</p>\n")
  g.close()


f = open("test", "r")
n = 5 

begin = False
end = False
obj = ""
count = 0
data = None

info = {}
while True:
  if count == n:
    break
  line = f.readline()
  if not begin:
    split = line.index(" ")
    info['time'] = line[split:].strip()
    info['id'] = line[:split].strip()
    begin = True
  elif line.rstrip() == '}':
    obj += line 
    end = True
  else:
    obj += line 

  if end:
    end = False
    begin = False
    data = json.loads(obj)
    write_output(info, data)
    count += 1
f.close()
