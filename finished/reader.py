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

def write_output(info, data):
  g = open('output/' + info['id'] + ".html", 'w')
  
  g.write("<h1>" + info['time'] + "</h1>");

  g.close()


f = open("test", "r")
n = 1

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
