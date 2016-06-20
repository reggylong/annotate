
import os
import argparse
import random




parser = argparse.ArgumentParser()
parser.add_argument('--input', default="annotations.xml")
parser.add_argument('--output_dir', default="output")
parser.add_argument('--n', default=10)
args = parser.parse_args()

if not os.path.exists(args.output_dir):
  os.mkdir(args.output_dir)

f = open(args.input, 'r')
os.chdir(os.getcwd() + "/" + args.output_dir)
i = 0
for i in xrange(int(args.n)):
  id = int(f.readline().strip())
  g = open(str(id) + ".xml", 'w')
  while True:
    line = f.readline()
    if len(line.strip()) == 0: break
    g.write(line)
  g.close()

f.close()
