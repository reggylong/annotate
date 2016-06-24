
import os
import argparse
import random




parser = argparse.ArgumentParser()
parser.add_argument('--input', default="outputs/0.out")
parser.add_argument('--output_dir', default="xml_output")
parser.add_argument('--n', default=30)
args = parser.parse_args()

if not os.path.exists(args.output_dir):
  os.mkdir(args.output_dir)

f = open(args.input, 'r')
os.chdir(os.getcwd() + "/" + args.output_dir)
i = 0
for i in xrange(int(args.n)):
  g = open(str(i) + ".xml", 'w')
  while True:
    line = f.readline()
    if len(line.strip()) == 0: break
    g.write(line)
  g.close()

f.close()
