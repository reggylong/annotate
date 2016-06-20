
import os
import argparse
import random




parser = argparse.ArgumentParser()
parser.add_argument('--input', default="output.xml")
parser.add_argument('--output_dir', default="output")
parser.add_argument('--ids', default= "1 2 3 4 5 6 7 8 9 10")
args = parser.parse_args()

if not os.path.exists(args.output_dir):
  os.mkdir(args.output_dir)

f = open(args.input, 'r')
os.chdir(os.getcwd() + "/" + args.output_dir)
ids = [int(i) for i in args.ids.split()]
ids.sort()


f.close()
