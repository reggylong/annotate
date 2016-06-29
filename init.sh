#!/bin/bash

:'
mkdir classes &&
mkdir resources &&
wget http://nlp.stanford.edu/software/stanford-corenlp-full-2015-12-09.zip &&
unzip stanford-corenlp-full-2015-12-09.zip &&
rm -rf stanford-corenlp-full-2015-12-09.zip

for file in stanford-corenlp-full-2015-12-09/*.jar; do
  mv $file resources
done

rm -rf stanford-corenlp-full-2015-12-09 &&

cd resources &&
for file in *.jar; do
  jar xf $file
done
wget http://nlp.stanford.edu/software/stanford-srparser-2014-10-23-models.jar
cd .. 

mkdir finished

# website seems to be down
wget http://www.cs.washington.edu/ai/clzhang/release.tar.gz &&
tar zxvf release.tar.gz


wget http://reverb.cs.washington.edu/reverb-latest.jar && mkdir Reverb && mv reverb-latest.jar Reverb && jar xf Reverb/reverb-latest.jar

'
