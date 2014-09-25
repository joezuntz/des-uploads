import progressbar
import sys
import argparse


input_file = sys.argv[1]
output_base = sys.argv[2]
max_lines = int(sys.argv[3])
progress = progressbar.ProgressBar(maxval=25227560)

f = 1
for i,line in progress(enumerate(open(input_file))):
	if i%max_lines==0:
		if i==0:
			header=line
		output = open(output_base+"_%.4d.ssv"%f, 'w')
		f+=1
		output.write(header)
		if i==0: continue #do not write header line twice
	output.write(line)
