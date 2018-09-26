# -*- coding: utf-8 -*-

import argparse
import os
import time
import sys
import json

parser = argparse.ArgumentParser()

parser.add_argument('-p', '-prefix', '--prefix', help='The prefix of the input', required=True)
parser.add_argument('-s', '-suffix', '--suffix', help='The suffix of the input', required=True)
parser.add_argument('-n', '-number', '--number', help='The number of splitted files', required=True)

args = vars(parser.parse_args())

#run on nfs!

if __name__ == "__main__":
    input_fn = args['prefix'] + args['suffix']

    n = int(args['number'])
    out_fs = []
    for i in range(n):
        write_fn = 'mod_split_' + str(n) + '_' + args['prefix'] + "_" + str(i) + args['suffix']
        out_fs.append(open(write_fn, 'w'))


    with open(input_fn, "r") as in_f:

        write_f = 0

        while True:
            ln = in_f.readline()

            out_fs[write_f].write(ln)

            if(len(ln) <= 1):
                break
            write_f = (write_f + 1) % n

        in_f.close()
        for i in range(n):
            out_fs[i].close()