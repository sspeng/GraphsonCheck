# -*- coding: utf-8 -*-

import argparse
import os
import time
import multiprocessing
import subprocess
import sys
import json
from mpi4py import MPI

comm = MPI.COMM_WORLD

my_rank = comm.Get_rank()
comm_sz = comm.Get_size()

parser = argparse.ArgumentParser()

parser.add_argument('-p', '-prefix', '--prefix', help='The prefix of the input', required=True)
parser.add_argument('-s', '-suffix', '--suffix', help='The suffix of the input', required=True)

args = vars(parser.parse_args())

if(comm_sz == 1):
    print('should run in parallel with mpi')
    exit()

if __name__ == "__main__":
    #read in all the paper data

    input_fn = args['prefix'] + str(my_rank) + args['suffix']

    output_map_files = []
    output_map_ine_dics = []
    output_map_oute_dics = []

    input_reduce_fns = []
    for i in range(comm_sz):
        fn = 'es' + "_from_" + str(my_rank) + "_to_" + str(i) + args['suffix']
        f = open(fn, 'w')
        output_map_files.append(f)

        fn = 'es' + "_from_" + str(i) + "_to_" + str(my_rank) + args['suffix']
        input_reduce_fns.append(fn)

        output_map_ine_dics.append({})
        output_map_oute_dics.append({})

    #map
    with open(input_fn, "r") as in_f:

        while True:
            ln = in_f.readline()

            if(len(ln) <= 1):
                break

            pln = json.loads(ln)

            if('outE' in pln):
                for key in pln['outE']:
                    oute_list = pln['outE'][key]
                    for ele in oute_list:

                        list_to_append    = [0] * 3
                        list_to_append[0] = key
                        list_to_append[1] = pln['id']
                        list_to_append[2] = ele['inV']

                        if(not ele['id'] in output_map_oute_dics):
                            map_proc = ele['id'] % comm_sz
                            output_map_oute_dics[map_proc][ele['id']] = list_to_append
                        else:
                            printf('oute gg from ', my_rank, ', id = ', ele['id'])
                            sys.stdout.flush()
                            exit(3)


            if('inE' in pln):
                for key in pln['inE']:
                    ine_list = pln['inE'][key]
                    for ele in ine_list:

                        list_to_append    = [0] * 3
                        list_to_append[0] = key
                        list_to_append[1] = ele['outV']
                        list_to_append[2] = pln['id']

                        if(not ele['id'] in output_map_ine_dics):
                            map_proc = ele['id'] % comm_sz
                            output_map_ine_dics[map_proc][ele['id']] = list_to_append
                        else:
                            printf('ine gg from ', my_rank, ', id = ', ele['id'])
                            sys.stdout.flush()
                            exit(3)


    for i in range(comm_sz):
        output_map_files[i].write(json.dumps(output_map_oute_dics[i], separators=(',', ':')) + '\n')
        output_map_files[i].write(json.dumps(output_map_ine_dics[i], separators=(',', ':')) + '\n')
        output_map_files[i].close()

        output_map_oute_dics[i] = 'doge'
        output_map_ine_dics[i] = 'doge'

    comm.Barrier()
    if(my_rank == 0):
        print('map step finished.')
        sys.stdout.flush()
    time.sleep(1)
    comm.Barrier()

    #reduce
    reduce_dic = {} #obj is list
    reduce_list = []

    reduce_ine_dic = {}
    reduce_oute_dic = {}

    for i in range(comm_sz):
        with open(input_reduce_fns[i], 'r') as in_f:
            lns = in_f.readlines()
            in_f.close()
            os.system('rm ' + input_reduce_fns[i]) #无情

            oute_pln = json.loads(lns[0])
            ine_pln = json.loads(lns[1])

            for key in oute_pln:
                ikey = int(key)
                reduce_oute_dic[ikey] = oute_pln[key]

            for key in ine_pln:
                ikey = int(key)
                reduce_ine_dic[ikey] = ine_pln[key]

    # reduce_list.sort()

    misin_dic = {}
    misout_dic = {}
    diff_dic = {}


    misin_dic['cnt'] = 0
    misout_dic['cnt'] = 0
    diff_dic['cnt'] = 0

    misin_dic['list'] = []
    misout_dic['list'] = []
    diff_dic['list'] = []

    for key in reduce_ine_dic:
        if(key in reduce_oute_dic):
            #check val
            list_in = reduce_ine_dic[key]
            list_out = reduce_oute_dic[key]

            for i in range(3):
                if(not list_in[i] == list_out[i]):
                    print('edge id =', key, ', list_in:', list_in, 'list_out:', list_out)
                    diff_dic['cnt'] = diff_dic['cnt'] + 1

                    dic_to_append = {}
                    dic_to_append['id'] = key
                    dic_to_append['list_in'] = list_in#label, from, to
                    dic_to_append['list_out'] = list_out#label, from, to

                    diff_dic['list'].append(dic_to_append)

                    break
        else:
            print('mismatch! id =', key, ', dic_in =', reduce_ine_dic[key])

            misin_dic['cnt'] = misin_dic['cnt'] + 1
            dic_to_append = {}
            dic_to_append['id'] = key
            dic_to_append['list_in'] = reduce_ine_dic[key]

            misin_dic['list'].append(dic_to_append)



    for key in reduce_oute_dic:
        if(not key in reduce_ine_dic):
            print('mismatch! id =', key, ', dic_out =', reduce_oute_dic[key])
            misout_dic['cnt'] = misout_dic['cnt'] + 1

            dic_to_append = {}
            dic_to_append['id'] = key
            dic_to_append['list_out'] = reduce_oute_dic[key]

            misout_dic['list'].append(dic_to_append)

    diff_name = 'tmp_diff_' + str(my_rank) + '.json'
    misout_name = 'tmp_misout_' + str(my_rank) + '.json'
    misin_name = 'tmp_misin_' + str(my_rank) + '.json'

    f=open(diff_name, 'w')
    f.write(json.dumps(diff_dic))
    f.close()

    f=open(misout_name, 'w')
    f.write(json.dumps(misout_dic))
    f.close()

    f=open(misin_name, 'w')
    f.write(json.dumps(misin_dic))
    f.close()

    comm.Barrier()
    if(my_rank == 0):
        print('reduce main step finished.')
        sys.stdout.flush()
    time.sleep(1)
    comm.Barrier()

    #reduce final

    #awesome, wasteing time.
    os.system('rm ' + diff_name)
    os.system('rm ' + misout_name)
    os.system('rm ' + misin_name)

    if(my_rank == 0):
        for i in range(1, comm_sz):
            diff_name = 'tmp_diff_' + str(i) + '.json'
            misout_name = 'tmp_misout_' + str(i) + '.json'
            misin_name = 'tmp_misin_' + str(i) + '.json'

            f=open(diff_name, 'r')
            tmp_diff = json.load(f)
            f.close()

            f=open(misout_name, 'r')
            tmp_misout = json.load(f)
            f.close()

            f=open(misin_name, 'r')
            tmp_misin = json.load(f)
            f.close()

            os.system('rm ' + diff_name)
            os.system('rm ' + misout_name)
            os.system('rm ' + misin_name)

            misin_dic['cnt'] = misin_dic['cnt'] + tmp_misin['cnt']
            misout_dic['cnt'] = misout_dic['cnt'] + tmp_misout['cnt']
            diff_dic['cnt'] = diff_dic['cnt'] + tmp_diff['cnt']

            for ele in tmp_misin['list']:
                misin_dic['list'].append(ele)

            for ele in tmp_misout['list']:
                misout_dic['list'].append(ele)

            for ele in tmp_diff['list']:
                diff_dic['list'].append(ele)

        diff_name = 'ioes_diff_' + str(my_rank) + '.json'
        misout_name = 'ioes_misout_' + str(my_rank) + '.json'
        misin_name = 'ioes_misin_' + str(my_rank) + '.json'


        f=open(diff_name, 'w')
        f.write(json.dumps(diff_dic))
        f.close()

        f=open(misout_name, 'w')
        f.write(json.dumps(misout_dic))
        f.close()

        f=open(misin_name, 'w')
        f.write(json.dumps(misin_dic))
        f.close()

