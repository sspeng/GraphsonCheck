# how to use

``` bash
#split the input graphson file into 10 parts
python text_file_mod_line_splitter.py -p tiny_amazon -s .gson -n 10

#parallelly parse the inE and outE, check if them matches. the result will be written as ioes_diff_0.json, ioes_misin_0.json, and ioes_misout_0.json
#if the all of the graphson file's inE and outE match in pair, the content of 3 json file will be: {"cnt": 0, "list": []}
mpirun -n 10 python fine_ioes_mapreduce.py -p mod_split_10_tiny_amazon_ -s .gson
```