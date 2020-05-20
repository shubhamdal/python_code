import sys
import re
from collections import OrderedDict
import shutil



def replace_case_insensitive(old, new, myfile):
        return re.sub(re.escape(old), new, myfile, flags=re.IGNORECASE)


def read_string_file(string_file):
    string_od = OrderedDict()
    with open(string_file, 'r') as myfile:
        for line in myfile:
            a=re.split(r'=', str(line.rstrip('\n')))
            string_od[str(a[0])] = str(a[1]);
    return string_od


def read_target_file(target_file):
    target_file_list = list()
    with open(target_file, 'r') as myfile:
        for line in myfile:
            target_file_list.append(line.rstrip('\n'))
    return target_file_list

def replace_string(string_dict,eachFile):
    print("Starting_File: "+eachFile)
    myfile=""
    with open(eachFile, 'r') as f:
        myfile = f.read()
        for key,value in string_dict.items():
            myfile = replace_case_insensitive(str(key),str(value),myfile)
    shutil.copy2(eachFile,"./bkup/"+eachFile)
    with open(eachFile, 'w') as f:
        f.write(myfile)
    f.close()

def main(string_file,target_file):
    string_dict=read_string_file(string_file)
    target_file_list=read_target_file(target_file)
    for eachFile in target_file_list:
        replace_string(string_dict,eachFile)

def find_line_with_matching_string(strng,target_file):
    target_file_list=read_target_file(target_file)
    for eachFile in target_file_list:
        with open(eachFile, 'r') as f:
            myfile = f.readlines()
        for line in myfile:
            if strng in line and '_2500' not in line:
                print(line)
   

if __name__ == '__main__':
    string_file=sys.argv[1]
    target_file=sys.argv[2]
    run_type=sys.argv[3]
    strng=sys.argv[4]
    if run_type==str(1):
        main(string_file,target_file)
    else:
        find_line_with_matching_string(strng,target_file)


# string file format
# stringa=stringb

#target file will consist of names of all the files in which the above string is to be replaced
