import re

findcreateString='| CREATE EXTERNAL TABLE'
findRowFormatString='| ROW FORMAT SERDE'
listcreatetable=[]
listRowFormat=[]
with open('hive_adhoc_DDLs_output_file.txt','r') as f:#, open('only_hive_tables.txt','w')as writefile:
    content = f.read()
    #print((content))
    #print (content.index('| CREATE EXTERNAL TABLE'))

for index1,value in enumerate(content):
	if content[index1:index1+(len(findcreateString))] == findcreateString:
		listcreatetable.append(index1)
	if content[index1:index1+(len(findRowFormatString))] == findRowFormatString:
		listRowFormat.append(index1)
		
	

print(len(listcreatetable))
print(len(listRowFormat))
	
for i in range(0,353):
	if ('`sgmntn_dim_key' in content[listcreatetable[i]:listRowFormat[i]]):
		print(content[listcreatetable[i]:listRowFormat[i]])
			
			
			
# to be integrated in the above code.
#with open('Hive_adhoc_tables_with_sgmntdimkeyDDLs.txt','r') as myfile:
#    for i in myfile.readlines():
#        if 'CREATE EXTERNAL TABLE ' in i:
#            print(i)
