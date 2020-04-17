# ##############################################
# This script works like minibase once you update the file name in 
# FILENAME="project2_testdata" or anything it will perform the same operation that Minibase does but operations are performed inmemory using pandas dataframe
# Steps to run
# update the FILENAME variable eg FILENAME="project2_testdata" without .csv part
# run using terminal 
# python testing_automation.py 
# an output file is generated with the details of the execution
# Current limitation only works one type at a time bigtType = [1,2,3,4,5]
# comment un comment any specific part version or query part you want to test
# sample custom filter can be added as below 
#  ,['"*" [Donkey,Fox] [38141,40385]','"Donkey" <= colkey <= "Fox" and 38141 <= value <= 40385']
#
# where position 0 is for the bitT query filter and postion 1 is for what pandas dataframe will do.
  






import os
import subprocess
import numpy as np
import pandas as pd


BUFFER_SIZE=200
FILENAME="project2_testdata"
#FILENAME="batch_insert_5_row_key_col_key"
#FILENAME="batch_insert_Cal_GR_TW"
DBNAME = "abc"
TESTS_OUTPUTFILE = "TestOutput.txt"
TMP_QUERY_FILE= "TempQuery.csv"
VERSIONED_FILE= "sorted_28251_records_query_test.csv"
#outputfile = " > op.txt"

def delete_dbfile():
    dir_name = "/tmp/"
    test = os.listdir(dir_name)
    #print(test)
    for item in test:
        if item.endswith("big_db"):
            os.remove(os.path.join(dir_name, item))
            print("\nBIGDB file exists hence deleting\n")
    #print(os.listdir(dir_name))


def Test_Version(type):
    
    filters = ['"*" "*" "*"']
    delete_dbfile()
    #delete_dbfile()
    os.system("echo ============================================================================= >> " + TESTS_OUTPUTFILE)
    os.system("echo ============================================================================= >> " + TESTS_OUTPUTFILE)
    os.system("echo ============================================================================= >> " + TESTS_OUTPUTFILE)
    batchinsert = "java" + " " + "BatchInsert" + " " + FILENAME + " " + str(type) + " " + DBNAME + " " + str(BUFFER_SIZE)
    outputfile = " > op.txt"
    os.system(batchinsert + outputfile)
    colnames=['rowkey', 'colkey', 'value', 'timestamp'] 
    df = pd.read_csv(FILENAME+".csv",names=colnames,header=None)
    #df=pd.read_csv(VERSIONED_FILE,names=colnames,header=None)
    print("Executing Query")
    #df_ip=df.groupby(['rowkey', 'colkey']).tail(3).sort_values(by=['rowkey','colkey','timestamp'])
    df1=df.groupby(['rowkey', 'colkey']).apply(lambda x: x.sort_values(['timestamp'])).reset_index(drop=True)
    df2=df1.groupby(['rowkey', 'colkey']).tail(3).reset_index(drop=True)
    df2.sort_values(by=['rowkey','colkey','timestamp'], ascending = False).reset_index(drop=True, inplace=True)
    query = "java" + " " + "Query" + " " + DBNAME + " " + str(1) + " " + filters[0] + " " + str(BUFFER_SIZE)
    #print(query)
    os.system(query + outputfile)
    print("Executing Query 1")
    os.system("tail -n +11 op.txt|head -n -5|sed 's/  \+/,/g' > " + TMP_QUERY_FILE)
    if(os.path.exists(TMP_QUERY_FILE)):
        dft= pd.read_csv(TMP_QUERY_FILE,names=colnames,header=None)
        os.remove(TMP_QUERY_FILE)
    dftmp=dft.sort_values(by=['rowkey','colkey','timestamp'], ascending = True)
    dftmp.reset_index(drop=True, inplace=True)
    if(dftmp.equals(df2)):
        print("Matched")
    else:
        print("Not Matched")
        print(df2)
        print(dftmp)
        
    #delete_dbfile()
    
def match_noMatch(df1,df2):
    if (df1.empty and df2.empty):
        return "Matched Both empty result set"
    elif (df1.equals(df2)):
        return "Matched Both has same contents"
    else:
        return "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Not Matched!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        


def delete_allClassFiles():
    os.system("find . -type f -name \"*.class\" -delete")

def CreateDBFileWithType(type):
    os.system("echo ============================================================================= >> " + TESTS_OUTPUTFILE)
    os.system("echo ============================================================================= >> " + TESTS_OUTPUTFILE)
    os.system("echo ============================================================================= >> " + TESTS_OUTPUTFILE)
    #delete_dbfile()
    os.system("javac BatchInsert.java")
    outputfile = " > op.txt"
    batchinsert = "java" + " " + "BatchInsert" + " " + FILENAME + " " + str(type) + " " + DBNAME + " " + str(BUFFER_SIZE)
    os.system(batchinsert + outputfile)
    os.system("echo BATCH_INSERT_FOR_TYPE_" + str(type) + " >> " + TESTS_OUTPUTFILE)
    os.system("echo >> " + TESTS_OUTPUTFILE)
    os.system("echo " + batchinsert + " >> " + TESTS_OUTPUTFILE)
    os.system("tail -5 op.txt >> " + TESTS_OUTPUTFILE)
    os.system("echo ============================================================================= >> " + TESTS_OUTPUTFILE)
    os.system("echo >> "+ TESTS_OUTPUTFILE)

def TestQuery(type):
    delete_dbfile()
    os.system("javac Query.java")
    orderList=[1,2,3,4,6]
    #orderList=[1,2]
    colnames=['rowkey', 'colkey', 'value', 'timestamp'] 
    orderListTest=[['rowkey','colkey','timestamp'],['colkey','rowkey','timestamp'],['rowkey','timestamp'],['colkey','timestamp'],['timestamp']]
    batchinsert = "java" + " " + "BatchInsert" + " " + FILENAME + " " + str(type) + " " + DBNAME + " " + str(BUFFER_SIZE)
    outputfile = " > op.txt"
    print("\nPerforming Batch insert\n")
    os.system(batchinsert + outputfile)
    print("\nPerforming loading in Pandas dataframe \n")
    ######rading the same file in pandas and all the sort logic will be applied
    colnames=['rowkey', 'colkey', 'value', 'timestamp'] 
    df0 = pd.read_csv(FILENAME+".csv",names=colnames,header=None)
    #df=pd.read_csv(VERSIONED_FILE,names=colnames,header=None)
    print("Executing Query")
    #df_ip=df.groupby(['rowkey', 'colkey']).tail(3).sort_values(by=['rowkey','colkey','timestamp'])
    df1=df0.groupby(['rowkey', 'colkey']).apply(lambda x: x.sort_values(['timestamp'])).reset_index(drop=True)
    df=df1.groupby(['rowkey', 'colkey']).tail(3).reset_index(drop=True)
    df.sort_values(by=['rowkey','colkey','timestamp'], ascending = False).reset_index(drop=True, inplace=True)
    
    #df = pd.read_csv(VERSIONED_FILE,names=colnames,header=None)
    
    #'"*" "*" "*"'
#    filters = ['[California,Greece] [Donkey,Fox] "*"', '[California,Greece] Fox "*"' , '[California,Greece] [Donkey,Fox] "*"','[California,Greece] [Donkey,Fox] [38141,40385]','[California,Greece] "*" [38141,40385]','[California,Greece] Coyote "*"','"*" Coyote [38141,40385]','California "*" [38141,40385]','"*" [Donkey,Fox] [38141,40385]','Greece Donkey 18300','Greece "*" 18300','Greece "*" "*"','"*" "*" 18300' ]
#    filters = ['[California,Greece] [Donkey,Fox] "*"','[California,Greece] "*"	"*"','"*" [Donkey,Fox] "*"','Greece Donkey 18300','Greece Donkey "*"','Greece "*" "*"','"*" Donkey "*"','"*" "*" 18300']
    filters = [['Greece Donkey 18300','rowkey == "Greece" and colkey == "Donkey" and value == 18300']
                ,['[California,Greece] [Donkey,Fox] "*"', '"California" <= rowkey <= "Greece" and "Donkey" <= colkey <= "Fox"']
                ,['[California,Greece] [Donkey,Fox] [38141,40385]', '"California" <= rowkey <= "Greece" and "Donkey" <= colkey <= "Fox" and 38141 <= value <= 40385']
                ,['"*" "*" [38141,40385]', '38141 <= value <= 40385']
                ,['"*" [Donkey,Fox] [38141,40385]','"Donkey" <= colkey <= "Fox" and 38141 <= value <= 40385']
                ,['"*" "*" "*"','rowkey == rowkey and colkey == colkey and value == value']
                ]
    
    # for filter in filters:
    #     print(filter)

    for order in orderList:
        print("\nPerforming filter and sort tests \n")
        os.system("echo ORDER_TYPE_" + str(order) + " ----------------------------------------- >> " + TESTS_OUTPUTFILE)
        #for filter in filters:
        for i in range(len(filters)):
             #tmpfilter = filters[0][0]
             query = "java" + " " + "Query" + " " + DBNAME + " " + str(order) + " " + filters[i][0] + " " + str(BUFFER_SIZE)
             outputfile = " > op.txt"
             os.system(query + outputfile)
             process = subprocess.Popen(['grep','-i','"Exception"', 'op.txt'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
             stdout, stderr = process.communicate()
             #print("OUTPUT " + stdout)
             #filter_str=filter_test[0]
             if(order == 1):
                dft=df.query(filters[i][1]).sort_values(by=orderListTest[0])
                dft.reset_index(drop=True, inplace=True)
                #print(dft)
             elif(order == 2):
                dft=df.query(filters[i][1]).sort_values(by=orderListTest[1])
                dft.reset_index(drop=True, inplace=True)
             elif(order == 3):
                dft=df.query(filters[i][1]).sort_values(by=orderListTest[2])
                dft.reset_index(drop=True, inplace=True)
             elif(order == 4):
                dft=df.query(filters[i][1]).sort_values(by=orderListTest[3])
                dft.reset_index(drop=True, inplace=True) 
             elif(order==6):
                dft=df.query(filters[i][1]).sort_values(by=orderListTest[4])
                dft.reset_index(drop=True, inplace=True)                 
             if(stdout):
                print(stdout)
                os.system("echo EXCEPTION OCCURED " + stdout)
             if (stderr):
                print(stderr)
             os.system("echo " + query + " >> " + TESTS_OUTPUTFILE)
             os.system("tail -5 op.txt >> " + TESTS_OUTPUTFILE)
             os.system("tail -n +9 op.txt|head -n -4|sed 's/  \+/,/g' >> " + TMP_QUERY_FILE)
             if(os.path.exists(TMP_QUERY_FILE)):
                colnames=['rowkey', 'colkey', 'value', 'timestamp']
                dftmp= pd.read_csv(TMP_QUERY_FILE,names=colnames,header=None)
             #print(dftmp)
             #print(dft)
             
             print(query)
             results = match_noMatch(dftmp,dft)
             os.system("echo " + str(results) + " >> " + TESTS_OUTPUTFILE)
             print("Result   ",results)
             
             # if(dftmp.equals(dft)):
                 # print(query)
                 # print("Matched")
                 
                 # os.system("echo " + "Matched" + " >> " + TESTS_OUTPUTFILE)
             # else:
                 # print("Not Matched")
                 # print("\nOutput from Query\n")
                 # print(dftmp)
                 # print("\n Output from Testing n")
                 # print(dft)
                 # os.system("echo " + "Not Matched" + " >> " + TESTS_OUTPUTFILE)
                # #os.system("echo >> "+ TESTS_OUTPUTFILE)
             os.remove(TMP_QUERY_FILE)
             os.remove("op.txt")
                
             
             
             os.system("echo >> "+ TESTS_OUTPUTFILE)
             





if __name__=="__main__":
    delete_dbfile()
    #delete_allClassFiles()
    #delete_dbfile()
    if(os.path.exists(TESTS_OUTPUTFILE)):
        os.remove(TESTS_OUTPUTFILE)
    

    bigtType = [1,2,3,4,5]


    for type in bigtType:
        print("TYPE" , type)
        #Test_Version(type)
        TestQuery(type)
    
        #CreateDBFileWithType(type)
        #TestQuery(type)
    #delete_dbfile()
    #os.remove("op.txt")
