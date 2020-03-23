import os
import subprocess


BUFFER_SIZE=100
FILENAME="project2_testdata"
DBNAME = "abc"
TESTS_OUTPUTFILE = "TestOutput.txt"

def delete_dbfile():
    dir_name = "/tmp/"
    test = os.listdir(dir_name)
    for item in test:
        if item.endswith(".bigtable-db"):
            os.remove(os.path.join(dir_name, item))


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
    os.system("javac Query.java")
    orderList=[1,2,3,4,6]
    #'"*" "*" "*"'
    filters = ['California "*" "*"', '[California,Greece] "*" "*"',
               '"*" Fox "*"', '"*" [Donkey,Fox] "*"',
               '"*" "*" 8000', '"*" "*" [200,8000]',
               'California1 Fox "*"', '[Kalifornia,Punk] "*" "*"',
               '"*" [A,Z] "*"', '"*" "*" "*"']

    # for filter in filters:
    #     print(filter)

    for order in orderList:
        os.system("echo ORDER_TYPE_" + str(order) + " ----------------------------------------- >> " + TESTS_OUTPUTFILE)
        for filter in filters:
             #os.system("echo FILTER " + filter + " >> " + TESTS_OUTPUTFILE)
             query = "java" + " " + "Query" + " " + DBNAME + " " + str(type) + " " + str(order) + " " + filter + " " + str(BUFFER_SIZE)
             outputfile = " > op.txt"
             os.system(query + outputfile)
             process = subprocess.Popen(['grep','-i','"Exception"', 'op.txt'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
             stdout, stderr = process.communicate()
             #print("OUTPUT " + stdout)

             if(stdout):
                print(stdout)
                os.system("echo EXCEPTION OCCURED " + stdout)
             if (stderr):
                print(stderr)
             #print("ERR "+stderr)
             os.system("echo %%%%%%%%%%%%%%%   ----\>  " + query + " >> " + TESTS_OUTPUTFILE)
             os.system("head -30 op.txt >> " + TESTS_OUTPUTFILE)
             os.system("tail -5 op.txt >> " + TESTS_OUTPUTFILE)
             #os.system("echo ------------------------------------------------------------------------------- >> " + TESTS_OUTPUTFILE)
             os.system("echo >> "+ TESTS_OUTPUTFILE)





if __name__=="__main__":

    delete_allClassFiles()
    delete_dbfile()
    if(os.path.exists(TESTS_OUTPUTFILE)):
        os.remove(TESTS_OUTPUTFILE)

    bigtType = [1,2,3,4,5]

    for type in bigtType:
        print("TYPE" , type)
        CreateDBFileWithType(type)
        TestQuery(type)

    os.remove("op.txt")