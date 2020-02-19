JDKPATH = /Library/Java/JavaVirtualMachines/jdk-13.0.2.jdk/Contents/Home
LIBPATH = /Users/rahulsanjay/Downloads/DBMSI/minjava/javaminibase/construction
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

bm:BufMgr.java
	$(JAVAC) BufMgr.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
