import sys
from pyspark 
import SparkContext, SparkConf


def maxacc(word):
	
	f = word.split(",")
	
	return(f[0],(int(f[3])+int(f[4])+int(f[5])+int(f[6])+int(f[7])+int(f[8])+int(f[9])+int(f[10])))



def boromaxacc(word):
	
	f = word.split(",")
	
	return(f[1],(int(f[4])+int(f[6])+int(f[8])+int(f[10])))


def zipmaxacc(word):
	
	f = word.split(",")
	
	return(f[2],(int(f[4])+int(f[6])+int(f[8])+int(f[10])))

def vehmaxacc(word):
	
	f = word.split(",")
	
	#print(f[11])
	
	#print(int(f[3])+int(f[4])+int(f[5])+int(f[6])+int(f[7])+int(f[8])+int(f[9])+int(f[10]))
	
	return(f[11],(int(f[3])+int(f[4])+int(f[5])+int(f[6])+int(f[7])+int(f[8])+int(f[9])+int(f[10])))
def perpedinj(word):
	
	f = word.split(",")
	
	f1 = f[0].split("/")
	
	return("PersonPedestrianInjured"+f1[2],(int(f[3])+int(f[5])))


def perpedkil(word):
	
	f = word.split(",")
	
	f1 = f[0].split("/")
	
	return("PersonPedestrianKilled"+f1[2],(int(f[4])+int(f[6])))


def cycinjkil(word):
	
	f = word.split(",")
	
	f1 = f[0].split("/")
	
	return("CyclistiInjuredKilled"+f1[2],(int(f[7])+int(f[8])))



def motinjkil(word):
	
	f = word.split(",")
	
	f1 = f[0].split("/")
	
	return("MotoristInjuredKilled"+f1[2],(int(f[9])+int(f[10])))


if __name__ == "__main__":
	
	conf = SparkConf().setAppName("Spark CC Project2")
	
	sc = SparkContext(conf=conf)
	
	tokenized = sc.textFile(sys.argv[1])
	
	print(tokenized)
	
	x = []
	
	
	finalWord1 = tokenized.map(lambda word:maxacc(word))
	
	finalWord2 = tokenized.map(lambda word:boromaxacc(word))
	
	finalWord3 = tokenized.map(lambda word:zipmaxacc(word))		
	
	finalWord4 = tokenized.map(lambda word:perpedinj(word))
	
	finalWord5 = tokenized.map(lambda word:perpedkil(word))
	
	finalWord6 = tokenized.map(lambda word:cycinjkil(word))
	
	finalWord7 = tokenized.map(lambda word:motinjkil(word))
	
	finalWord8 = tokenized.map(lambda word:vehmaxacc(word))
	
	
	fin1 = finalWord1.reduceByKey(lambda v1,v2:int(v1 +v2))
	
	l1 = fin1.collect()
	
	fin2 = finalWord2.reduceByKey(lambda v1,v2:int(v1 +v2))
	
	l2 = fin2.collect()
	
	fin3 = finalWord3.reduceByKey(lambda v1,v2:int(v1 +v2))
	
	l3 = fin3.collect()
	
	fin4 = finalWord4.reduceByKey(lambda v1,v2:int(v1 +v2))
	
	l4 = fin4.collect()
	
	fin5 = finalWord5.reduceByKey(lambda v1,v2:int(v1 +v2))
	
	l5 = fin5.collect()
	
	fin6 = finalWord6.reduceByKey(lambda v1,v2:int(v1 +v2))
	
	l6 = fin6.collect()
	
	fin7 = finalWord7.reduceByKey(lambda v1,v2:int(v1 +v2))
	
	l7 = fin7.collect()
	
	fin8 = finalWord8.reduceByKey(lambda v1,v2:int(v1 +v2))
	
	l8 = fin8.collect()
	
	final = fin1.union(fin2).union(fin3).union(fin4).union(fin5).union(fin6).union(fin7).union(fin8)
	
	list1 = final.collect()

	#Task 2 Output
	
	final.repartition(1).saveAsTextFile("/user/sinhark/books/out2")
	
	test = sc.parallelize(l1)
	
	test1 = sc.parallelize(l2)
	
	test2= sc.parallelize(l3)
	
	test4 = sc.parallelize(l4)
	
	test5 = sc.parallelize(l5)	
	
	test6 = sc.parallelize(l6)
	
	test7 = sc.parallelize(l7)
	
	test8 = sc.parallelize(l8)
	
	s1 = "Date with maximum accident count"
	
	x.append(s1)
	
	x1=(test.max(key = lambda x: x[1]))
	
	x.append(x1)
	
	x10=test1.max(key = lambda x: x[1])
	
	s2 = "Borough having maximum accident fatality"
		
	x.append(s2)
	
	x.append(x10)
	
	#print(x)
	
	x2=test2.max(key = lambda x: x[1])
	
	s3 = "Zip with maximum accident fatality"
	
	x.append(s3)
	
	x.append(x2)
	
	x3=test5.max(key = lambda x: x[1])
	
	s4 = "Year in which maximum Number Of Persons and Pedestrians Injured"
	
	x.append(s4)
	
	x.append(x3)
	
	x4=test4.max(key = lambda x: x[1])
	
	s5 = "Year in which maximum Number Of Persons and Pedestrians Killed"
	
	x.append(s5)
	
	x.append(x4)
	
	x5=test6.max(key = lambda x: x[1])
	
	s6 = "Year in which maximum Number Of Cyclist Injured and Killed"
	
	x.append(s6)
	
	x.append(x5)
	
	x6 =test7.max(key = lambda x: x[1])
	
	s7 = "Year in which maximum Number Of Motorist Injured and Killed"
	
	x.append(s7)
	
	x.append(x6)
	
	x7=test8.max(key = lambda x: x[1])
	
	s8 = "Vehicle involved in maximum  accident"
	
	x.append(s8)
	
	x.append(x7)
	
	print1 = sc.parallelize(list(x))
	
	#Task 3 output
	
	print1.repartition(1).saveAsTextFile("/user/sinhark/books/final3")
