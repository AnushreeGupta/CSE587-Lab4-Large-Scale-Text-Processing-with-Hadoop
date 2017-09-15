# Large Scale Text Processing with Hadoop

Major goals of the Lab 4 are to:

1. Identify problems solvable using MR approach.
2. Design MR Algorithms for solving big data problems involving Write Once Read Many (WORM) data such as historical text, health care data.
3. Understand and learn to apply MapReduce (MR) algorithm for processing large data sets.
4. Store and retrieve text data in Hadoop Distributed File System (HDFS) as <key,value>.
5. Implement the MR solutions designed in steps 2 and 3 on a stand-alone virtual machine or on the cloud (Amazon AWS, Google or cloud service providers).
6. Interpret the results to enable decision making.

The lab is divided into two activities.

### Activity 1: Wordcount on Classical Latin text

This problem was provided by researchers in the Classics department at UB. They have provided two classical texts and a lemmatization file to convert words from one form to a standard or normal form. In this case you will use several passes through the documents.

* Pass 1: Lemmetization using the lemmas.csv file
* Pass 2: Identify the words in the texts by <word <docid, [chapter#, line#]> for two documents.
* Pass 3: Repeat this for multiple documents.

### Activity 2: Word co-occurrence among multiple documents

* In this activity you are required to “scale up” the word co-occurrence by increasing the number of documents processed from 2 to n. Record the performance of the MR infrastructure and plot it. 
* From word co-occurrence that deals with just 2-grams (or two words co-occurring) increase the co-occurrence to n=3 or 3 words co-occurring. Discuss the results and plot the performance and scalability.

###### *Note: The step-wise procedure is explained in the Lab4_Report.pdf*
