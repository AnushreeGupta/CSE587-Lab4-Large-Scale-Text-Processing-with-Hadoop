#Below code was used to make a text file for the output from mapreduce. 


python2.7 
Python 2.7.12 (default, Nov 19 2016, 06:48:10) 
[GCC 5.4.0 20160609] on linux2
Type "help", "copyright", "credits" or "license" for more information.
>>> a = open("part-r-00000").read().split("\n")
>>> a[0]
'#\t7'
>>> a = [i.split("\t") for i in a]
>>> a[0]
['#', '7']
>>> b = open("text.txt","w")
>>> for i in a:
...     b.write(i[0]*int(i[1]) + " ")
... 
IndexError: list index out of range
>>> for i in a:
...     if len(i) != 2:
...             print i
... 
['']
>>> b.close()
>>> 
[2]+  Stopped                 python2.7

