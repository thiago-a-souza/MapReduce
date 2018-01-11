# Author
Thiago Alexandre Domingues de Souza

# MapReduce
MapReduce is a parallel programming model described by Google to process large amounts of data (1). This method enables programmers to design parallel applications on large clusters using commodity hardware, allowing the development of scalable programs without worrying about the computing environment. Behind the scenes, the system hides the complexity of partitioning and distributing the data across nodes, load balancing, tracking the job execution, monitoring nodes availability, etc. As result of that, the programmer can focus on the problem that needs to be solved rather than handling underlying  circumstances.

Programs based on the MapReduce model should implement two functions: map and reduce. The map function is applied to every row of the dataset, extracting relevant columns and returning a (key, value) pair. An internal step, called "Shuffle and Sort", takes the output from the map function, divides the data into partitions, corresponding to the reducer nodes, performs an in-memory sort by key and transfers the partitions to reducer nodes. The reducer node merges partitions coming from different mapper nodes into a larger partition, maintaining their sort order (2). This step produces the input for the reduce function, coming in a (key, list(values)) format. The reduce function performs a commutative and associative operation, aggregating the input based on the implemented logic, and returns a final key/value pair. The MapReduce phases are displayed in Figure 1:

![MapReduce phases](/img/mapreduce_phases.png "Figure 1: MapReduce phases")

In order reduce the network traffic and optimize the data transfer between mapper and reducer nodes, MapReduce programs can implement a combiner function. This optimization function run on mapper nodes, taking as input the output from the map function and its output is sent to the reduce function. A typical combiner implementation is similar to the reduce function. However, depending on the reduce operation implemented, the combiner cannot use the same logic as the reduce, because the combiner has only the data available on its particular mapper node rather than all the data available on the reducer. It's important to note that combiners are optimization functions. As such, there's no guarantee how many times it will run or if it will run at all.

## Logical MapReduce Example

Assume that the table below represents a sample of a large dataset of movie ratings downloaded from GroupLens website (3). And our goal is to calculate the number of movies rated per user. Using a MapReduce approach to solve this problem requires designing a map function, that will extract the required columns from the dataset, and a reduce function, that will return the number of movies rated per user.


| USER_ID |  MOVIE_ID | RATING | TIMESTAMP |
|:-------:|:---------:|:------:|:---------:|
| 100 | 6 | 3.0 | 854194023 |
| 122 | 1 | 3.0 | 832773057 |
| 100 | 7 | 3.0 | 854194024 |
| 144 | 10 | 5.0 | 837455462 |
| 133 | 47 | 2.5 | 1416147036 |
| 122 | 73 | 4.0 | 832773302 |
| 122 | 150 | 3.0 | 832772925 |



The MapReduce program can be summarized as follows:

**1. Map:** 
this function processes every row of the dataset and returns a (key, value) pair. In this case, the key is the *USER_ID* and the value is 1, indicating that the corresponding movie was rated.

Map output: (100,1), (122,1), (100,1), (144,1) (133,1), (122,1), (122,1)




**2. Shuffle and sort:**
this internal phase take as input the output from the map function and creates partitions based on the reducer nodes. After that, it sorts the data by key, transfer them to reducer nodes and merge partitions. This step returns a (key, list(values)) pair, which is used by the reduce function.

Shuffle and sort output: (100,(1,1)), (122,(1,1,1)),  (133,(1)),  (144,(1))

**3. Reduce:**
this function receives a (key, list(values)) pair of input from the previous phase and applies the implemented function. It returns a (key, value) pair, where the key is the *USER_ID* and the value is the sum of items in the list received.

Reduce output: (100,2), (122,3), (133,1), (144,1)

## MapReduce in Java
Because Hadoop APIs are written in Java, this is a popular programing language to solve MapReduce problems. A typical MapReduce program in Java has three classes:

- **Mapper:** class implements the map function 
- **Reducer:** class implements the reduce function
- **Driver:** this is the main class, it defines de mapper/reducer classes and additional configurations


**dependencies:**
```
<properties>
  <hadoop.version>2.8.1</hadoop.version>
</properties>

<dependencies>
  <dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-mapreduce-client-core</artifactId>
    <version>${hadoop.version}</version>
  </dependency>
  <dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>${hadoop.version}</version>
  </dependency>
</dependencies>	
```

**Java classes:**
```java
package com.thiago;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MoviesRatedPerUser {
  public static class MoviesRatedPerUserMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");	
      context.write(new Text(tokens[0]), new IntWritable(1));
    }
  }	
  
  public static class MoviesRatedPerUserReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
    public void reduce(Text key, Iterable<IntWritable> count, Context context) throws IOException, InterruptedException {
      long sum = 0;	
      Iterator<IntWritable> it = count.iterator();

      while(it.hasNext()) {
            it.next();
            sum++;
      }
      context.write(key, new LongWritable(sum));
    }
  }
	
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {	
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MoviesRatedPerUser");
    job.setJarByClass(MoviesRatedPerUser.class);
	    
    job.setMapperClass(MoviesRatedPerUserMapper.class);	   
    job.setReducerClass(MoviesRatedPerUserReducer.class);
	    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
 	    	    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);		
  }
}
```

**Generating jar file:**
```
mvn package
```

**Running the job:**
```
hadoop jar MRJava-1.0.jar com.thiago.MoviesRatedPerUser /your/path/to/input/file /your/path/to/output/directory
```

## MapReduce in Python
In addition to Java support, thanks to Hadoop Streaming, MapReduce programs can be developed in any programming language, providing the mapper and reducer functions that read from standard input and write to standard output. However, this flexibility comes at a higher computational cost.

Python is a powerful interpreted programming language. It has a very natural and concise syntax, making it easier the adoption for both new and experienced programmers. Besides, it has a very active community that develops rich libraries for a wide range of problems. As result of that, Python is a very popular language in several fields, such as web development, data science, mobile, etc.

Writing MapReduce programs in Python does not require any additional library, but the mrjob (4) package makes it easier to develop and run your code. Another nice thing about mrjob is that it allows running MapReduce programs without installing Hadoop. That way the developer can easily test the code before running on a cluster. Additionally, running mrjobs on cloud  services such as Amazon EMR is also supported.

**Installing mrjob:**
```
pip install mrjob
```

**Python mrjob:**
```python
from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviesRatedPerUser(MRJob):
	def mapper(self, _, line):
		(userID, movieID, rating, timestamp) = line.split(',')
		yield userID, 1 

	def reducer(self, userID, values):
		yield userID, sum(values)

if __name__ == '__main__':
	MoviesRatedPerUser.run()
```

**Running locally:**
```
python MoviesRatedPerUser.py your_input_file.txt
```
**Running on Hadoop:**
```
python MoviesRatedPerUser.py -r hadoop --hadoop-streaming-jar /your/path/to/hadoop-streaming-{your version}.jar  your_input_file.txt
```




# References
(1) Dean, Jeffrey, and Sanjay Ghemawat. "MapReduce: simplified data processing on large clusters." Communications of the ACM 51.1 (2008): 107-113.

(2) White, Tom. Hadoop: The definitive guide. " O'Reilly Media, Inc.", 2012.

(3) GroupLens - https://grouplens.org/datasets/movielens/

(4) mrjob documentation - https://pythonhosted.org/mrjob/

