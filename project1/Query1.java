import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
//import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.filecache.DistributedCache;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query1 extends Configured implements Tool{
	// Two input files: schema
	//	customer.txt: customerID, Name, Age, Gender(“male”/“female”), CountryCode(integer between 1 and 10), Salary.
	//  transaction.txt: transID,customerID, TransTotal(price), TransNumItems, TransDesc.
	
	// Write a job(s) that reports for every customer, 
	// the number of transactions that customer did and the total sum of these transactions. 
	// The output file should have one line for each customer containing:
	// CustomerID, CustomerName, NumTransactions, TotalSum
	

	public static class Query1Mapper extends Mapper<Object, Text, Text, Text> {
		//since the customer file is very small, we can use distributed cache
		// at the mapper side by sending the file to mappers setup method.
		// map the customer data into a HashMap "nameTable"
		private HashMap <String, String> nameTable =  new HashMap <String, String>();
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException, FileNotFoundException {
			// configure the distributed cache from file (localPaths[0])
			Path[] localPaths = context.getLocalCacheFiles(); 
			if (localPaths.length == 0) {
				throw new FileNotFoundException("Distributed cache file not found."); }
			    // 
			File localFile = new File(localPaths[0].toUri());
		    BufferedReader customer = new BufferedReader(new FileReader(localFile));
		    while (true) {
		    	String l = customer.readLine();
                if (l!=null) {
		    		String[] line= l.split(","); //line[0]=customerID, line[1]=name
		    		nameTable.put(line[0], line[1]);
		    	}
                else if (l == null) {break;}
		    }    
		}
		
		@Override
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			// map each row: key = customerID; value = transaction cost
            String[] line = value.toString().split(",");  // line[0]=transID, line[1]=customerID, line[2]=cost 
            String name = nameTable.get(line[1]);  //get the customer name
            context.write(new Text(line[1]), new Text(name+","+line[2]));
		}
	}
	
	public static class Query1Reducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException { 
			Integer transCount = 0;
			Float spentSum = 0.0f;
			String customerName = "";
			for (Text v: values) {
			    // process each record: add transaction costs
				//record = name+cost
				String[] record = v.toString().split(",");
				customerName = record[0];
				spentSum += Float.parseFloat(record[1]);
				transCount += 1;
			}
			
			// write the reduce output, the key gives the customerID
			context.write(new Text(key),new Text(customerName+","+transCount+","+spentSum)); 
		}
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage:  <customer input> <transaction input> <output>");
			return -1;
		}
		//Configuration conf = new Configuration();
		Job job = new Job(getConf(), "Query1"); 
		job.setJarByClass(getClass());
		//parse agrs, set file path
		Path customerInputPath = new Path(args[0]);
		Path transactionInputPath = new Path(args[1]); 
		Path outputPath = new Path(args[2]);
		//set input/output format class and mapper class
		FileInputFormat.addInputPath(job, transactionInputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		//job.setOutputFormatClass(TextOutputFormat.class);
		//set mapper,combiner,reducer class
		job.setMapperClass(Query1Mapper.class);
		//job.setCombinerClass(Query1Reducer.class); 
		job.setReducerClass(Query1Reducer.class);
		//set K, V classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(Text.class);
		// the DistributedCache.addCacheFile(file, getConf()) is deprecated
		job.addCacheFile(customerInputPath.toUri());
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		// Auto-generated method stub
		int exitCode = ToolRunner.run(new Query1(), args); 
		System.exit(exitCode);
	}

}
