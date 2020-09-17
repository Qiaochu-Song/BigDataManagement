import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
//import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query3 extends Configured implements Tool{
	// Two input files: schema
	//	customer.txt: customerID, Name, Age, Gender(“male”/“female”), CountryCode(integer between 1 and 10), Salary.
	//  transaction.txt: transID,customerID, TransTotal(price), TransNumItems, TransDesc.
	
	// Write ONE job that join two files on customerID
    // reports for every country code, 
    // the number of customers having this code
	// as well as the min and max of TransTotal fields for the transactions done by those customers.
	// The output file should have one line for each customer containing:
	// report “CountryCode, NumberOfCustomers, MinTransTotal, MaxTransTotal”
		

	public static class Query3Mapper extends Mapper<Object, Text, Text, Text> {
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
			// maintain a HashMap for customerID: {countryCode}
			File localFile = new File(localPaths[0].toUri());
		    BufferedReader customer = new BufferedReader(new FileReader(localFile));
		    while (true) {
		    	String l = customer.readLine();
                if (l!=null) {
		    		String[] line= l.split(","); //line[0]=customerID, line[4] = countryCode
		    		nameTable.put(line[0], line[4]);
		    	}
                else if (l == null) {break;}
		    }    
		}
		
		@Override
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			// map each row: key = countryID; value = customerID+transaction cost
            String[] line = value.toString().split(",");  // line[0]=transID, line[1]=customerID, line[2]=cost, line[3]=itemCount
            // parse the record
            String customerID = line[1];
            String cost = line[2];
            //get the customer countryCode from private HashMap
            String countryCode = nameTable.get(customerID);
            context.write(new Text(countryCode), new Text(customerID+","+cost));
		}
	}
	
	public static class Query3Reducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			Float maxTransTotal = 0.0f;
			Float minTransTotal = Float.MAX_VALUE;
			HashSet<String> customer = new HashSet<String>();
			
			while (iter.hasNext()) {
			    // process each record: add transaction costs, count transaction, find minTransTotal and maxTransTotal
				// use a HashSet to process unique customers
				Text r = new Text(iter.next());
				//record = "customerID, cost"
				String[] record = r.toString().split(",");  
				String customerID = record[0];
				Float cost = Float.parseFloat(record[1]);
				customer.add(customerID);
				if (cost < minTransTotal) {
					minTransTotal = cost;
				}
				if (cost > maxTransTotal) {
					maxTransTotal = cost;
				}
			}
			Integer customerCount = customer.size();
			// write the reduce output, the key gives the customerID
			context.write(new Text(key),new Text(customerCount+","+minTransTotal.toString()+","+maxTransTotal.toString())); 
		}
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage:  <customer input> <transaction input> <output>");
			return -1;
		}

		Job job = new Job(getConf(), "Query3"); 
		job.setJarByClass(getClass());
		//parse agrs, set file path
		Path customerInputPath = new Path(args[0]);
		Path transactionInputPath = new Path(args[1]); 
		Path outputPath = new Path(args[2]);
		FileInputFormat.addInputPath(job, transactionInputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		//set mapper,combiner,reducer class
		job.setMapperClass(Query3Mapper.class);
		//job.setCombinerClass(Query3Reducer.class); 
		job.setReducerClass(Query3Reducer.class);
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
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new Query3(), args); 
		System.exit(exitCode);
	}

	}
