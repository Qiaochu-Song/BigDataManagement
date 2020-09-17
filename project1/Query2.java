import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
//import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
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

public class Query2 extends Configured implements Tool{
	// Two input files: schema
	//	customer.txt: customerID, Name, Age, Gender(“male”/“female”), CountryCode(integer between 1 and 10), Salary.
	//  transaction.txt: transID,customerID, TransTotal(price), TransNumItems, TransDesc.
	
	// Write a job(s) that join two files on customerID
	// The output file should have one line for each customer containing:
	// report “CustomerID, Name, Salary, NumOfTransactions, TotalSum, MinItems”
	

	public static class Query2Mapper extends Mapper<Object, Text, Text, Text> {
		//since the customer file is very small, we can use distributed cache
		// at the mapper side by sending the file to mappers setup method.
		// map the customer data into a HashMap "nameTable" 
		private HashMap <String, String[]> nameTable =  new HashMap <String, String[]>();
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException, FileNotFoundException {
			// configure the distributed cache from file (localPaths[0])
			Path[] localPaths = context.getLocalCacheFiles(); 
			if (localPaths.length == 0) {
				throw new FileNotFoundException("Distributed cache file not found."); }
			// maintain a HashMap for customerID: {name, salary}
			File localFile = new File(localPaths[0].toUri());
		    BufferedReader customer = new BufferedReader(new FileReader(localFile));
		    while (true) {
		    	String l = customer.readLine();
                if (l!=null) {
		    		String[] line= l.split(","); //line[0]=customerID, line[1]=name, line[5] = salary
		    		nameTable.put(line[0], new String[]{line[1],line[5]});
		    	}
                else if (l == null) {break;}
		    }    
		}
		
		@Override
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			// map each row: key = customerID; value = transaction cost, itemcount
            String[] line = value.toString().split(",");  // line[0]=transID, line[1]=customerID, line[2]=cost, line[3]=itemCount
            // parse the record
            String cost = line[2];
            String itemCount = line[3];
            //get the customer {name, salary} from private HashMap
            String[] info = nameTable.get(line[1]);  
            String name = info[0];
            String salary = info[1];
            context.write(new Text(line[1]), new Text(name+","+salary+","+cost+","+itemCount));
		}
	}
	
	public static class Query2Reducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator(); 
			Integer transCount = 0;
			Float spentSum = 0.0f;
			Integer minItem = Integer.MAX_VALUE;
			String customerName = "";
			String salary = "";
			while (iter.hasNext()) {
			    // process each record: add transaction costs, count transaction, find minItem
				Text r = new Text(iter.next());
				//record = name+salary+cost+itemCount
				String[] record = r.toString().split(",");  
				customerName = record[0];
				salary = record[1];
				spentSum += Float.parseFloat(record[2]);
				transCount += 1;
				Integer itemCount = Integer.parseInt(record[3]);
				if (Integer.parseInt(record[3]) < minItem) {
					minItem = itemCount;
				}
			}
			
			// write the reduce output, the key gives the customerID
			context.write(new Text(key),new Text(customerName+","+salary+","+transCount.toString()+","+spentSum.toString()+","+minItem.toString())); 
		}
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage:  <customer input> <transaction input> <output>");
			return -1;
		}

		Job job = new Job(getConf(), "Query2"); 
		job.setJarByClass(getClass());
		//parse agrs, set file path
		Path customerInputPath = new Path(args[0]);
		Path transactionInputPath = new Path(args[1]); 
		Path outputPath = new Path(args[2]);
		FileInputFormat.addInputPath(job, transactionInputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		//set mapper,combiner,reducer class
		job.setMapperClass(Query2Mapper.class);
		//job.setCombinerClass(Query2Reducer.class); 
		job.setReducerClass(Query2Reducer.class);
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
		int exitCode = ToolRunner.run(new Query2(), args); 
		System.exit(exitCode);
	}

}
