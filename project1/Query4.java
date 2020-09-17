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

public class Query4 extends Configured implements Tool{
	// Two input files: schema
	//	customer.txt: customerID, Name, Age, Gender(“male”/“female”), CountryCode(integer between 1 and 10), Salary.
	//  transaction.txt: transID,customerID, TransTotal(price), TransNumItems, TransDesc.
	
	// The Age attribute is divided into six groups, which are [10, 20), [20, 30), [30, 40), [40, 50), [50, 60), and [60, 70].
	// Within each of the above age ranges, 
	// further division is performed based on the “Gender”, 
	// i.e., each of the 6 age groups is further divided into two groups.
	// For each group, report "Age Range, Gender, MinTransTotal, MaxTransTotal, AvgTransTotal"
		

	public static class Query4Mapper extends Mapper<Object, Text, Text, Text> {
		// since the customer file is very small, we can use distributed cache
		// at the mapper side by sending the file to mappers setup method.
		// map the customer data into a HashMap "nameTable" 
		
		private HashMap <String, String[]> nameTable =  new HashMap <String, String[]>();
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException, FileNotFoundException {
			// configure the distributed cache from file (localPaths[0])
			Path[] localPaths = context.getLocalCacheFiles(); 
			if (localPaths.length == 0) {
				throw new FileNotFoundException("Distributed cache file not found."); }
			// maintain a HashMap for customerID: {age, gender}
			File localFile = new File(localPaths[0].toUri());
		    BufferedReader customer = new BufferedReader(new FileReader(localFile));
		    while (true) {
		    	String l = customer.readLine();
                if (l!=null) {
		    		String[] line= l.split(","); //line[0]=customerID, line[2] = Age, line[3]=gender
		    		nameTable.put(line[0], new String[] {line[2], line[3]});
		    	}
                else if (l == null) {break;}
		    }    
		}
		
		@Override
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			// map each row: key = (ageRange, gender); value = customerID+transaction cost
            String[] line = value.toString().split(",");  // line[0]=transID, line[1]=customerID, line[2]=cost, line[3]=itemCount
            // parse the record
            String customerID = line[1];
            String cost = line[2];
            //get the customer's age and gender from private HashMap
            Integer age = Integer.parseInt(nameTable.get(customerID)[0]);
            String gender = nameTable.get(customerID)[1];
            String ageGroup = ""
            // get age group [10, 20), [20, 30), [30, 40), [40, 50), [50, 60), and [60, 70]
            if (age < 40) {
            	if (age>=30) {ageGroup = "[30, 40)";}
            	else if (age >=20) {ageGroup = "[20, 30)";}
            	else {ageGroup = "[10, 20)";}
            }
            else {
            	if (age < 50) {ageGroup = "[40, 50)";}
            	else if (age < 60) {ageGroup = "[50, 60)";}
            	else {ageGroup = "[60, 70]";}
            }
            
            context.write(new Text(ageGroup + "," + gender), new Text(cost));
		}
	}
	
	public static class Query4Reducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			Float maxTransTotal = 0.0f;
			Float minTransTotal = Float.MAX_VALUE;
			Float sumTransTotal = 0.0f;
			HashSet<String> customer = new HashSet<String>();
			Integer count = 0;
			while (iter.hasNext()) {
			    // process each record: find minTransTotal, maxTransTotal and avgTransTotal for each group
				Text r = new Text(iter.next());
				//record = "customerID, cost"
				String record = r.toString();  
				Float cost = Float.parseFloat(record);
				count += 1;
				sumTransTotal += cost;
				if (cost < minTransTotal) {
					minTransTotal = cost;
				}
				if (cost > maxTransTotal) {
					maxTransTotal = cost;
				}
			}
			Float avgTransTotal = sumTransTotal / (float) count;
			
			// write the reduce output, the key gives the customerID
			context.write(new Text(key),new Text(minTransTotal.toString()+","+maxTransTotal.toString()+","+avgTransTotal.toString())); 
		}
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage:  <customer input> <transaction input> <output>");
			return -1;
		}

		Job job = new Job(getConf(), "Query4"); 
		job.setJarByClass(getClass());
		//parse agrs, set file path
		Path customerInputPath = new Path(args[0]);
		Path transactionInputPath = new Path(args[1]); 
		Path outputPath = new Path(args[2]);
		FileInputFormat.addInputPath(job, transactionInputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//set mapper,combiner,reducer class
		job.setMapperClass(Query4Mapper.class);
		//job.setCombinerClass(Query4Reducer.class); 
		job.setReducerClass(Query4Reducer.class);
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
		int exitCode = ToolRunner.run(new Query4(), args); 
		System.exit(exitCode);
	}

	}
