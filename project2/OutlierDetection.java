//package proj02;
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
//import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OutlierDetection extends Configured implements Tool{
	public static class Point{
		public int x;
		public int y;
		public int num_neighbors;
		public Point(int a, int b) {  // constructor, no return type
			this.x = a;
			this.y = b;	
			this.num_neighbors = 0;
		}
	}
	public static class OutlierMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
		    // get the radius info from configuration file
			Configuration conf = context.getConfiguration();
            String radius = conf.get("r");
            int r = Integer.parseInt(radius);
            
			String[] line = value.toString().split(",");
			int x = Integer.parseInt(line[0]);
			int y = Integer.parseInt(line[1]);

			// divide the zone into 100*100 grids
			// map each point (circle(p,r)) to the grid it is in as main point
			// map each point to the grid its circle zone overlaps as support point, mark with "s"
			int x_idx = Math.min(x / 100, 99);  // the x-grid index, start from 0, max = 99
			int y_idx = Math.min(y / 100, 99);  // the x-grid index, start from 0, max = 99
			// k = gridX,gridY, v = point_x, point_y
			context.write(new Text(x_idx+","+y_idx), new Text(x+","+y));
			int dist_x_l = x - x_idx * 100;  // the related bound is x_idx*100
			int dist_x_r =  (x_idx + 1) * 100 - x;  // the related bound is (x_idx + 1)*100
			int dist_y_u = (y_idx + 1) * 100 - y;  // the related bound is (y_idx+1)*100
			int dist_y_d = y - y_idx * 100;  // the related bound is y_idx*100
			// distance to the 4 vertices of the grid
			double dist_diag_ul = Math.sqrt(dist_x_l * dist_x_l + dist_y_u * dist_y_u);
			double dist_diag_ur = Math.sqrt(dist_x_r * dist_x_r + dist_y_u * dist_y_u);
			double dist_diag_dl = Math.sqrt(dist_x_l * dist_x_l + dist_y_d * dist_y_d);
			double dist_diag_dr = Math.sqrt(dist_x_r * dist_x_r + dist_y_d * dist_y_d);
			// k = gridX,gridY, v = point_x, point_y
			// do NOT map out of the whole zone
			// add a label "s" to indicate this point is added as a support point
			if (dist_x_l < r && x_idx != 0) {context.write(new Text((x_idx-1)+","+y_idx), new Text("s,"+x+","+y));}
			if (dist_x_r < r && x_idx != 99) {context.write(new Text((x_idx+1)+","+y_idx), new Text("s,"+x+","+y));}
			if (dist_y_u < r && y_idx != 99) {context.write(new Text(x_idx+","+(y_idx+1)), new Text("s,"+x+","+y));}
			if (dist_y_d < r && y_idx != 0) {context.write(new Text(x_idx+","+(y_idx-1)), new Text("s,"+x+","+y));}

			if (dist_diag_ul > r && x_idx!=0 && y_idx!=99) {context.write(new Text((x_idx-1)+","+ (y_idx+1)), new Text("s,"+x+","+y));}
			if (dist_diag_ur > r && x_idx!=99 && y_idx!=99) {context.write(new Text((x_idx+1)+","+ (y_idx+1)), new Text("s,"+x+","+y));}
			if (dist_diag_dl > r && x_idx!=0 && y_idx!=0) {context.write(new Text((x_idx-1)+","+ (y_idx-1)), new Text("s,"+x+","+y));}
			if (dist_diag_dr > r && x_idx!=99 && y_idx!=0) {context.write(new Text((x_idx+1)+","+ (y_idx-1)), new Text("s,"+x+","+y));}
		}
	}		
	
    public static class OutlierReducer extends Reducer<Text, Text, NullWritable, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			// get the radius and k info from configuration file
			Configuration conf = context.getConfiguration();  // or directly use getConf method of Configured interface
            String radius = conf.get("r");
            int r = Integer.parseInt(radius);
			int k = Integer.parseInt(conf.get("k"));
			
			Iterator<Text> iter = values.iterator();
			// we need two array list to extract points in the grid and keep all mapped points as support
			// in case we write one point multiple times to output
			ArrayList<Point> mainPoints = new ArrayList<Point>();
			ArrayList<Point> supportPoints = new ArrayList<Point>();
			while(iter.hasNext()) {
				Text t = new Text(iter.next());
				//record = "x,y" or "s,x,y" as a support point
				String[] rec = t.toString().split(",");
				if (rec[0]=="s") {  // this is a support point
					Point p = new Point(Integer.parseInt(rec[1]),Integer.parseInt(rec[2]));
					supportPoints.add(p);
				}
				else if(rec.length == 2){  // this is a main point(x,y), add to both ArrayLists
					Point p = new Point(Integer.parseInt(rec[0]),Integer.parseInt(rec[1]));  
					mainPoints.add(p);
					supportPoints.add(p);
				}
			}
			// Loop through all points support this zone, count neighbors
			// note the point itself is counted
			for (int i=0; i<mainPoints.size(); i++) {
				for (int j=0; j<supportPoints.size(); j++) {  
					int x_dist = mainPoints.get(i).x - supportPoints.get(j).x;  //can be negative
					int y_dist = mainPoints.get(i).y - supportPoints.get(j).y;  //can be negative
					double dist = Math.sqrt(x_dist * x_dist + y_dist * y_dist);
					if (dist < r) {mainPoints.get(i).num_neighbors++;}  // the #neighbors of main point++
				}
			}
			
			for(Point p: mainPoints) {
				if(p.num_neighbors<(k+1)) {context.write(NullWritable.get(),new Text(p.x+","+p.y));}
		    }
        }
    }
    
	public int run(String[] args) throws Exception {
		if (args.length != 4) {  // test with r = 50, k = 50
			System.out.println("Usage: <inputFileDir> <outputDir> <radius> <k>");
			return -1;
		}
		// set the r and k in configuration xml
        Configuration conf = getConf();
        conf.set("r", args[2]);
        conf.set("k", args[3]);
        
		Job job = new Job(getConf(), "OutlierDetection"); 
		job.setJarByClass(getClass());
		
		//parse args, set file path
		Path pointInputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		FileInputFormat.addInputPath(job, pointInputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		//set mapper,combiner,reducer class
		job.setMapperClass(OutlierMapper.class);
		job.setReducerClass(OutlierReducer.class);
		
		//set K, V classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class); 
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new OutlierDetection(), args); 
		System.exit(exitCode);
	}
}
