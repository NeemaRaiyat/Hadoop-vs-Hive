import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.StringTokenizer;

import javax.management.loading.MLetContent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.NavigableMap;




// import java.math.Double;


public class Q2Memory {

	//mapper to pass the net paid values to reducers where the key is the storeID
	public static class NetSumMapper extends Mapper<Object, Text, Text, Text> {

		private Text net = new Text();
		private Text sk = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString(), "|", true);
			//start and end dates stored from the users arguments 
			int start = Integer.parseInt(context.getConfiguration().get("startDate"));
			int end = Integer.parseInt(context.getConfiguration().get("endDate"));

			Integer itemSoldDate = null;
			String storeID = null;
			String netPaid = null;

			int i = 1;

			//iterates through the records of the store_sales file
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				//increments i when a pipe is found to show what attribute is currently being read
				if (token.equals("|")) {
					i ++;
				}
				else {
					//stores the required attributes
					if (i == 1) {
						itemSoldDate = Integer.parseInt(token);
					}
					else if (i == 8) {
						storeID = token;
					}
					else if (i == 21) {
						netPaid = token;
					}
				}
			}

			//makes sure none of the required attributes are empty
			if (itemSoldDate != null && storeID != null && netPaid != null) {
				//makes sure the items were sold within the given date range
				if (itemSoldDate >= start && itemSoldDate <= end) {
					
					sk.set(storeID);
					net.set(netPaid);
					//uses the storeID as the key so reducers take all netPaid values for each store
					context.write(sk, net);

				}

			}

		}

	}

	// reducer taking net paid value for each storeID as inputs
	public static class NetSumReducer extends Reducer<Text,Text,Text,Text> {
	
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Get path to store table stored in cache
			URI[] cacheFiles = context.getCacheFiles();
			URI storeFile = cacheFiles[0];
			Path path = new Path(storeFile.toString());

			// Open store table as file
			org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(context.getConfiguration()); 
			FSDataInputStream fdsis = fs.open(path);

			// Read store table file
			BufferedReader br = new BufferedReader(new InputStreamReader(fdsis));
			
			// Store key and floor space for current reducer
			String actualStoreSk = key.toString();
			String actualFloorSpace = null;

			// Run through every line of stores table
			String line;
			while ((line = br.readLine()) != null) {
				
				// Extract information
				StringTokenizer itr = new StringTokenizer(line, "|", true);
				String curStoreSK = null;
				String curFloorspace = null;

				int i = 1;

				// Get key and floor space attributes of current line
				while (itr.hasMoreTokens()) {
					String token = itr.nextToken();
					if (token.equals("|")) {
						i ++;
					}
					else {
						if (i == 1) {
							curStoreSK = token;
						}
						else if (i == 8) {
							curFloorspace = token;
							break;
						}
					}
				}

				// If correct store key then memorise the floor space and break to stop reading store file
				if (curStoreSK != null) {
					
					if (curStoreSK.equals(actualStoreSk)) {
						actualFloorSpace = curFloorspace;
						break;
					}

				}
			}

			
			double totalNet = 0.0;

			// Iterate through each value given by mapper and sum net paids
			for (Text val: values) {
				double curNet = Double.parseDouble(val.toString());
				totalNet += curNet;
			}
			
			// Write store key, floor space and net pay to intermediate file
            String netResults = String.format("%.2f", totalNet);
			String results = actualFloorSpace + "\t" + netResults;

			if (actualFloorSpace != null && actualStoreSk != null) {
				context.write(key, new Text(results));
			}
            
		}

	}

	//mapper to pass the results to a single reducer
	public static class LimitMapper extends Mapper<Object, Text, Text, Text> {


		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
	
			String[] inputs = value.toString().split("\\s+");
	
			String shopID = inputs[0];
			String floor = inputs[1];
			String net = inputs[2];
			
			//uses one as the key so all values go to the same reducer
			context.write(new Text("1"), new Text(shopID+"\t"+floor+"\t"+net));
			
			
		}
	}

	//reducer to sort and limit the results
	public static class LimitReducer extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//value of K stored from the users arguments
			int k = Integer.parseInt(context.getConfiguration().get("limit"));
			
			//custom comparator to sort the results by highest total net paid and resolve ties by floor space
			Comparator<String> customComparator = new Comparator<String>() {
				@Override
				public int compare(String s1, String s2) {
					// Split the strings into their respective parts
					String[] parts1 = s1.split("\\s+");
					String[] parts2 = s2.split("\\s+");
	
					// Parse the second and third elements as doubles
					double net1 = Double.parseDouble(parts1[2]);
					double floor1 = Double.parseDouble(parts1[1]);
					double net2 = Double.parseDouble(parts2[2]);
					double floor2 = Double.parseDouble(parts2[1]);
	
					// Compare the second elements first, breaking ties with the third elements
					if (net1 < net2) {
						return -1;
					} else if (net1 > net2) {
						return 1;
					} else {
						return Double.compare(floor1, floor2);
					}
				}
			};
			
			
			// Add each result to arraylist
			ArrayList<String> resultArrayList = new ArrayList<>();

			for(Text value: values){
				resultArrayList.add(value.toString());
				
			}
			
			// Sort results according to comparator
			Collections.sort(resultArrayList, customComparator);
			
			// Write out results from end of arraylist up to limit k
			int lim = 0;
			for (int i = resultArrayList.size() - 1; i >= 0; i --) {
				if (lim == k) {
					break;
				}
				String element = resultArrayList.get(i);
				context.write(new Text(element), null);
				lim ++;
			}
			
	
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("startDate", args[1]);
		conf.set("endDate", args[2]);
		
		//setting path for the intermediate file
		String job1Output = "output/TempOutput";
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(job1Output);
		//deletes the file if it already exists
		fs.delete(path, true);

		//setting the parameters for the first job
		Job job = Job.getInstance(conf, "Sum_net_paid");
		job.addCacheFile(new Path(args[4]).toUri());
		job.setJarByClass(Q2Memory.class);
		job.setMapperClass(NetSumMapper.class);
		job.setReducerClass(NetSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//input path set to the path given by the user
		FileInputFormat.addInputPath(job, new Path(args[3]));
		//output path set to the path of the intermediate file
		FileOutputFormat.setOutputPath(job, new Path(job1Output));
		//job1 waits for completion before starting job2
		job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		conf2.set("limit", args[0]);

		//setting the parameters for the second job
		Job job2 = Job.getInstance(conf2, "Limit_and_sort");
		job2.setJarByClass(Q2Memory.class);
		job2.setMapperClass(LimitMapper.class);
		job2.setReducerClass(LimitReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		//input path set to the path of the intermediate file
		FileInputFormat.addInputPath(job2, new Path(job1Output));
		//output path set to the path given by the user
		FileOutputFormat.setOutputPath(job2, new Path(args[5]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}

}