import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.StringTokenizer;

import javax.management.loading.MLetContent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.util.Map;
import java.util.TreeMap;


public class Q1A {

  static Integer startDate = 0;
  static Integer endDate = 0;
  static Integer limit = 0;

  //key for the treeMaps used in a reducer
	public static class TreeKey {
		public String str;
		public Double num;
	
		public TreeKey(String str, Double num){
			this.str = str;
			this.num = num;
		}
	
		@Override
		public String toString(){
			return this.num.toString();
		}
	}
	//used to sort the keys in the tree maps by the first value then by the second
	public static class TreeOrdering implements Comparator<TreeKey>{
		@Override
		public int compare(TreeKey k1, TreeKey k2){
			if (k1.num == k2.num) {
				return k1.str.compareTo(k2.str);
			}
			return k1.num.compareTo(k2.num);
		}
	}
//mapper to read in the data from the 40G data set and pass the relevant attributes to reducers
//with the key as the store ID
  public static class NetSumMapper extends Mapper<Object, Text, Text, DoubleWritable>{

	private final static DoubleWritable net = new DoubleWritable();
	private Text sk = new Text();


	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
      	StringTokenizer itr = new StringTokenizer(value.toString(), "|", true);
		//holds the start and end dates give by the user
		int start = Integer.parseInt(context.getConfiguration().get("startDate"));
		int end = Integer.parseInt(context.getConfiguration().get("endDate"));

		Integer itemSoldDate = null;
		String storeID = null;
		String netPaid = null;

		int i = 1;
		//iterates through each line of the input data 
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken();
			//increments i when a pipe is found, showing it is pointing to the next attribute
			if (token.equals("|")) {
				i ++;
			}
			else {
				if (i == 1) {
					//stores the date the item was sold
					itemSoldDate = Integer.parseInt(token);
				}
				else if (i == 8) {
					//stores the store ID
					storeID = token;
				}
				else if (i == 21) {
					//stores the net paid
					netPaid = token;
				}
			}
		}

		//does not write anything if any of the needed attributes are null
		if (itemSoldDate != null && storeID != null && netPaid != null) {
			//checks if the item sold date is within the given dates 
			if (itemSoldDate >= start && itemSoldDate <= end && start <= end) {
				
				sk.set(storeID);
				net.set(Double.parseDouble(netPaid));
				//reducers have the key of the storeID so all items for each store
				//go to separate reducers
				context.write(sk, net);

			}

		}
		
	}
  }

//reducer to sum the total net paid of items for each store
  public static class NetSumReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

	private DoubleWritable result = new DoubleWritable();

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

		Double sum = 0.0;
		//iterates through all values given to the reducer
		for (DoubleWritable val : values) {
			Double net = val.get();
			//increments the sum by each net paid
			sum += net;
		}

		result.set(sum);

		//writes the result to an intermediate file
		context.write(key, result);

	}
  }

  //mapper to take the store IDs and total net paid and pass them all to the same reducer
  public static class LimitMapper extends Mapper<Object, Text, Text, Text>{

	private final static Text netSums = new Text();
	private Text one = new Text();


	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		

		String[] inputs = value.toString().split("\\s+");

		String shopID = inputs[0];
		String netSum = inputs[1];
		String result = shopID+","+netSum;

		//setting the key to one so that all data goes to the same reducer
		one.set("1");
		netSums.set(result);
		
		context.write(one, netSums);
		
		
	}
  }

  //reducer to sort the data and limit it to the bottom K results
  public static class LimitReducer extends Reducer<Text,Text,Text,Text> {

	private Text sk = new Text();
	private Text net = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//getting the value of K given by the user
		int k = Integer.parseInt(context.getConfiguration().get("limit"));
		
		//tree map to sort the results
		TreeMap<TreeKey,String> sorted = new TreeMap<TreeKey,String>(new TreeOrdering());

		for(Text value: values){

			String[] inputs = value.toString().split(",");
			//creates a key for the tree map with the storeID and total net paid (to manage when two stores have the same total net paid)
			TreeKey myKey = new TreeKey(inputs[0], Double.parseDouble(inputs[1]));
			//adds each store to the tree map
			sorted.put(myKey,inputs[0]);
			
		}

		int i = 0;
		//iterates through the tree map
		for(Map.Entry element: sorted.entrySet()){
			//stops when the limit is reached
			if(i==k){
				break;
			}
			//writes the data to the output file
			sk.set("ss_store_sk_"+(String)element.getValue());
			Double num_double = Double.parseDouble(element.getKey().toString());
			net.set(String.format("%.2f", num_double));
			context.write(sk, net);
			i++;
		}

		

	}
  }



  public static void main(String[] args) throws Exception {


	Configuration conf = new Configuration();
	//user inputs
	conf.set("startDate", args[1]);
	conf.set("endDate", args[2]);

	//defining a file path for the intermediate output file
	String job1Output = "output/Q1Job";
	FileSystem fs = FileSystem.get(conf);
	Path path = new Path(job1Output);
	//deletes the intermediate file if it already exists
	fs.delete(path, true);


	//defining the first job
	Job job = Job.getInstance(conf, "Sum_all_net_paid");
	job.setJarByClass(Q1A.class);
	job.setMapperClass(NetSumMapper.class);
	job.setCombinerClass(NetSumReducer.class);
	job.setReducerClass(NetSumReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);
	//input file given by the user
	FileInputFormat.addInputPath(job, new Path(args[3]));
	//output file set as the intermediate file
	FileOutputFormat.setOutputPath(job, new Path(job1Output));
	//job waits until it is finished before starting the next job
	job.waitForCompletion(true);

	Configuration conf2 = new Configuration();
	//user input
	conf2.set("limit", args[0]);

	//setting the parameters for the second job
	Job job2 = Job.getInstance(conf2, "Limit_and_sort");
	job2.setJarByClass(Q1A.class);
	job2.setMapperClass(LimitMapper.class);
	job2.setReducerClass(LimitReducer.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
	//input file as the intermediate file
	FileInputFormat.addInputPath(job2, new Path(job1Output));
	//output file given by the user
	FileOutputFormat.setOutputPath(job2, new Path(args[4]));

	
	System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
