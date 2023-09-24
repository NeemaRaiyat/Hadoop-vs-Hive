import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.StringTokenizer;

import javax.management.loading.MLetContent;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.fs.FileSystem;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;


// import java.math.Double;


public class Q1C {

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
			return k2.num.compareTo(k1.num);
		}
	}

//mapper to pass net paid tax values to reducers with the keys as the sold dates 
  public static class NetPaidTaxMapper extends Mapper<Object, Text, Text, DoubleWritable>{

	private final static DoubleWritable net = new DoubleWritable();
	private Text daykey = new Text();


	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
      	StringTokenizer itr = new StringTokenizer(value.toString(), "|", true);
		//gets the start and end dates from the users input
		int start = Integer.parseInt(context.getConfiguration().get("startDate"));
		int end = Integer.parseInt(context.getConfiguration().get("endDate"));

		String itemSoldDate = null;
		String netPaidTax = null;

		int i = 1;
		//iterates through each line of the input file
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken();
			//increments i when a pipe is found to show what attribute is currently being read
			if (token.equals("|")) {
				i ++;
			}
			//stores the required attributes 
			else {
				if (i == 1) {
					itemSoldDate = token;
				}
				else if (i == 22) {
					netPaidTax = token;
				}
			}
		}

		//does not write anything if the record has no itemsold date or netPaidTax
		if (itemSoldDate != null && netPaidTax != null) {

            int soldDateAsInt = Integer.parseInt(itemSoldDate);
			//ensures the item was sold inbetween the given date range
			if (soldDateAsInt >= start && soldDateAsInt <= end && start >= end) {
				
				daykey.set(itemSoldDate);
				net.set(Double.parseDouble(netPaidTax));
				//uses the date as a key so all ss_net_paid_inc_tax's from the same day go to the same reducer
				context.write(daykey, net);

			}
		}
	}
  }

  //reducer to sum  up the total net paid income tax for each day
  public static class NetPaidTaxReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

	private DoubleWritable result = new DoubleWritable();

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

		Double sum = 0.0;

		//iterates through the values from the input file
		for (DoubleWritable val : values) {
			Double net = val.get();
			//sums all ss_net_paid_inc_tax for the given day
			sum += net;
		}

		result.set(sum);
		context.write(key, result);

	}
  }

  //mapper to pass all values to a single reducer so they can be sorted
  public static class LimitMapper extends Mapper<Object, Text, Text, Text>{

	private final static Text netSums = new Text();
	private Text one = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] inputs = value.toString().split("\\s+");

		String day = inputs[0];
		String netSum = inputs[1];
		String result = day+","+netSum;

		one.set("1");
		netSums.set(result);
		//uses one as the key so all days and total tax values are sent to the same reducer
		context.write(one, netSums);
		
		
	}
  }

  //reducer orders the results and limits it by K 
  public static class LimitReducer extends Reducer<Text,Text,Text,Text> {

	private Text sk = new Text();
	private Text net = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		//stores K, given by the user 
		int k = Integer.parseInt(context.getConfiguration().get("limit"));

		//tree map used to sort the days in order of total net tax
		TreeMap<TreeKey,String> sorted = new TreeMap<TreeKey,String>(new TreeOrdering());

		for(Text value: values){

			String[] inputs = value.toString().split(",");
			//uses a tree key to deal with days with equal total net paid inc tax
			TreeKey myKey = new TreeKey(inputs[0], Double.parseDouble(inputs[1]));

			sorted.put(myKey, inputs[0]);
			
		}

		int i = 0;

		// NavigableMap<Double,String> reversed = sorted.descendingMap();

		//iterates through each value in the treeMap
		for(Map.Entry element: sorted.entrySet()){
			//stops writing to the output file when the given limit is reaches
			if(i==k){
				break;
			}
			Double num_double = Double.parseDouble(element.getKey().toString());
			sk.set("ss_sold_date_sk_"+(String)element.getValue());
			net.set(String.format("%.2f", num_double));
			//writes the results to the output file
			context.write(sk, net);
			i++;
		}

	}
  }



  public static void main(String[] args) throws Exception {


	Configuration conf = new Configuration();

	conf.set("startDate", args[1]);
	conf.set("endDate", args[2]);

	//setting the path for the first intermediate file
	String job1Output = "output/Q1CJob";
	FileSystem fs = FileSystem.get(conf);
	Path path = new Path(job1Output);
	//deletes the file if it already exists
	fs.delete(path, true);

	//setting the parameters for the first job
	Job job = Job.getInstance(conf, "Sum_all_net_paid");
	job.setJarByClass(Q1C.class);
	job.setMapperClass(NetPaidTaxMapper.class);
	job.setCombinerClass(NetPaidTaxReducer.class);
	job.setReducerClass(NetPaidTaxReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);
	//input path set as the path given by the user
	FileInputFormat.addInputPath(job, new Path(args[3]));
	//output path set as the interdiate file path
	FileOutputFormat.setOutputPath(job, new Path(job1Output));
	//job1 waits until completion to start job2
	job.waitForCompletion(true);

	Configuration conf2 = new Configuration();
	conf2.set("limit", args[0]);

	//setting the parameters for job2
	Job job2 = Job.getInstance(conf2, "Limit_and_sort");
	job2.setJarByClass(Q1C.class);
	job2.setMapperClass(LimitMapper.class);
	job2.setReducerClass(LimitReducer.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
	//input path set as the intermediate file's path
	FileInputFormat.addInputPath(job2, new Path(job1Output));
	//output path specified by the user
	FileOutputFormat.setOutputPath(job2, new Path(args[4]));

	
	System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}