import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.StringTokenizer;

import javax.management.loading.MLetContent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.util.Map;
import java.util.TreeMap;
import java.util.NavigableMap;

public class Q2Reduced {

  static Integer startDate = 0;
  static Integer endDate = 0;
  static Integer limit = 0;

  //mapper for the store_sales table, sends the netPaids as values to a reducer with the storeID as it's key
  public static class NetSumMapper extends Mapper<Object, Text, Text, Text>{

	private Text net = new Text();
	private Text sk = new Text();


	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
      	StringTokenizer itr = new StringTokenizer(value.toString(), "|", true);
		//start and end dates gotten from the user
		int start = Integer.parseInt(context.getConfiguration().get("startDate"));
		int end = Integer.parseInt(context.getConfiguration().get("endDate"));

		Integer itemSoldDate = null;
		String storeID = null;
		String netPaid = null;

		int i = 1;
		//iterates through the lines of the input file
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

		//does not write anything if any of the required attributes are empty
		if (itemSoldDate != null && storeID != null && netPaid != null) {

			if (itemSoldDate >= start && itemSoldDate <= end && start <= end) {
				//key set as the storeID
				sk.set(storeID);
				//value set as ss (to show its from the table store_sales) and the netPaid
				net.set("ss,"+netPaid);
				context.write(sk, net);

			}

		}
		
	}
  }

  //mapper for the store table. The value is the floor space and the key for the reducers are the storeIDs
  public static class FloorSpaceMapper extends Mapper<Object, Text, Text, Text>{

	private Text floorSpace = new Text();
	private Text sk = new Text();


	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
      	StringTokenizer itr = new StringTokenizer(value.toString(), "|", true);

		String storeID = null;
		String floor = null;

		int i = 1;

		//iterates through the records of the store table
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken();
			//increments i when a pipe is found to show what attribute is currently being read
			if (token.equals("|")) {
				i ++;
			}
			else {
				//stores the required attributes
				if (i == 1) {
					storeID = token;
				}
				else if (i == 8) {
					floor = token;
				}
            }
		}


		if (storeID != null && floor != null) {
			//sets the key as the storeID
            sk.set(storeID);
			//sets the value as s (to show it is from the store table), and the floorspace
            floorSpace.set("s,"+floor);
            context.write(sk, floorSpace);
		}
		
	}
  }

  //reducer taking inputs from two mappers, one using the store_sales table and one using the store table
  public static class FloorNetReducer extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String floorSpace = "";
        double totalNet = 0.0;

		//iterates through each value given by the two mappers
		for (Text val: values) {
			String[] parts = val.toString().split(",");
			//checks if the value has come from the store table or store_sales
            if(parts[0].equals("ss")){
				//increments the total net paid when the value is from the store_sales table
                totalNet += Double.parseDouble(parts[1]);
            }
            else{
				//stores the floor spave when the value is from the store table
                floorSpace = parts[1];
            }
		}

		//makes sure the floor space is not a null value
        if(!floorSpace.equals("")){
            String netResults = String.format("%.2f", totalNet);
			//writes the results to the intermediate file
            context.write(key, new Text(floorSpace+"\t"+netResults));
        }

	}
  }

  //mapper to send all the values to a single reducer
  public static class LimitMapper extends Mapper<Object, Text, Text, Text>{


	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		

		String[] inputs = value.toString().split("\\s+");

		String shopID = inputs[0];
		String floor = inputs[1];
        String net = inputs[2];

		//uses 1 as the key so all values go to the same reducer
		context.write(new Text("1"), new Text(shopID+"\t"+floor+"\t"+net));
		
		
	}
  }

  //reducer to order and limit the results
  public static class LimitReducer extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		//value of K stored from the arguments 
		int k = Integer.parseInt(context.getConfiguration().get("limit"));
		//comparator used to order by highest netPaid total and resolve ties by the floorspace
		Comparator<String> customComparator = new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                // Split the strings into their respective parts
                String[] parts1 = s1.split("\\s+");
                String[] parts2 = s2.split("\\s+");

                // Parse the second and third elements as doubles
                double net1 = Double.parseDouble(parts1[0]);
                double floor1 = Double.parseDouble(parts1[1]);
                double net2 = Double.parseDouble(parts2[0]);
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

		//treeMap for sorting the results
		TreeMap<String,String> sorted = new TreeMap<String,String>(customComparator);

		//iterates through each value 
		for(Text value: values){
			String[] inputs = value.toString().split("\\s+");
			//adds the results to the treemap for sorting
			sorted.put(inputs[2]+"\t"+inputs[1],inputs[0]);
			
		}

		//reverses the order of the treeMap
        NavigableMap<String,String> reversed = sorted.descendingMap();

		int i = 0;
		//iterates through each value of the treeMap
		for(Map.Entry element: reversed.entrySet()){
			//breaks when the limit is reached
			if(i==k){
				break;
			}
			//writes the results to the output file
			context.write(new Text(element.getValue().toString()), new Text(element.getKey().toString()));
			i++;
		}

		

	}
  }



  public static void main(String[] args) throws Exception {


	Configuration conf = new Configuration();

	conf.set("startDate", args[1]);
	conf.set("endDate", args[2]);

	//setting path for the intermediate file
	String job1Output = "output/Q2JobRed";
	FileSystem fs = FileSystem.get(conf);
	Path path = new Path(job1Output);
	//deletes the file if it exists already
	fs.delete(path, true);


	//setting the parameters for the first job
	Job job = Job.getInstance(conf, "ReducedSide_Job1");
	job.setJarByClass(Q2Reduced.class);
	job.setReducerClass(FloorNetReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	//multiple input paths must be used for the two tables, both are given by the user
	MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, NetSumMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[4]), TextInputFormat.class, FloorSpaceMapper.class);
	//output path set as the intermediate path
    FileOutputFormat.setOutputPath(job, new Path(job1Output));
	//job1 waits for completion before starting job2
	job.waitForCompletion(true);

	Configuration conf2 = new Configuration();
	conf2.set("limit", args[0]);

	//setting the parameters for job2
	Job job2 = Job.getInstance(conf2, "ReducedSide_sort");
	job2.setJarByClass(Q2Reduced.class);
	job2.setMapperClass(LimitMapper.class);
	job2.setReducerClass(LimitReducer.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
	//input path set as the path to the intermediate file
	FileInputFormat.addInputPath(job2, new Path(job1Output));
	//output path set to the path given by the user
	FileOutputFormat.setOutputPath(job2, new Path(args[5]));

	
	System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}