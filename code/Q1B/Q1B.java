import java.io.IOException;
import java.util.StringTokenizer;

import javax.management.loading.MLetContent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
import java.util.Comparator;

public class Q1B {

  static Integer startDate = 0;
  static Integer endDate = 0;

  //key for the treeMaps used in a reducer
  public static class TreeKey {
	public String str;
	public Integer quantity;

	public TreeKey(String str, Integer quantity){
		this.str = str;
		this.quantity = quantity;
	}

	@Override
	public String toString(){
		return this.quantity.toString();
	}
  }

  	//used to sort the keys in the tree maps by the first value then by the second
  public static class TreeOrdering implements Comparator<TreeKey>{
	@Override
	public int compare(TreeKey k1, TreeKey k2){
		if (k1.quantity == k2.quantity) {
			return k1.str.compareTo(k2.str);
		}
		return k1.quantity.compareTo(k2.quantity);
	}
  }

  //method to reverse the ordering of the tree map
  public static class ReverseTreeOrdering implements Comparator<TreeKey>{
	@Override
	public int compare(TreeKey k1, TreeKey k2){
		if (k1.quantity == k2.quantity) {
			return k1.str.compareTo(k2.str);
		}
		return k2.quantity.compareTo(k1.quantity);
	}
  }

  //mapper to pass the quantities and store IDs to reducers with the key as the itemID and storeID 
  public static class QuantitySumMapper extends Mapper<Object, Text, Text, Text>{

	private final static Text storeQuant = new Text();
	private Text sk = new Text();


	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
        StringTokenizer itr = new StringTokenizer(value.toString(), "|", true);

		//storing the start and end dates given by the user
		int start = Integer.parseInt(context.getConfiguration().get("startDate"));
		int end = Integer.parseInt(context.getConfiguration().get("endDate"));

		Integer itemSoldDate = null;
		String storeID = null;
        String itemID = null;
		String quantity = null;

		int i = 1;

		//iterates through each line of the input data
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken();
			//increments i when a pipe is found to show what attribute is currently being read
			if (token.equals("|")) {
				i ++;
			}
			else {
				//stores the needed attributes
				if (i == 1) {
					itemSoldDate = Integer.parseInt(token);
				}
				else if (i == 8) {
					storeID = token;
				}
                else if (i == 3) {
					itemID = token;
				}
				else if (i == 11) {
					quantity = token;
				}
			}
		}

		//makes sure none of the required attributes are empty
		if (itemSoldDate != null && storeID != null && itemID != null && quantity != null) {
			//makes sure the item sold date is between the given dates
			if (itemSoldDate >= start && itemSoldDate <= end && start <= end) {

				String itemStore = itemID + "," + storeID.toString();
                String storeQuantStr = storeID.toString() + "	" + quantity.toString();
				
				//sets the key as the itemID and storeID
				sk.set(itemStore);
				//value is set as the storeID and the item quantity
				storeQuant.set(storeQuantStr);
				context.write(sk, storeQuant);

			}

		}
		
	}

  }

  //reducer sums the quantities for each item and store
  public static class QuantitySumReducer extends Reducer<Text,Text,Text,Text> {

	private Text result = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Integer sum = 0;
        String storeID = "";

		//iterates through the inputted values
		for (Text val : values) {
            String[] inputs = val.toString().split("\\s+");

            // Obtain the integer quantity which will be used to sum
			Integer quantity = Integer.parseInt(inputs[1]);
			// Obtain the integer for the store id
            storeID = inputs[0];
			//sums the quantity
			sum += quantity;
		}

        //writes the itemID, storeID and summed quantity to an intermediate file      
        String str = storeID + "	" + sum.toString();
        result.set(str);
        context.write(key, result);

	}
  }

  //mapper to take the itemID, storeID and summed quantity, and pass it to a reducer with the storeID as the key
  public static class StoreQuantityMapper extends Mapper<Object, Text, Text, Text>{

	private final static Text itemQuant = new Text();
	private Text sk = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] inputs = value.toString().split("\\s+");

		// value -->    itemID,storeID    storeID    quantity

		String itemID = inputs[0].split(",")[0];	
		String storeID = inputs[1];

		String quantity = inputs[2];
		String str = quantity + "	" + itemID;

		// Passing to Reducer     -->    Key: StoreID     Value: Quantity    ItemID
		sk.set(storeID);
		itemQuant.set(str);

		context.write(sk, itemQuant);
	}
  }

  //reducer to limit each store to ther least M items 
  public static class StoreQuantityReducer extends Reducer<Text,Text,Text,Text> {

	private Text result = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Integer sum = 0;
		String items = "";

		// tree map to sort the items by their quantity sold
		TreeMap<TreeKey,String> sorted = new TreeMap<TreeKey,String>(new TreeOrdering());

		//iterating through the inputted values
		for (Text val : values) {
			String[] inputs = val.toString().split("\\s+");

			String itemID = inputs[1];
			Integer quantity = Integer.parseInt(inputs[0].toString());
			//tree key used so items with the same quantities will not be overwritten
			TreeKey myKey = new TreeKey(itemID, quantity);
			//adding each item to the treemap
			sorted.put(myKey, itemID);
			//incrementing the sum to find the total quantity for a store
			sum += quantity;
		}

		// Get M Least Selling Items

		//gets the value M given by the user
		int m = Integer.parseInt(context.getConfiguration().get("M"));
		int i = 0;
		for(Map.Entry element: sorted.entrySet()){
			//stops writing when the M least selling items have been written 
			if(i==m) { break; }
			//concatenates the least selling M items together with their quantity first, then itemID
			items = items + (String)element.getValue() + "," + element.getKey().toString() + "|";
			i++;
		}

		String str = sum.toString() + "	   " + items;
        result.set(str);

		// Reducer Returns -->    Key: StoreID    Value: QuantitySum    ItemID1,ItemQuantity1|ItemID2,ItemQuantity2|...|ItemIDM,ItemQuantityM|
        context.write(key, result);
	}
  }

  //mapper to pass all inputted stores to the same reducer
  public static class OrderLimitMapper extends Mapper<Object, Text, Text, Text>{

	private final static Text data = new Text();
	private Text one = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] inputs = value.toString().split("\\s+");

		String str = inputs[0] + "    " + inputs[1] + "    " + inputs[2];
	
		//sets the key as one so that all values are passed to the same reducer
		one.set("1");
		data.set(str);

		context.write(one, data);
		
	}
  }

  //reducer to order the results and limit them to the top N stores
  public static class OrderLimitReducer extends Reducer<Text,Text,Text,Text> {

	private Text sk = new Text();
	private Text itemID_quant = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//stores the value N given by the user
		int n = Integer.parseInt(context.getConfiguration().get("N"));
		//tree map to sort the stores, also uses a reverse function so stores are sorted by descending order
		TreeMap<TreeKey,String> sorted = new TreeMap<TreeKey,String>(new ReverseTreeOrdering());

		// Sort Stores by QuantitySum
		for (Text val : values) {
			String value = val.toString();
			String[] inputs = value.split("\\s+");

			Integer quantity = Integer.parseInt(inputs[1].toString());
			//treeKey used to deal with stores which have the same total quantity of items 
			TreeKey myKey = new TreeKey(inputs[0], quantity);

			// value -->    StoreID    QuantitySum    ItemID1,ItemQuantity1|...|ItemIDM,ItemQuantityM|
			sorted.put(myKey, value);
		}

		// Isolate Top N Stores
		int i = 0;
		for(Map.Entry element: sorted.entrySet()){
			//stops writing once the top N stores have been written
			if(i==n){ break; }

			String value = (String)element.getValue();
			String[] inputs = value.split("\\s+");
			String storeID = "ss_store_sk_" + inputs[0];
			String[] items = inputs[2].split("\\|");

			// Iterate through items and context.write(StoreID    ItemID    ItemQuantity)
			for (String item:items){
				String[] parts = item.split(",");

				String itemID = parts[0];
				String quantity = parts[1];

				String str = "ss_item_sk_" + itemID + "    " + quantity;

				sk.set(storeID);
				itemID_quant.set(str);
				context.write(sk, itemID_quant);
			}

			i++;
		}

		

	}
  }
  public static void main(String[] args) throws Exception {

	Configuration conf = new Configuration();

	conf.set("startDate", args[2]);
	conf.set("endDate", args[3]);

	//defining a path for the first intermediate file
	String job1Output = "output/Q1BJob1";
	FileSystem fs = FileSystem.get(conf);
	Path path = new Path(job1Output);
	//deleting the file if it already exists
	fs.delete(path, true);

	//setting the parameters for the first job
	Job job = Job.getInstance(conf, "Sum_item_quantity");
	job.setJarByClass(Q1B.class);
	job.setMapperClass(QuantitySumMapper.class);
	job.setCombinerClass(QuantitySumReducer.class);
	job.setReducerClass(QuantitySumReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	//input path set to the path given by the user
	FileInputFormat.addInputPath(job, new Path(args[4]));
	//output path set as the first intermediate file
	FileOutputFormat.setOutputPath(job, new Path(job1Output));
	//job1 waits until it is completed to start the next job
	job.waitForCompletion(true);

	Configuration conf2 = new Configuration();

	conf2.set("M", args[0]);

	//defining a path for the second intermediate file
	String job2Output = "output/Q1BJob2";
	path = new Path(job2Output);
	//deleting the file if it already exists
	fs.delete(path, true);

	//setting the paramters for the second job
	Job job2 = Job.getInstance(conf2, "Sum_store_quantity");
	job2.setJarByClass(Q1B.class);
	job2.setMapperClass(StoreQuantityMapper.class);
	job2.setReducerClass(StoreQuantityReducer.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
	//input path set the first intermediate file
	FileInputFormat.addInputPath(job2, new Path(job1Output));
	//output path set as the second intermediate file
	FileOutputFormat.setOutputPath(job2, new Path(job2Output));
	//job2 waits until completion before starting job3
	job2.waitForCompletion(true);

	Configuration conf3 = new Configuration();

	conf3.set("N", args[1]);

	//setting the parameters for the final job
	Job job3 = Job.getInstance(conf3, "Order_limit_stores");
	job3.setJarByClass(Q1B.class);
	job3.setMapperClass(OrderLimitMapper.class);
	job3.setReducerClass(OrderLimitReducer.class);
	job3.setOutputKeyClass(Text.class);
	job3.setOutputValueClass(Text.class);
	//input file path set as the second intermediate file
	FileInputFormat.addInputPath(job3, new Path(job2Output));
	//output file path given by the user
	FileOutputFormat.setOutputPath(job3, new Path(args[5]));

	System.exit(job3.waitForCompletion(true) ? 0 : 1);
  }
}

// *** MAP REDUCE OVERVIEW ***
// Map Reduce 1
//     - Obtain ItemID, StoreID, TotalItemQuantity
// Map Reduce 2
//     - Obtain StoreID, TotalItemQuantitySum, Tuple([ItemID1, ItemQuantity1], [ItemID2, ItemQuantity2], ...)
//     - Limit and Order Items
// Map Reduce 3
//     - Obtain ItemID, StoreID, TotalItemQuantity
//     - Limit and Order Stores