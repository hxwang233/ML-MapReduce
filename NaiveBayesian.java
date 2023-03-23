

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NaiveBayesian {

	private static int labelIndex = -1;
	private static int total = 0;
	private static String trainSetPath = null;
	private static Map<Integer,Integer> map1 = new HashMap<>();
	private static Map<Integer,Map<Integer, Map<Double, Integer>>> map2 = new HashMap<>();
	
	// map2 <标签, <属性, <取值, 个数>> 
	public static void main(String[] args) throws Exception {
		Configuration configuration=new Configuration();
		configuration.set("fs.defaultFS", "hdfs://localhost:9000");
		if (args.length!=4) {
			System.err.println("Usage: NaiveBayesian <trainSet> <testSet> <output> <labelIndex> ");
			System.exit(1); //非正常退出
		}
		trainSetPath = args[0];
		labelIndex = Integer.parseInt(args[3]);
		Job job =Job.getInstance(configuration,"NaiveBayesian");
		job.setJarByClass(NaiveBayesian.class);		
		job.setMapperClass(NBMap.class);
		job.setReducerClass(NBReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);	
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}

	public static class NBMap extends Mapper<Object, Text, Text, IntWritable>{
		
		@Override
		/* Map之前将文件仅读入一次内存, 构造统计表 */
		public void setup(Context context) throws IOException{  
			//Configuration conf = context.getConfiguration();
			FileSystem fs = null;
			try {
				fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
			} catch (IOException | URISyntaxException e) {
				e.printStackTrace();
			}
			FSDataInputStream fis = fs.open(new Path("hdfs://localhost:9000" + trainSetPath));
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line = br.readLine();
			while (line != null) {
				total += 1;
				String [] tokens=line.split(",");
				int label = Integer.parseInt(tokens[labelIndex]);
				if (!map1.containsKey(label)) {
					map1.put(label, 0);
					map2.put(label, new HashMap<>());
				}
				map1.put(label, map1.get(label)+1);
				//map2 <标签, <属性, <取值, 个数>>
				for (int i = 0; i < tokens.length; i++) {
					if (i != labelIndex) {
						if (!map2.get(label).containsKey(i)) {
							Map<Integer, Map<Double, Integer>> map = map2.get(label);
							map.put(i, new HashMap<>());
							map2.put(label, map);
						}
						if (!map2.get(label).get(i).containsKey(Double.parseDouble(tokens[i]))) {
							Map<Integer, Map<Double, Integer>> m1 = map2.get(label);
							Map<Double, Integer> m2 = m1.get(i);
							m2.put(Double.parseDouble(tokens[i]), 0);
							m1.put(i, m2);
							map2.put(label, m1);
						}
						Map<Integer, Map<Double, Integer>> m1 = map2.get(label);
						Map<Double, Integer> m2 = m1.get(i);
						int count = m2.get(Double.parseDouble(tokens[i])) + 1;
						m2.put(Double.parseDouble(tokens[i]), count);
						m1.put(i, m2);
						map2.put(label, m1);																
					}
				}
				line = br.readLine();
			}
		//	br.close();
		//	fis.close();
		//	fs.close();
		}
		
		
		@Override
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
		  //Data data = new Data();		
			String line = value.toString();
			String [] tokens = line.split(",");
			int label  = Integer.MIN_VALUE;
			double max = Integer.MIN_VALUE;
			for (Integer k1: map1.keySet()) {
				double px = 1;
				Map<Integer, Map<Double, Integer>> attributes = map2.get(k1); 	
				System.out.println(attributes.toString());
				for (int k2 = 0; k2 < tokens.length; k2++) {
					if (k2 != labelIndex) {
						Map<Double, Integer> values = attributes.get(k2);
						double dik = values.get(Double.parseDouble(tokens[k2]));
						px *= dik / map1.get(k1);
					}
				}
				double pci = Double.valueOf(map1.get(k1)) / total;
				if (px * pci > max) {
					max = px * pci;
					label = k1;
				}
			}			
			context.write(new Text(value.toString()),new IntWritable(label));  // (data, label)	
		}
	}
	
	public static class NBReduce extends Reducer<Text, IntWritable, Text, IntWritable>{	
		@Override
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			for(IntWritable v : values) {
				context.write(new Text(""), v);
			}
		}
	}

}
