
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

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

public class KNN {
	
	private static int labelIndex = -1;
	private static int K = -1;
	private static ArrayList<Data> datum = new ArrayList<Data>();
	private static Utils utils = new Utils();
	private static String trainSetPath = null;
	
	public static void main(String[] args) throws Exception {
		Configuration configuration=new Configuration();
		configuration.set("fs.defaultFS", "hdfs://localhost:9000");
		if (args.length!=5) {
			System.err.println("Usage: KNN <trainSet> <testSet> <output> <labelIndex> <K>");
			System.exit(1); //非正常退出
		}
		trainSetPath = args[0];
		labelIndex = Integer.parseInt(args[3]);
		K = Integer.parseInt(args[4]);
		Job job =Job.getInstance(configuration,"KNN");
		job.setJarByClass(KNN.class);		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);	
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
	
	public static class Map extends Mapper<Object, Text, Text, IntWritable>{
		
		@Override
		/* Map之前将文件仅读入一次内存, 并存入 datum列表用于计算距离 */
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
				Data data = new Data();
				String [] tokens=line.split(",");
				for (int i = 0; i < tokens.length; i++) {
					if (i == labelIndex) {
						int label = Integer.parseInt(tokens[i]);
						data.setLabel(label);
					}else {
						data.addFeature(Integer.parseInt(tokens[i]));
					}
				}
				datum.add(data);
				line = br.readLine();
			}
		//	br.close();
		//	fis.close();
		//	fs.close();
		}
		
		@Override
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			Data data = new Data();
			Double[] disArray = new Double[K];  // k个距离
			int[] labelArray  = new int[K];     // 对应的标签label
			for (int i = 0; i < K; i++) {
				disArray[i] = Double.MAX_VALUE;
			}			
			String line=value.toString();
			String [] tokens=line.split(",");
			for (int i = 0; i < tokens.length; i++) {
				if (i == labelIndex) {
					int label = Integer.parseInt(tokens[i]);
					data.setLabel(label);
				}else {
					data.addFeature(Integer.parseInt(tokens[i]));
				}
			}
			for(Data d: datum) {  // 遍历datum
				double dis = utils.dis(d, data);
				int index = utils.getMaxIndex(disArray);
				if (dis < disArray[index]) {
					disArray[index] = dis;
					labelArray[index] = d.getLabel();
				}
			}
			for (int i = 0; i < K; i++) {
				context.write(new Text(value.toString()),new IntWritable(labelArray[i]));  // (data, label)
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{	
		@Override
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int []temp = new int [2];  // 计数器 （label, count）  MJRTY算法
			temp[0] = 0;
			temp[1] = 0;
			for(IntWritable num : values) {
				int label = num.get();
				if (label == temp[0]) {
					temp[1] += 1;
				}else {
					temp[1] -= 1;
					if (temp[1] < 0) {
						temp[0] = label;
						temp[1] = -temp[1];
					}
				}
			}
			context.write(new Text(""), new IntWritable(temp[0]));
		}
	}

}
