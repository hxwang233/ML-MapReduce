package lab4;

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

public class Kmeans {
	
	private static int count = 1;  //迭代次数
	private static int K = -1; //聚类数
	private static String dataPath = null;  //输入
	private static String outPath = null;  //输出
	private static Utils utils = new Utils();
	private static ArrayList<Data> centers = new ArrayList<>();  //旧中心
	private static ArrayList<Data> newCenters = null; //新中心
	
    public static void run(String dataPath,String outPath,boolean flag) throws Exception{
		Configuration configuration=new Configuration();
		configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        Job job =Job.getInstance(configuration,"Kmeans");
		job.setJarByClass(Kmeans.class);		
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);	
        if(flag){ //结束迭代前都需执行reduce
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
        } 
        if (count!=1){
        	Path path = new Path(outPath);
        	FileSystem hdfs = path.getFileSystem(configuration);
            hdfs.delete(path ,true);
        }
        FileInputFormat.addInputPath(job, new Path(dataPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        count += 1;
        job.waitForCompletion(true);
    }
	
	public static void main(String[] args) throws Exception {
		Configuration configuration=new Configuration();
		configuration.set("fs.defaultFS", "hdfs://localhost:9000");
		if (args.length!=3) {
			System.err.println("Usage: Kmeans <data> <output> <K>");
			System.exit(1); //非正常退出
		}
		dataPath = args[0];
		outPath = args[1];
		K = Integer.parseInt(args[2]);   
        while(true){
            run(dataPath,outPath,true); //输入、输出路径、true-迭代未结束  false-迭代结束  
            if(utils.compareCenters(centers, newCenters)){
            	run(dataPath,outPath,false);
                break;
            }
        }
	}	
	
	public static class Map extends Mapper<Object, Text, IntWritable, Text>{
		
		@Override
		/* Map之前将文件仅读入一次内存, 随机选择质心*/
		public void setup(Context context) throws IOException{  
			if (newCenters != null) {
				centers = newCenters; //改变引用到新的中心所指的ArrayList
				return;
			}
			FileSystem fs = null;
			try {
				fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
			} catch (IOException | URISyntaxException e) {
				e.printStackTrace();
			}
			FSDataInputStream fis = fs.open(new Path("hdfs://localhost:9000" + dataPath));
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line = br.readLine();
			double[] kf = null;
			int flag = 1;
			while (line != null) { //按列求和
				String [] tokens=line.split(",");
				if (flag==1) {
					kf = new double[tokens.length];
				}
				for (int i = 0; i < tokens.length; i++) {
					kf[i] += Double.parseDouble(tokens[i]);
				}
				flag ++;
				line = br.readLine();
			}
			for (int j = 1; j <= K; j++) {
				Data data = new Data();
				for (int i = 0; i < kf.length; i++) {
					data.addFeature(j*kf[i]/(K+1)); //分位
				}
				centers.add(data);
			}

		}
		
		@Override
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			Data data = new Data();
			Double[] disArray = new Double[K];  // k个距离
			for (int i = 0; i < K; i++) { //初始化距离
				disArray[i] = Double.MAX_VALUE;
			}			
			String line=value.toString();
			String [] tokens=line.split(",");
			for (int i = 0; i < tokens.length; i++) { //数据处理
				data.addFeature(Double.parseDouble(tokens[i]));
			}
			for (int i = 0; i < centers.size(); i++) {
				double dis = utils.dis(centers.get(i), data); //计算距离
				disArray[i] = dis;
			}
			int index = utils.getMinIndex(disArray);  // 所属中心的下标
			context.write(new IntWritable(index), value);  // (index, data)
		}
	}
	
	public static class Reduce extends Reducer<IntWritable, Text, Text, IntWritable>{	
		@Override
		public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			newCenters = new ArrayList<Data>();  //新的中心
			ArrayList<Data> datum = new ArrayList<>();
			for(Text value: values) { //处理数据
				Data data = new Data();
				String line = value.toString();
				String [] tokens = line.split(",");
				for (int i = 0; i < tokens.length; i++) {
					data.addFeature(Double.parseDouble(tokens[i]));
				}
				datum.add(data);
			}
			Data center = utils.getMeans(datum);  //计算均值
			newCenters.add(center);
		}
	}

}
