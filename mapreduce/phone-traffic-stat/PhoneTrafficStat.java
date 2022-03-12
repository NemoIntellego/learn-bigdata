package site.nemo.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

public class PhoneTrafficStat {

    public static class TrafficMapper
            extends Mapper<Object, Text, Text, TrafficEntity>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split("\t");
            if (arr.length < 10) {
                return;
            }
            String phone = arr[1];
            try {
                long upload = Long.parseLong(arr[8]);
                long download = Long.parseLong(arr[9]);
                context.write(new Text(phone), new TrafficEntity(upload, download, upload + download));
            } catch (NumberFormatException e) {
                System.out.println("parseLong failed:" + e.getMessage());
            }
        }
    }

    public static class TrafficReducer
            extends Reducer<Text, TrafficEntity, Text, TrafficEntity> {

        public void reduce(Text key, Iterable<TrafficEntity> values,
                           Context context) throws IOException, InterruptedException {
            long totalUpload = 0;
            long totalDownload = 0;
            long sum = 0;
            for (TrafficEntity val : values) {
                totalUpload += val.getUploadTraffic();
                totalDownload += val.getDownloadTraffic();
                sum += val.getTotalTraffic();
            }

            context.write(key, new TrafficEntity(totalUpload, totalDownload, sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: PhoneTrafficStat <in> <out>");
            System.exit(2);
        }
        System.out.println("otherArgs: " + Arrays.toString(otherArgs));

        Job job = Job.getInstance(conf, "PhoneTrafficStat");
        job.setJarByClass(PhoneTrafficStat.class);
        job.setMapperClass(TrafficMapper.class);
        job.setReducerClass(TrafficReducer.class);
        job.setCombinerClass(TrafficReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TrafficEntity.class);

        job.setNumReduceTasks(1);

        JobConf jobConf = new JobConf(job.getConfiguration());

        FileInputFormat.addInputPath(jobConf, new Path(otherArgs[otherArgs.length - 2]));
        FileOutputFormat.setOutputPath(jobConf, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}