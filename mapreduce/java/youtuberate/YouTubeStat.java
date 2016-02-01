package youtuberate;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class YouTubeStat {

    public class VideoStatMapper
            extends Mapper<LongWritable, Text, DoubleWritable, ArrayWritable>{

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            String[] statLine = line.split("\\t");
            String videoID = statLine[0];
            String rating = statLine[7];
            String[] array = {rating, videoID};

            DecimalFormat df = new DecimalFormat("#.00");
            DoubleWritable rate = new DoubleWritable(Double.valueOf(df.format(statLine[6])));

            context.write(rate, new ArrayWritable(array));

        }
    }

    public static class ArrayMultiOutputReducer
            extends Reducer<DoubleWritable, ArrayWritable, Text,IntWritable> {

        private MultipleOutputs<Text, IntWritable> multipleOutputs;
        private IntWritable result = new IntWritable();

        public void reduce(DoubleWritable key, Iterable<ArrayWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            Text[] array = new Text[2];

            for (ArrayWritable val: values){
                array = (Text)val.get();
                //TODO: sort output by rating
//                for (Writable writable: val.get()){
//                    Text text = (Text)writable;
//                    String s = text.toString();
//                }
            }

            //TODO use MultipleOutputs and generate output with custom filename
            multipleOutputs.write(rating, videoID, key.toString());
        }


    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(YouTubeStat.class);
        job.setMapperClass(VideoStatMapper.class);
        job.setCombinerClass(ArrayMultiOutputReducer.class);
        job.setReducerClass(ArrayMultiOutputReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}