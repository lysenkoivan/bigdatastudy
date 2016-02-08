package youtuberate;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

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

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }

    }

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

            context.write(rate, new TextArrayWritable(array));

        }
    }

    public static class ArrayMultiOutputReducer
            extends Reducer<DoubleWritable, ArrayWritable, Text, IntWritable> {

        private MultipleOutputs<Text, IntWritable> multipleOutputs;

        public void reduce(DoubleWritable key, Iterable<TextArrayWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            List<String[]> array = new ArrayList<String[]>();

            for (TextArrayWritable val: values){
                array.add(val.toStrings());
            }

            //sorting output by rating using Collections.sort with comparator
            Collections.sort(array, new Comparator<String[]>() {
                @Override
                public int compare(final String[] entry1, final String[] entry2) {
                    final Integer rating1 = Integer.parseInt(entry1[0]);
                    final Integer rating2 = Integer.parseInt(entry2[0]);
                    return rating1.compareTo(rating2);
                }
            });

            for (String[] val: array) {
                Integer rating = Integer.parseInt(val[0]);
                String videoID = val[1];
                String filename = key.toString();
            //using MultipleOutputs for generating output with custom filename
            multipleOutputs.write(new Text(videoID), new IntWritable(rating), filename);

            }
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