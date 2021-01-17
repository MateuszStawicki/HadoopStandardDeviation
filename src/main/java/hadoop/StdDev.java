package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StdDev {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {
        int series = 0;
        String hand = "";
        String[] fingersNames = new String[]{"first", "second", "third", "fourth", "fifth"};
        Double[] fingersResults = new Double[fingersNames.length];
        DoubleWritable tempWritable = new DoubleWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            try {
                JSONObject obj = new JSONObject(value.toString());
                for (int k = 0; k < fingersNames.length; k++) {
                    fingersResults[k] = Double.valueOf(obj.getJSONObject("features2D").get(fingersNames[k]).toString());
                }

                for (int j = 0; j < 5; j++) {
                    hand = obj.getString("side");
                    series = obj.getInt("series");
                    tempWritable.set(fingersResults[j]);
                    context.write(new Text(hand + "_" + series + "_" + "_" + fingersNames[j]), tempWritable);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    public static class stdDevReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable output = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> results,
                           Context context
        ) throws IOException, InterruptedException {

            List<Double> cache = new ArrayList<>();
            for (DoubleWritable val : results) {
                cache.add(val.get());
            }

            double sum = cache.stream().mapToDouble(Double::doubleValue).sum();
            double mean = sum / (double) cache.size();
            double sumOfSquares = 0.0;

            for (double value : cache) {
                sumOfSquares += Math.pow((value - mean), 2.0);
            }
            output.set(Math.sqrt(sumOfSquares / (double) cache.size()));

            context.write(key, output);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: StdDev <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "StdDev");
        job.setJarByClass(StdDev.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(stdDevReducer.class);
        job.setReducerClass(stdDevReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}