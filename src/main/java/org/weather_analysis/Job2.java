package org.weather_analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Job2 {

    public static class Job2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context ctx)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("location_id")) return;

            String[] c = line.split(",", -1);
            if (c.length < 12) return;  // ensure precipitation_sum exists

            String date = c[1];  // "M/d/yyyy"
            String[] d = date.split("/");
            if (d.length != 3) return;

            String month = d[0].length() == 1 ? "0" + d[0] : d[0];
            String year = d[2];

            String yearMonth = year + "-" + month;

            double precip = 0.0;
            try {
                precip = Double.parseDouble(c[11]); // precipitation_sum (mm)
            } catch (Exception e) {
                return;
            }

            ctx.write(new Text(yearMonth), new DoubleWritable(precip));
        }
    }

    public static class Job2Reducer extends Reducer<Text, DoubleWritable, NullWritable, Text> {

        private String maxMonth = null;
        private double maxVal = -1.0;

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context ctx)
                throws IOException, InterruptedException {

            double total = 0;
            for (DoubleWritable v : values) {
                total += v.get();
            }

            if (total > maxVal) {
                maxVal = total;
                maxMonth = key.toString();
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            context.write(NullWritable.get(),
                    new Text(maxMonth + "," + maxVal));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.err.println("Usage: Main <weather_in> <location_csv> <job1_out> <job2_out>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job2 = Job.getInstance(conf, "max-precip-month");
        job2.setJarByClass(Job2.class);

        job2.setMapperClass(Job2Mapper.class);
        job2.setReducerClass(Job2Reducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DoubleWritable.class);

        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);

        job2.setNumReduceTasks(1);

        TextInputFormat.addInputPath(job2, new Path(args[0]));
        TextOutputFormat.setOutputPath(job2, new Path(args[3]));

        job2.waitForCompletion(true);
    }
}
