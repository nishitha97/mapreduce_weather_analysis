package org.weather_analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.ArrayList;
import java.util.List;

public class Job1 {

    public static class Job1Mapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            try {
                String line = value.toString().trim();
                if (line.isEmpty()) return;

                // Skip headers
                if (line.startsWith("location_id,date") || line.startsWith("location_id,latitude"))
                    return;

                String[] parts = line.split(",", -1);

                // locationData.csv (8 columns)
                if (parts.length == 8) {
                    String locationId = parts[0].trim();
                    String cityName = parts[7].trim();

                    context.write(new Text(locationId), new Text("L|" + cityName));
                }

                // weatherData.csv (21 columns normally)
                else if (parts.length >= 14) {
                    String locationId = parts[0].trim();
                    String date = parts[1].trim();

                    String precipitationSum = parts[11].trim();
                    String precipitationHours = parts[13].trim();

                    if (locationId.isEmpty() || date.isEmpty())
                        return;

                    context.write(
                            new Text(locationId),
                            new Text("W|" + date + "|" + precipitationSum + "|" + precipitationHours)
                    );
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Job1Reducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) {
            try {
                String cityName = null;
                List<String[]> weatherRows = new ArrayList<>();

                for (Text t : values) {
                    String v = t.toString();

                    if (v.startsWith("L|")) {
                        cityName = v.substring(2);
                    }
                    else if (v.startsWith("W|")) {
                        String[] p = v.split("\\|", -1);
                        weatherRows.add(new String[]{p[1], p[2], p[3]});
                    }
                }

                if (cityName == null) return;

                for (String[] row : weatherRows) {

                    String date = row[0];
                    double precipSum = parseDouble(row[1]);
                    double precipHours = parseDouble(row[2]);

                    int month = Integer.parseInt(date.split("/")[0]);

                    String period = (month >= 9 || month <= 3) ? "Sep-Mar" : "Apr-Aug";

                    ctx.write(
                            new Text(cityName),
                            new Text(period + "," + precipSum + "," + precipHours)
                    );
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private double parseDouble(String s) {
            try {
                if (s == null || s.isEmpty()) return 0.0;
                return Double.parseDouble(s);
            } catch (Exception ex) {
                return 0.0;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: Job1 <weather_csv> <location_csv> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weather Job1 Join");

        job.setJarByClass(Job1.class);

        job.setMapperClass(Job1Mapper.class);
        job.setReducerClass(Job1Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}