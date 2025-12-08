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

import java.text.SimpleDateFormat;
import java.util.*;

public class Job1 {

    public static class Job1Mapper extends Mapper<LongWritable, Text, Text, Text> {

        private SimpleDateFormat inputDateFormat = new SimpleDateFormat("M/d/yyyy");

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            try {
                String line = value.toString().trim();
                if (line.isEmpty()) return;

                // Skip headers
                if (line.startsWith("location_id,date") || line.startsWith("location_id,latitude"))
                    return;

                String[] parts = line.split(",", -1);

                // locationData.csv
                if (parts.length == 8) {
                    String locationId = parts[0].trim();
                    String cityName = parts[7].trim();
                    context.write(new Text(locationId), new Text("L|" + cityName));
                }

                // weatherData.csv
                else if (parts.length >= 14) {
                    String locationId = parts[0].trim();
                    String dateStr = parts[1].trim();
                    String tempMeanStr = parts[5].trim();          // temperature_2m_mean
                    String precipSumStr = parts[11].trim();        // precipitation_sum (mm)

                    if (locationId.isEmpty() || dateStr.isEmpty()) return;

                    context.write(new Text(locationId),
                            new Text("W|" + dateStr + "|" + tempMeanStr + "|" + precipSumStr));
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Job1Reducer extends Reducer<Text, Text, Text, Text> {

        private SimpleDateFormat inputDateFormat = new SimpleDateFormat("M/d/yyyy");

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            try {
                String cityName = null;
                List<String[]> weatherRows = new ArrayList<>();

                // Separate location vs weather records
                for (Text t : values) {
                    String v = t.toString();
                    if (v.startsWith("L|")) {
                        cityName = v.substring(2);
                    } else if (v.startsWith("W|")) {
                        String[] p = v.split("\\|", -1);
                        weatherRows.add(new String[]{p[0], p[1], p[2]}); // date, temp_mean, precip_sum
                    }
                }

                if (cityName == null || weatherRows.isEmpty()) return;

                // Map to aggregate by YYYY-MM
                Map<String, Double> totalPrecipMap = new HashMap<>();
                Map<String, Double> tempSumMap = new HashMap<>();
                Map<String, Integer> tempCountMap = new HashMap<>();

                for (String[] row : weatherRows) {
                    String dateStr = row[0];
                    double temp = parseDouble(row[1]);
                    double precip = parseDouble(row[2]); // precipitation_sum now

                    Date date = inputDateFormat.parse(dateStr);
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(date);
                    String yymm = String.format("%04d-%02d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1);

                    totalPrecipMap.put(yymm, totalPrecipMap.getOrDefault(yymm, 0.0) + precip);
                    tempSumMap.put(yymm, tempSumMap.getOrDefault(yymm, 0.0) + temp);
                    tempCountMap.put(yymm, tempCountMap.getOrDefault(yymm, 0) + 1);
                }

                // Emit aggregated results
                for (String yymm : totalPrecipMap.keySet()) {
                    double totalPrecip = totalPrecipMap.get(yymm);
                    double meanTemp = tempSumMap.get(yymm) / tempCountMap.get(yymm);
                    context.write(new Text(cityName + "," + yymm),
                            new Text(String.format("%.3f,%.3f", totalPrecip, meanTemp)));
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
        Job job = Job.getInstance(conf, "Weather Job1 Aggregate");

        job.setJarByClass(Job1.class);

        job.setMapperClass(Job1Mapper.class);
        job.setReducerClass(Job1Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // weatherData.csv
        FileInputFormat.addInputPath(job, new Path(args[1])); // locationData.csv
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}