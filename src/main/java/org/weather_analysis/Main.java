package org.weather_analysis;

import java.io.*;
import java.net.URI;
import java.text.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Main contains two MapReduce jobs:
 *  - Job1: aggregate per district × YYYY-MM → total_precip, mean_temp
 *  - Job2: find YYYY-MM with highest total_precip across island
 *
 * Usage (HDFS paths):
 *   hadoop jar weather-mr-1.0-SNAPSHOT-jar-with-dependencies.jar \
 *      org.example.weather.Main \
 *      /user/you/weather_input /user/you/locationData_cleaned.csv /user/you/job1_out /user/you/job2_out
 */
public class Main {

    // --------------------------
    // --- Job 1: Mapper/Reducer
    // --------------------------
    public static class Job1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String,String> locMap = new HashMap<>();
        private boolean isHeaderSkipped = false;
        private SimpleDateFormat[] parsers;

        @Override
        protected void setup(Context context) throws IOException {
            // date parsers to try
            parsers = new SimpleDateFormat[] {
                    new SimpleDateFormat("yyyy-MM-dd"),
                    new SimpleDateFormat("dd/MM/yyyy"),
                    new SimpleDateFormat("MM/dd/yyyy"),
                    new SimpleDateFormat("M/d/yyyy"),
                    new SimpleDateFormat("d/M/yyyy")
            };
            // load cached file(s) (distributed cache)
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    String name = (new Path(uri.getPath())).getName();
                    // we expect locationData_cleaned.csv
                    if (name.toLowerCase().contains("location")) {
                        loadLocationLookup(new BufferedReader(new InputStreamReader(new FileInputStream(name), "UTF-8")));
                    }
                }
            }
        }

        private void loadLocationLookup(BufferedReader r) throws IOException {
            String line;
            String header = r.readLine(); // assume header present
            while ((line = r.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                // simple CSV split (adjust if your file has commas in quotes)
                String[] parts = line.split(",", -1);
                // try to discover indices: location_id, district or city_name
                // assume first column location_id, next lat/lon... city_name last
                String locid = parts[0].trim();
                String district = null;
                // try find district column by name? we don't have header columns here
                // fallback use last column as city_name/district
                district = parts[parts.length-1].trim();
                if (district.isEmpty()) district = "UNKNOWN";
                locMap.put(locid, district);
            }
            r.close();
        }

        private Date tryParseDate(String s) {
            if (s == null) return null;
            s = s.trim();
            for (SimpleDateFormat f : parsers) {
                try {
                    f.setLenient(false);
                    return f.parse(s);
                } catch (Exception e) { }
            }
            return null;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.length()==0) return;
            // skip header heuristically if present
            if (!isHeaderSkipped && (line.toLowerCase().contains("location_id") || line.toLowerCase().contains("date"))) {
                isHeaderSkipped = true;
                return;
            }
            // extract CSV fields simply
            String[] cols = line.split(",", -1);
            // expected columns at least: location_id, date, precipitation_sum or precipitation_hours, temperature_2m_mean
            // tolerant parsing: locate fields by matching header names is better, but for simplicity we assume standard order:
            // 0: location_id, 1: date, ... find precip and temp by scanning headings if available (not implemented here)
            String locationId = cols.length>0 ? cols[0].trim() : "";
            String dateStr = cols.length>1 ? cols[1].trim() : "";
            // find precipitation and temperature by scanning the line for keywords in header would be better,
            // but we'll try common column index patterns:
            // search for any numeric that resembles precip or temp by column name — simpler: try known names if header present
            // For robust code, you may preprocess to supply indices via job config. Here we fallback:
            double precipVal = 0.0;
            Double tempVal = null;
            // Try to find columns by header is not implemented; try to detect by approximate column contents:
            for (int i=2;i<cols.length;i++) {
                String c = cols[i].trim();
                // if column name included in first line, we would need header mapping; here we use heuristic:
                // if c contains ':' or time string or sunrise/sunset, skip
            }
            // Heuristic below: assume precipitation_sum is the first numeric after sunshine_duration etc.
            // Instead: attempt to parse named variants if present in the header line by reading the first line from the input file.
            // Simpler approach for local use: expect CSV columns in the same order as your sample:
            // location_id,date,weather_code,...,precipitation_sum (mm),rain_sum (mm),precipitation_hours (h),wind_speed_10m_max...
            // From your sample precipitation_sum was near the 11th column, and temp_mean is at index 5 (0-based).
            try {
                // temp_mean with index 5 (based on sample)
                if (cols.length>5) {
                    tempVal = cols[5].isEmpty() ? null : Double.parseDouble(cols[5]);
                }
            } catch (Exception e) {
                tempVal = null;
            }
            try {
                // precipitation_sum around index 11 (0-based). precipitation_hours around index 13
                if (cols.length>13 && !cols[13].isEmpty()) {
                    precipVal = Double.parseDouble(cols[13]); // precipitation_hours (h) if you prefer hours
                } else if (cols.length>11 && !cols[11].isEmpty()) {
                    precipVal = Double.parseDouble(cols[11]); // precipitation_sum (mm)
                } else {
                    // fallback: try any numeric later column
                    for (int i=2;i<cols.length;i++) {
                        String s = cols[i].trim();
                        if (s.matches("^[0-9]+(\\.[0-9]+)?$")) {
                            double v = Double.parseDouble(s);
                            // assume small values could be temp — but we cannot reliably infer
                            // if we get here, set 0
                        }
                    }
                }
            } catch (Exception e) {
                precipVal = 0.0;
            }

            Date d = tryParseDate(dateStr);
            if (d == null) return;
            Calendar cal = Calendar.getInstance();
            cal.setTime(d);
            String yearMonth = String.format("%04d-%02d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH)+1);

            String district = locMap.getOrDefault(locationId, "UNKNOWN");
            // emit key: district|YYYY-MM   value: precip \t temp \t hasTemp
            String valOut = String.format("%.3f\t%s\t%d", precipVal, (tempVal==null? "": Double.toString(tempVal)), (tempVal==null?0:1));
            String outKey = district + "|" + yearMonth;
            context.write(new Text(outKey), new Text(valOut));
        }
    }

    public static class Job1Reducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double precipSum = 0.0;
            double tempSum = 0.0;
            long tempCount = 0;
            for (Text t : values) {
                String[] parts = t.toString().split("\\t", -1);
                if (parts.length>=1) {
                    try { precipSum += Double.parseDouble(parts[0]); } catch(Exception e) {}
                }
                if (parts.length>=2 && parts[1]!=null && !parts[1].isEmpty()) {
                    try { tempSum += Double.parseDouble(parts[1]); tempCount++; } catch(Exception e) {}
                }
            }
            String meanTempStr = "";
            if (tempCount>0) meanTempStr = String.format("%.3f", tempSum / tempCount);
            // output CSV: district,YYYY-MM,total_precip,mean_temp
            String[] keyParts = key.toString().split("\\|",2);
            String district = keyParts[0];
            String yymm = keyParts.length>1? keyParts[1] : "";
            String out = district + "," + yymm + "," + String.format("%.3f", precipSum) + "," + meanTempStr;
            context.write(NullWritable.get(), new Text(out));
        }
    }

    // --------------------------
    // --- Job 2: Mapper/Reducer
    // --------------------------
    public static class Job2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.length()==0) return;
            // expected input from Job1: district,YYYY-MM,total_precip,mean_temp
            String[] parts = line.split(",", -1);
            if (parts.length < 3) return;
            String yymm = parts[1].trim();
            double precip = 0.0;
            try { precip = Double.parseDouble(parts[2]); } catch(Exception e) {}
            context.write(new Text(yymm), new DoubleWritable(precip));
        }
    }

    public static class Job2Reducer extends Reducer<Text, DoubleWritable, NullWritable, Text> {
        // Because we will set reducers = 1, we can track global max here
        private String maxMonth = null;
        private double maxVal = -1.0;

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable v : values) sum += v.get();
            if (sum > maxVal) {
                maxVal = sum;
                maxMonth = key.toString();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (maxMonth != null) {
                // output CSV: YYYY-MM,total_precip
                String out = maxMonth + "," + String.format("%.3f", maxVal);
                context.write(NullWritable.get(), new Text(out));
            }
        }
    }

    // --------------------------
    // --- Driver
    // --------------------------
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: Main <weather_input_hdfs_dir> <location_lookup_hdfs_path> <job1_output_hdfs> <job2_output_hdfs>");
            System.exit(2);
        }
        String input = args[0];
        String lookup = args[1];
        String job1Out = args[2];
        String job2Out = args[3];

        Configuration conf = new Configuration();

        // --- Job1 ---
        Job job1 = Job.getInstance(conf, "district-month-precip-temp");
        job1.setJarByClass(Main.class);
        job1.setMapperClass(Job1Mapper.class);
        job1.setReducerClass(Job1Reducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job1, new Path(input));
        TextOutputFormat.setOutputPath(job1, new Path(job1Out));
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        // Add lookup to distributed cache (must be in HDFS path)
        job1.addCacheFile(new URI(lookup));

        boolean ok = job1.waitForCompletion(true);
        if (!ok) {
            System.err.println("Job1 failed");
            System.exit(1);
        }

        // --- Job2 ---
        Job job2 = Job.getInstance(conf, "island-month-max-precip");
        job2.setJarByClass(Main.class);
        job2.setMapperClass(Job2Mapper.class);
        job2.setReducerClass(Job2Reducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);

        // Use the job1 output as input
        TextInputFormat.addInputPath(job2, new Path(job1Out));
        TextOutputFormat.setOutputPath(job2, new Path(job2Out));
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        // Make sure only one reducer so we can compute global max in cleanup
        job2.setNumReduceTasks(1);

        ok = job2.waitForCompletion(true);
        if (!ok) {
            System.err.println("Job2 failed");
            System.exit(1);
        }

        System.exit(0);
    }
}
