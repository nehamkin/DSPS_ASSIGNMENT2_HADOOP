import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class Step6 {

    public static String STEP5_INPUT = "/step5output";
    public static String STEP6_OUTPUT = "s3://orrisoutputbucketstep6/output";


    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            Text newKey = new Text();
            newKey.set(String.format("%s %s",splits[0],splits[1]));
            Text newValue = new Text();
            context.write(newKey, value);
        }

    }

    private static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //should only happen once
            for(Text val : values)
                context.write(key, val);
        }

    }



    private static class CompareClass extends WritableComparator {

        protected CompareClass() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable key1, WritableComparable key2) {
            String[] key1parsed = key1.toString().split("\t");
            String k1w1 = key1parsed[0];
            String k1w2 = key1parsed[1];
            double k1prob = Double.parseDouble(key1parsed[3]);

            String[] key2parsed = key2.toString().split(" ");
            String k2w1 = key2parsed[0];
            String k2w2 = key2parsed[1];
            double k2prob = Double.parseDouble(key2parsed[3]);

            if (k1w1.equals(k2w1) && k1w2.equals(k2w2)) {
                return (int) Math.signum(k2prob - k1prob);
            }
            return (k1w1+" "+k1w2).compareTo(k2w1+" "+k2w2);

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step6.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setSortComparatorClass(Step6.CompareClass.class);
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(STEP5_INPUT));
        FileOutputFormat.setOutputPath(job, new Path(STEP6_OUTPUT));
        job.waitForCompletion(true);
    }
}



