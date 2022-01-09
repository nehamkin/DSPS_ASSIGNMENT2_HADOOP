import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import java.io.IOException;


public class Step1 {

    private static String STEP1_OUTPUT = "/step1output/";

    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] strings = value.toString().split("\t");
            String w1 = strings[0];
            if(!(w1.equals("*"))){
                int occur = Integer.parseInt(strings[2]);
                Text newKey = new Text();
                newKey.set(String.format("%s",w1));
                Text newVal = new Text();
                Text star = new Text();
                star.set("*");
                newVal.set(String.format("%d",occur));
                context.write(newKey, newVal);
                context.write(star, newVal);
            }
        }

    }

    private static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String w1 = key.toString();
            int sum = 0;

            for (Text val : values) {
                sum += Long.parseLong(val.toString());
            }

            Text newKey = new Text();
            newKey.set(String.format("%s",w1));
            Text newVal = new Text();
            newVal.set(String.format("%d",sum));
            context.write(newKey, newVal);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step1.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path("s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data"));
        FileOutputFormat.setOutputPath(job, new Path(STEP1_OUTPUT));
        job.waitForCompletion(true);
    }
}





