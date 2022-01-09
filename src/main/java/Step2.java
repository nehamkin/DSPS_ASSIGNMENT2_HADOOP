import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import java.io.IOException;

public class Step2 {

    private static String STEP2_OUTPUT="/step2output/";


    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] strings = value.toString().split("\t");
            String[] words = strings[0].split(" ");
            if(words.length>1){
                String w1 = words[0];
                String w2 = words[1];
                int occur = Integer.parseInt(strings[2]);
                Text newKey = new Text();
                newKey.set(String.format("%s %s",w1,w2));
                Text newVal = new Text();
                newVal.set(String.format("%d",occur));
                context.write(newKey ,newVal);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String oldKey=key.toString();
            int sum = 0;
            for (Text val : values) {
                sum += Long.parseLong(val.toString());
            }
            Text newKey = new Text();
            newKey.set(String.format("%s",oldKey));
            Text newVal = new Text();
            newVal.set(String.format("%d",sum));
            context.write(newKey, newVal);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step2.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path("s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data"));
        FileOutputFormat.setOutputPath(job, new Path(STEP2_OUTPUT));
        job.waitForCompletion(true);


    }

}
