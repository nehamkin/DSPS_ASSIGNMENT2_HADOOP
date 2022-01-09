import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import java.io.IOException;

/**
 * Input - 3gram database
 * Output - sums the occurrences of each key
 */
public class Step3 {

    private static String STEP3_OUTPUT="/step3output/";

    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            // the value should be <w1 w2 ?w3  /t count>
            String[] strings = value.toString().split("\t");
            String[] words = strings[0].split(" ");

            if(words.length>2){

                String w1 = words[0];
                String w2 = words[1];
                String w3= words[2];

                String all3 = String.format("%s %s %s",w1,w2,w3);

                int count = Integer.parseInt(strings[2]);

                Text newKey = new Text();
                newKey.set(all3);
                Text newVal = new Text();
                newVal.set(String.format("%d",count));
                context.write(newKey ,newVal);

//
//                Text textFirst2 = new Text();
//                textTotal.set(String.format("%s %s %s",w1,w2,"#first2"));
//                Text value2 = new Text();
//                value2.set(String.format("%s    %d",all3, count));
//                context.write(textTotal ,value2);
//
//                Text textSecond2 = new Text();
//                textTotal.set(String.format("%s %s %s",w2,w3,"#second2"));
//                Text value3 = new Text();
//                value3.set(String.format("%s    %d",all3, count));
//                context.write(textTotal ,value3);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String oldKey=key.toString();
            int sum_occ = 0;
            for (Text val : values) {
                sum_occ += Long.parseLong(val.toString());
            }
            Text newKey = new Text();
            newKey.set(String.format("%s",oldKey));
            Text newVal = new Text();
            newVal.set(String.format("%d",sum_occ));
            context.write(newKey, newVal);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step3.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path("s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        FileOutputFormat.setOutputPath(job, new Path(STEP3_OUTPUT));
        job.waitForCompletion(true);
    }

}
