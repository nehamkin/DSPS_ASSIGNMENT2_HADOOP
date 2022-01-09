import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.Iterator;

public class Step4 {

    private static String STEP2_INPUT="/step2output/";
    private static String STEP3_INPUT="/step3output/";
    private static String STEP4_OUTPUT="/step4output/";

    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {

            //each value in  step2output | step3output looks like w1 w2 ?w3 \t w4
            String[] strings = value.toString().split("\t");
            String[] words = strings[0].split(" ");

            //there will all ways be a least two words because its all the words from 2gram and 3gram
            String first = words[0];
            String second = words[1];

            //this is the count that we got from the one gram and two gram
            int count = Integer.parseInt(strings[1]) ;

            Text outputCount=new Text();
            outputCount.set(String.format("%d",count));

            Text w1w2 = new Text();
            w1w2.set(String.format("%s %s",first,second));

            //enters if it came from 3gram
            if(words.length>2){
                String third=words[2];
                Text w1w2w3_C = new Text();
                w1w2w3_C.set(String.format("%s %s %s\t%d",first,second,third,count));
                Text w2w3=new Text();
                w2w3.set(String.format("%s %s",second,third));
                context.write(w2w3, w1w2w3_C);
                context.write(w1w2, w1w2w3_C);
            }
            else{
                context.write(w1w2 ,outputCount);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override

        /**
         * key -> <w1,w2>
         * values -> <w1,w2,wi, count> | <count> | <wj,w1,w2, count>
         */
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] strings = key.toString().split(" ");

            String first = strings[0];
            String second = strings[1];

            Text newKey = new Text();
            Text newVal = new Text();

            String newValStr = "";

            int count;
            boolean found = false;

            //we will find the amount of times that w1w2 are in the corpus, it must be 1 of the values!!
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext() && !found){
                Text val = iterator.next();
                String[] s = val.toString().split("\t");
                if(s.length == 1) {
                    count = (int) Long.parseLong(s[0]);
                    newValStr = String.format("%s %s %d", first, second, count);
                    found = true;
                }
            }

            if(!found)
                return;

            for (Text val : values) {
                String[] splitVal = val.toString().split("\t");
                String[] words = splitVal[0].split("\t");
                if(words.length>1){
                    String prefix = "w2w3";
                    if(first.equals(words[0]))
                        prefix = "w1w2";
                    newKey.set(String.format("%s %s %s",words[0],words[1],words[2]));
                    newKey.set(prefix + " "+ newValStr);
                    context.write(newKey, newVal);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step4.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job, new Path(STEP2_INPUT), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(STEP3_INPUT), TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(STEP4_OUTPUT));
        job.waitForCompletion(true);
    }

}
