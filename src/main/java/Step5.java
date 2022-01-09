import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class Step5 {

    public static String STEP1_INPUT = "/step1output";
    public static String STEP3_INPUT = "/step3output";
    public static String STEP4_INPUT = "/step4output";
    public static String STEP5_OUTPUT = "/step5output";

    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] strings = value.toString().split("\t");
            String[] w1w2w3 = strings[0].split(" ");

            String w1 = w1w2w3[0];
            String w2 = w1w2w3[1];
            String w3= w1w2w3[2];
            int count;
            String[] val = strings[1].split(" ");
            Text text = new Text();
            text.set(String.format("%s %s %s",w1,w2,w3));
            //came from step3
            if(val.length == 1){
                count= Integer.parseInt(strings[1]);
                Text text1 = new Text();
                text1.set(String.format("%d",count));
                context.write(text ,text1);
            }
            //came from step4 <prefix wi wi+1 count>
            else{
                count= Integer.parseInt(val[3]) ;
                String prefix = val[0];
                String wi = val[1];
                String wi1 = val[2];
                Text newVal=new Text();
                newVal.set(String.format("%s %s %s %d",prefix,wi,wi1,count));
                context.write(text, newVal);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public static Long c0;
        public static HashMap <String, Double> oneGramMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] strings = key.toString().split(" ");

            String w1 = strings[0];
            String w2 = strings[1];
            String w3= strings[2];

            double n1= oneGramMap.get(w3);
            double n2=0.0;
            double n3=0.0;

            double c1= oneGramMap.get(w2);
            double c2=0.0;

            double k2=0.0;
            double k3=0.0;

            Text newKey = new Text();
            Text newVal = new Text();

            boolean gotw1w2 = false;
            boolean gotw2w3 = false;
            boolean gotw1w2w3 = false;

            for (Text val : values) {
                String[] parsedVal = val.toString().split(" ");

                //the count of w1w2w3
                if(parsedVal.length == 1){
                    n3=(double) Long.parseLong(parsedVal[0]);
                    k3=(Math.log(n3+1)+1)/(Math.log(n3+1)+2);
                    gotw1w2w3 = true;
                }

                else{
                    String prefix = parsedVal[0];
                    String wi = parsedVal[1];
                    String wi1 = parsedVal[2];
                    double wiwi1C = Long.parseLong(parsedVal[3]);
                    if(prefix.equals("w1w2")){
                        c2 = wiwi1C;
                        gotw1w2 = true;
                    }
                    else{
                        n2 = wiwi1C;
                        k2=(Math.log(n2+1)+1)/(Math.log(n2+1)+2);
                        gotw2w3=true;
                    }
                }

                if(gotw1w2 && gotw2w3 && gotw1w2w3){
                    double probability =
                                    (k3*(n3/c2))
                                    +((1-k3)*k2*(n2/c1))
                                    +((1-k3)*(1-k2)*(n1/c0));

                    newKey.set(String.format("%s %s %s",w1,w2,w3));
                    newVal.set(String.format("%s",probability));
                    context.write(newKey, newVal);
                }

            }

        }

        /**
         * Puts all the counts per each word in a map. this is scalable, I have checked and the Map will not exceed 12 megabytes
         */
        public void setup(Reducer.Context context) throws IOException {
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            RemoteIterator<LocatedFileStatus> it=fileSystem.listFiles(new Path(STEP1_INPUT),false);
            while(it.hasNext()){
                LocatedFileStatus fileStatus=it.next();
                if (fileStatus.getPath().getName().startsWith("part")){
                    FSDataInputStream InputStream = fileSystem.open(fileStatus.getPath());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(InputStream, StandardCharsets.UTF_8));
                    String line;
                    String[] onegrams;
                    while ((line = reader.readLine()) != null){
                        onegrams = line.split("\t");
                        if(onegrams[0].equals("*")){
                            c0=Long.parseLong(onegrams[1]);
                        }
                        else{
                            oneGramMap.put(onegrams[0], (double) Long.parseLong(onegrams[1]));
                        }
                    }
                    reader.close();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step5.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job, new Path(STEP4_INPUT), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(STEP3_INPUT), TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(STEP5_OUTPUT));
        job.waitForCompletion(true);
    }


}
