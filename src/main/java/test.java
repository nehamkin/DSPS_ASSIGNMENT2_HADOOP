

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.UUID;

public class test {
    public static AwsCredentialsProvider credentialsProvider;
    public static S3Client S3;
    public static Ec2Client ec2;
    public static EmrClient emr;


    //------------------------------
    //public static final String HEB_2GRAM = "s3://dsps2-sources/googlebooks-heb-all-2gram-20120701-sg";
//    public static final String HEB_2GRAM = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
//    public static final String RESULT_BUCKET = "dsps2-results";
//    public static final String SOURCES_BUCKET = "dsps2-sources";
//    public static final String FIRST_ITERATION_JAR_PATH = "s3://" + SOURCES_BUCKET + "/FirstIteration.jar";
//    public static final String SECOND_ITERATION_JAR_PATH = "s3://" + SOURCES_BUCKET + "/SecondIteration.jar";
//    public static final String NPMI_ITERATION_JAR_PATH = "s3://" + SOURCES_BUCKET + "/NpmiIteration.jar";
//    public static final String STOP_WORDS_FILE_NAME = "stop_words.txt";
//    public static final String STOP_WORDS_PATH = "s3://" + SOURCES_BUCKET + "/" + STOP_WORDS_FILE_NAME;
//    public static final Region REGIONS = Region.US_EAST_1;
//    public static final String FIRST_OUT = "s3://" + RESULT_BUCKET + "/first_out";
//    public static final String SECOND_OUT = "s3://" + RESULT_BUCKET + "/second_out";
//    public static final String OUTPUT_PATH = "s3://" + RESULT_BUCKET + "/result-" + UUID.randomUUID();
//    public static final Integer INSTANCE_COUNT = 8;
    //------------------------------


    public static void main(String[] args) {

//        credentialsProvider = EnvironmentVariableCredentialsProvider.create();

        S3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();

        ec2 = Ec2Client.builder()
                .region(Region.US_EAST_1)
                .build();

        emr = EmrClient.builder()
//                .credentialsProvider(credentialsProvider)
                .region(Region.US_EAST_1)
                .build();


        HadoopJarStepConfig step1 = HadoopJarStepConfig.builder()
                .jar("s3://orrinehamkinstepjarsbucket/Step1.jar")
                .args("Step1","null","s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data")
                .build();

        StepConfig step1Config = StepConfig.builder()
                .name("Step1")
                .hadoopJarStep(step1)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        HadoopJarStepConfig step2 = HadoopJarStepConfig.builder()
                .jar("s3://orrinehamkinstepjarsbucket/Step2.jar")
                .args("Step1","null","s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data")
                .build();

        StepConfig step2Config = StepConfig.builder()
                .name("Step2")
                .hadoopJarStep(step2)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        HadoopJarStepConfig step3 = HadoopJarStepConfig.builder()
                .jar("s3://orrinehamkinstepjarsbucket/Step3.jar")
                .args("Step1","null","s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data")
                .build();

        StepConfig step3Config = StepConfig.builder()
                .name("Step3")
                .hadoopJarStep(step3)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
                .instanceCount(3)
                .masterInstanceType(InstanceType.M4_LARGE.toString())
                .slaveInstanceType(InstanceType.M4_LARGE.toString())
                .hadoopVersion("2.7.3")
                .ec2KeyName("vockey")
                .placement(PlacementType.builder().build())
                .keepJobFlowAliveWhenNoSteps(false)
                .build();

        RunJobFlowRequest request = RunJobFlowRequest.builder()
                .name("onegram")
                .instances(instances)
                .steps(step1Config,step2Config,step3Config)
                .logUri("s3n://orrilogs/")
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .releaseLabel("emr-5.11.0")
                .build();

        RunJobFlowResponse response = emr.runJobFlow(request);
        String id = response.jobFlowId();
        System.out.println("our cluster id: "+id);

    }
}

