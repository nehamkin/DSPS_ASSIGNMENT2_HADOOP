

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;
import software.amazon.awssdk.services.s3.S3Client;

public class MainClass {

    public static S3Client S3;
    public static Ec2Client ec2;
    public static EmrClient emr;


    public static void main(String[] args) {


        S3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();

        ec2 = Ec2Client.builder()
                .region(Region.US_EAST_1)
                .build();

        emr = EmrClient.builder()
                .region(Region.US_EAST_1)
                .build();


        HadoopJarStepConfig step1 = HadoopJarStepConfig.builder()
                .jar("s3://orrinehamkinstepjarsbucket/Step1.jar")
                .build();

        StepConfig step1Config = StepConfig.builder()
                .name("Step1")
                .hadoopJarStep(step1)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        HadoopJarStepConfig step2 = HadoopJarStepConfig.builder()
                .jar("s3://orrinehamkinstepjarsbucket/Step2.jar")
                .build();

        StepConfig step2Config = StepConfig.builder()
                .name("Step2")
                .hadoopJarStep(step2)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        HadoopJarStepConfig step3 = HadoopJarStepConfig.builder()
                .jar("s3://orrinehamkinstepjarsbucket/Step3.jar")
                .build();

        StepConfig step3Config = StepConfig.builder()
                .name("Step3")
                .hadoopJarStep(step3)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        HadoopJarStepConfig step4 = HadoopJarStepConfig.builder()
                .jar("s3://orrinehamkinstepjarsbucket/Step4.jar")
                .build();

        StepConfig step4Config = StepConfig.builder()
                .name("Step4")
                .hadoopJarStep(step4)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        HadoopJarStepConfig step5 = HadoopJarStepConfig.builder()
                .jar("s3://orrinehamkinstepjarsbucket/Step5.jar")
                .build();

        StepConfig step5Config = StepConfig.builder()
                .name("Step5")
                .hadoopJarStep(step5)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        HadoopJarStepConfig step6 = HadoopJarStepConfig.builder()
                .jar("s3://orrinehamkinstepjarsbucket/Step6.jar")
                .build();

        StepConfig step6Config = StepConfig.builder()
                .name("Step6")
                .hadoopJarStep(step6)
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
                .steps(step1Config,step2Config,step3Config,step4Config,step5Config,step6Config)
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

