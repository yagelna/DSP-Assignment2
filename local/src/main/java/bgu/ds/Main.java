package bgu.ds;

import bgu.ds.config.AWSConfigProvider;
import bgu.ds.config.LocalAWSConfig;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

public class Main {
    private static final Region region = Region.US_EAST_1;
    public static EmrClient emr = EmrClient.builder().region(region).build();

    public final static LocalAWSConfig config = AWSConfigProvider.getConfig();

    public static void main(String[]args){
        System.out.println("[INFO] Connecting to aws");
        System.out.println( "list cluster");
        System.out.println( emr.listClusters());

        // Step 1
        HadoopJarStepConfig step1 = HadoopJarStepConfig.builder()
                .jar(String.format("s3://%s/%s/word-count-1.0.jar", config.bucketName(), config.jarsPath()))
                .mainClass("Step1")
                .build();

        StepConfig stepConfig1 = StepConfig.builder()
                .name("Step1")
                .hadoopJarStep(step1)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        //Job flow
        JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
                .instanceCount(config.numberOfInstances())
                .masterInstanceType(config.instanceType())
                .slaveInstanceType(config.instanceType())
                .hadoopVersion("3.3.6")
                .ec2KeyName("vockey")
                .keepJobFlowAliveWhenNoSteps(false)
                .placement(PlacementType.builder().availabilityZone("us-east-1a").build())
                .build();

        System.out.println("Set steps");
        RunJobFlowRequest runFlowRequest = RunJobFlowRequest.builder()
                .name("Map reduce project")
                .instances(instances)
                .steps(stepConfig1)
                .logUri(String.format("s3://%s/%s/", config.bucketName(), config.logsPath()))
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .releaseLabel("emr-7.0.0")
                .build();

        RunJobFlowResponse runJobFlowResponse = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResponse.jobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
