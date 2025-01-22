package bgu.ds;

import bgu.ds.config.AWSConfigProvider;
import bgu.ds.config.LocalAWSConfig;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

import java.io.DataInput;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Main {
    private static final Region region = Region.US_EAST_1;
    public static EmrClient emr = EmrClient.builder().region(region).build();

    public final static LocalAWSConfig config = AWSConfigProvider.getConfig();

    public static void main(String[]args){
        System.out.println("[INFO] Connecting to aws");
        System.out.println("list cluster");
        System.out.println(emr.listClusters());

        double minNpmi = args.length > 0 ? Double.parseDouble(args[0]) : config.defaultMinNpmi();
        double relativeMinNpmi = args.length > 1 ? Double.parseDouble(args[1]) : config.defaultRelativeMinNpmi();
        double thresholdNpmi = args.length > 2 ? Double.parseDouble(args[2]) : config.defaultThresholdNpmi();
        double sampleRate = args.length > 3 ? Double.parseDouble(args[3]) : config.defaultSampleRate();

        String dateSuffix = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date());

        // Step 1
        HadoopJarStepConfig step1 = HadoopJarStepConfig.builder()
                .jar(String.format("s3://%s/%s/count-N-cw1w2-1.0.jar", config.bucketName(), config.jarsPath()))
                .args(config.inputCorpusPath(),
                        String.format("s3n://assignment2-emr/output/count-N-cw1w2/%s", dateSuffix),
                        Boolean.toString(config.useCombiner()), Long.toString(config.maxSplitSize()),
                        "assignment2-emr", config.stopWordsPath(), Double.toString(sampleRate))
                .build();

        StepConfig stepConfig1 = StepConfig.builder()
                .name("Count N and C(w1, w2)")
                .hadoopJarStep(step1)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        // Step 2
        HadoopJarStepConfig step2 = HadoopJarStepConfig.builder()
                .jar(String.format("s3://%s/%s/count-cw1-1.0.jar", config.bucketName(), config.jarsPath()))
                .args(String.format("s3n://assignment2-emr/output/count-N-cw1w2/%s", dateSuffix),
                        String.format("s3n://assignment2-emr/output/count-cw1/%s", dateSuffix),
                        Boolean.toString(config.useCombiner()), Long.toString(config.maxSplitSize()))
                .build();

        StepConfig stepConfig2 = StepConfig.builder()
                .name("Count C(w1)")
                .hadoopJarStep(step2)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        // Step 3
        HadoopJarStepConfig step3 = HadoopJarStepConfig.builder()
                .jar(String.format("s3://%s/%s/count-cw2-1.0.jar", config.bucketName(), config.jarsPath()))
                .args(String.format("s3n://assignment2-emr/output/count-N-cw1w2/%s", dateSuffix),
                        String.format("s3n://assignment2-emr/output/count-cw2/%s", dateSuffix),
                        Boolean.toString(config.useCombiner()), Long.toString(config.maxSplitSize()))
                .build();

        StepConfig stepConfig3 = StepConfig.builder()
                .name("Count C(w2)")
                .hadoopJarStep(step3)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        // Step 4
        HadoopJarStepConfig step4 = HadoopJarStepConfig.builder()
                .jar(String.format("s3://%s/%s/calculate-npmi-1.0.jar", config.bucketName(), config.jarsPath()))
                .args(String.format("s3n://assignment2-emr/output/count-N-cw1w2/%s", dateSuffix),
                        String.format("s3n://assignment2-emr/output/count-cw1/%s", dateSuffix),
                        String.format("s3n://assignment2-emr/output/count-cw2/%s", dateSuffix),
                        String.format("s3n://assignment2-emr/output/calculate-npmi/%s", dateSuffix),
                        Long.toString(config.maxSplitSize()))
                .build();

        StepConfig stepConfig4 = StepConfig.builder()
                .name("Calculate NPMI")
                .hadoopJarStep(step4)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        // Step 5
        HadoopJarStepConfig step5 = HadoopJarStepConfig.builder()
                .jar(String.format("s3://%s/%s/filter-npmi-1.0.jar", config.bucketName(), config.jarsPath()))
                .args(String.format("s3n://assignment2-emr/output/calculate-npmi/%s", dateSuffix),
                        String.format("s3n://assignment2-emr/output/filter-npmi/%s", dateSuffix),
                        Boolean.toString(config.useCombiner()), Long.toString(config.maxSplitSize()),
                        Double.toString(minNpmi), Double.toString(relativeMinNpmi), Double.toString(thresholdNpmi))
                .build();

        StepConfig stepConfig5 = StepConfig.builder()
                .name("Filter NPMI")
                .hadoopJarStep(step5)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        // Step 6
        HadoopJarStepConfig step6 = HadoopJarStepConfig.builder()
                .jar(String.format("s3://%s/%s/sort-npmi-1.0.jar", config.bucketName(), config.jarsPath()))
                .args(String.format("s3n://assignment2-emr/output/filter-npmi/%s", dateSuffix),
                        String.format("s3n://assignment2-emr/output/sort-npmi/%s", dateSuffix),
                        Long.toString(config.maxSplitSize()))
                .build();

        StepConfig stepConfig6 = StepConfig.builder()
                .name("Sort NPMI")
                .hadoopJarStep(step6)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        // Step 7
        HadoopJarStepConfig step7 = HadoopJarStepConfig.builder()
                .jar(String.format("s3://%s/%s/top-n-1.0.jar", config.bucketName(), config.jarsPath()))
                .args(String.format("s3n://assignment2-emr/output/sort-npmi/%s", dateSuffix),
                        String.format("s3n://assignment2-emr/output/top-n-npmi/%s", dateSuffix),
                        Long.toString(config.maxSplitSize()), Integer.toString(config.topNpmi()))
                .build();

        StepConfig stepConfig7 = StepConfig.builder()
                .name("Top N NPMI")
                .hadoopJarStep(step7)
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
                .steps(stepConfig1, stepConfig2, stepConfig3, stepConfig4, stepConfig5, stepConfig6, stepConfig7)
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
