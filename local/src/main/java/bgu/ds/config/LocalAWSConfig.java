package bgu.ds.config;

public interface LocalAWSConfig {
    int numberOfInstances();
    String instanceType();
    String bucketName();
    String jarsPath();
    String logsPath();
}