package bgu.ds.config;

public interface LocalAWSConfig {
    int numberOfInstances();
    String instanceType();
    String bucketName();
    String jarsPath();
    String logsPath();
    String inputCorpusPath();
    String stopWordsPath();
    double defaultMinNpmi();
    double defaultRelativeMinNpmi();
    int topNpmi();
}