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
    double defaultThresholdNpmi();
    double defaultSampleRate();
    int topNpmi();
    boolean useCombiner();
    long maxSplitSize();
}