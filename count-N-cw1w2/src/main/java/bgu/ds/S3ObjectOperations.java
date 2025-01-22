package bgu.ds;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class S3ObjectOperations {
    private final S3Client s3Client;
    private static final Region region = Region.US_EAST_1;

    private static final S3ObjectOperations instance = new S3ObjectOperations();

    private S3ObjectOperations() {
        s3Client = S3Client.builder().region(region).build();
    }

    public static S3ObjectOperations getInstance() {
        return instance;
    }

    public byte[] getObjectAsByteArray(String bucketName, String key) {
        return s3Client.getObjectAsBytes(builder -> builder.bucket(bucketName).key(key)).asByteArray();
    }

}
