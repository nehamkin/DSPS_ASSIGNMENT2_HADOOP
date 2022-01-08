import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class S3Emptier {
    public static AwsCredentialsProvider credentialsProvider;
    public static S3Client S3;
    public static Ec2Client ec2;
    public static EmrClient emr;

    public static void listAndDeleteBucketObjects( String bucketName ) {

        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsResponse res = S3.listObjects(listObjects);
            List<S3Object> objects = res.contents();

            for (ListIterator iterVals = objects.listIterator(); iterVals.hasNext(); ) {
                S3Object myValue = (S3Object) iterVals.next();
                System.out.print("\n The name of the key is " + myValue.key());
                System.out.print("\n The owner is " + myValue.owner());
                deleteBucketObject(bucketName, myValue.key());
            }

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static void deleteBucketObject(String bucketName, String objectName) {

        ArrayList<ObjectIdentifier> toDelete = new ArrayList<ObjectIdentifier>();
        toDelete.add(ObjectIdentifier.builder().key(objectName).build());

        try {
            DeleteObjectsRequest dor = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(Delete.builder().objects(toDelete).build())
                    .build();
            S3.deleteObjects(dor);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        System.out.println("Done!");
    }


    public static DeleteBucketResponse deleteBucket(String bucket) {
        listAndDeleteBucketObjects(bucket);
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
        return S3.deleteBucket(deleteBucketRequest);
    }
    public static void main (String[] args){
        S3 = S3Client.builder()
                .region(Region.US_EAST_1)
                    .build();
        ListBucketsResponse buckets = S3.listBuckets();
            System.out.println("Your {S3} buckets are:");
            for (Bucket b : buckets.buckets()) {
                if(!b.name().equals("orrinehamkinstepjarsbucket"))
                    deleteBucket(b.name());
        }
    }
}
