package fr.adventium.aws.bucket.service;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class BootStrap {
//		static Logger  LOG = LoggerFactory.getLogger(BootStrap.class); 
	static AWSCredentials credential = new BasicAWSCredentials("AKIAJSKURGHC636OENNA",
			"lL0Wa8aNw7o3PpOzhwXDi2mHG26GllpnKE6fCn6w");

	public static Bucket getBucket(String bucket_name) {
		final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credential)).withRegion(Regions.DEFAULT_REGION)
				.build();
		Bucket named_bucket = null;
		List<Bucket> buckets = s3.listBuckets();
		for (Bucket b : buckets) {
			System.out.println("bucket " + b.getName());

			if (b.getName().equals(bucket_name)) {
				named_bucket = b;
			}
		}
		return named_bucket;
	}

	public static void uploadDocument(File file, Bucket bucket) {
		final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credential)).withRegion(Regions.DEFAULT_REGION)
				.build();
		
		s3.putObject(bucket.getName(), file.getName(), file);
		
	}
	public static Bucket createBucket(String bucket_name) {
		final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credential)).withRegion(Regions.DEFAULT_REGION)
				.build();
		Bucket b = null;
		if (s3.doesBucketExistV2(bucket_name)) {
			System.out.format("Bucket %s already exists.\n", bucket_name);
			b = getBucket(bucket_name);
		} else {
			try {
				b = s3.createBucket(bucket_name);
			} catch (AmazonS3Exception e) {
				System.err.println(e.getErrorMessage());
			}
		}
		return b;
	}

	public static void downloadObject(String bucketName, String filePath, String destination ) {
		final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credential)).withRegion(Regions.DEFAULT_REGION)
				.build();
		S3Object s3object = s3.getObject(bucketName, filePath);
		S3ObjectInputStream inputStream =  s3object.getObjectContent();
		
		try {
			FileUtils.copyToFile(inputStream, new File(destination));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println( "ERROR " + e.getMessage());
		}
	}
	
	
	public static void main(String[] args) {
		final String USAGE = "\n" + "CreateBucket - create an S3 bucket\n\n" + "Usage: CreateBucket <bucketname>\n\n"
				+ "Where:\n" + "  bucketname - the name of the bucket to create.\n\n"
				+ "The bucket name must be unique, or an error will result.\n";

		if (args.length < 1) {
			System.out.println(USAGE);
			System.exit(1);
		}

		String bucket_name = args[0];
		System.out.format("\nCreating S3 bucket: %s\n", bucket_name);
		Bucket b = createBucket(bucket_name);
		if (b == null) {
			System.out.println("Error creating bucket!\n");
		} else {
			System.out.println("Done!\n");
		}
		String file_path = args[1];
		File fileUploaded= Paths.get(file_path).toFile();
		String fileNameDownloaded = "AUTHORS"; 
		
		uploadDocument(fileUploaded, b);
		downloadObject(bucket_name, fileNameDownloaded, args[2]);
		
	}

}
