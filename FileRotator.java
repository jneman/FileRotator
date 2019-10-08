package com.foxtv.lambda;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.CollectionUtils;

public class FileRotator implements RequestHandler<S3Event, String> {
	private final String JPG_TYPE = (String) ".jpg";
	private final Regions THE_REGION = Regions.US_EAST_2;

	public String handleRequest(S3Event s3event, Context context) {
			System.out.println("This is a test......");
		    S3EventNotificationRecord record = s3event.getRecords().get(0);
		
		    String srcBucket = record.getS3().getBucket().getName();
		
		    // Object key may have spaces or unicode non-ASCII characters.
		    String srcKey = record.getS3().getObject().getUrlDecodedKey();
		    
		    System.out.println("srcBucket=" + srcBucket + ", srcKey=" + srcKey);
		    AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(THE_REGION).build();
		    String prefix = getPrefixFileName(srcKey);
		    if (prefix == null)
		    	return ("OK");
		    Integer max_num = getMaxNum(srcKey);
		    if (max_num == null)
		    	return ("OK");
		    
		    ObjectListing listing = s3.listObjects( srcBucket, prefix );
		    List<S3ObjectSummary> summaries = listing.getObjectSummaries();
	    	int max_num_used = -1;
	    	
	    	AmazonS3 s3Client = getS3Client();
	    	if (s3Client == null) {
	    		return ("OK");
	    	}
		    if (CollectionUtils.isNullOrEmpty(summaries)) {
		    	copyObject(s3Client, srcBucket, srcKey, prefix + "1" + JPG_TYPE);
		    	return("OK");
		    }
		    else {
		    	Pattern p = Pattern.compile(prefix + "([0-9]+)" + JPG_TYPE);
		    	// Find the maximum number used in a name of a file
			    for (S3ObjectSummary keyName : summaries) {
			    	Matcher m = p.matcher(keyName.getKey());
			    	if (m.find()) {
			    		int n = Integer.parseInt(m.group(1));
			    		if (n > max_num_used) {
			    			max_num_used = n;
			    		}
			    	}
			    }
		    }
		    if (max_num_used == -1) { // no files found
		    	copyObject(s3Client, srcBucket, srcKey, prefix + "1" + JPG_TYPE);
		    	return("OK");
		    }
		    else {
		    	if (max_num_used >= max_num) {
		    		max_num_used = max_num - 1;
		    	}
		    	for (int i = max_num_used; i >= 1; i--) {
		    		copyObject(s3Client, srcBucket, prefix + i + JPG_TYPE, prefix + (i + 1) + JPG_TYPE);
		    	}
		    	copyObject(s3Client, srcBucket, srcKey, prefix + "1" + JPG_TYPE);
		    }
		    return "Ok";
	}

	private String getPrefixFileName(String s)  {
		if (!s.endsWith(JPG_TYPE) && !s.startsWith("filerotator")) {
			System.out.println("File is not a filerotator");
		}
		s = s.substring(0, s.lastIndexOf("."));
		int idx1  = s.lastIndexOf(".", s.length());	
		int idx2 = s.lastIndexOf("_", s.length());
		if (idx1 == -1 || idx2 == -1) {
			System.out.println("not a supported file rotator");
			return null;
		}
		s = s.substring(idx1 + 1, idx2);
		if (s != null)
			s = s + "_";
		System.out.println(s);
		return s;
	}
	
	private Integer getMaxNum(String s) {
        Pattern p = Pattern.compile("[^\\d]*[\\d]+[^\\d]+([\\d]+)");
        Matcher m = p.matcher(s);
        if (m.find()) {
            Integer num = Integer.valueOf(m.group(1));
            return(num);
        }
        else {
        	return null;
        }
	}
	
	private void copyObject(AmazonS3 s3Client, String bucketName, String sourceKey, String destinationKey) {
			System.out.println("copyObject: bucketName=" + bucketName + ", sourceKey=" + sourceKey
					+ ", destinationKey=" + destinationKey);
	       try {
	            // Copy the object into a new object in the same bucket.
	            CopyObjectRequest copyObjRequest = new CopyObjectRequest(bucketName, sourceKey, bucketName, destinationKey);
	            s3Client.copyObject(copyObjRequest);
	        } catch (AmazonServiceException e) {
	            // The call was transmitted successfully, but Amazon S3 couldn't process 
	            // it, so it returned an error response.
	            e.printStackTrace();
	        } catch (SdkClientException e) {
	            // Amazon S3 couldn't be contacted for a response, or the client  
	            // couldn't parse the response from Amazon S3.
	            e.printStackTrace();
	        }
	  }
	
	private AmazonS3 getS3Client() {
		try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(THE_REGION)
                    .build();
            return s3Client;
		}
		catch (Exception exp){
			exp.printStackTrace();
			return null;
		}
	}
}
