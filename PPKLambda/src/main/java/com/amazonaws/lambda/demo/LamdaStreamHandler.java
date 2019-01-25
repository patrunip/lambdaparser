package com.amazonaws.lambda.demo;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;

public class LamdaStreamHandler implements RequestStreamHandler {

    //JSONParser parser = new JSONParser();

	
    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {

        // TODO: Implement your stream handler. See https://docs.aws.amazon.com/lambda/latest/dg/java-handler-io-type-stream.html for more information.
    	// TODO: https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-create-api-as-simple-proxy-for-lambda.html#api-gateway-proxy-integration-lambda-function-java
        // This demo implementation capitalizes the characters from the input stream.
//        int letter = 0;
//        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
//        JSONObject responseObject = new JSONObject();
//        
//        try {
//        	JSONObject baseEventObj = (JSONObject)parser.parse(reader);
//        	
//        	JSONObject respBodyObj = new JSONObject();
//        	respBodyObj.put("statusCode", "200");
//        	respBodyObj.put("message", new String("Hello World"));
//        	responseObject.put("body", "Hello World Lamda Response Test ");
//        	
//		} catch (Exception e) {
//			e.printStackTrace();
//			// TODO: handle exception
//		} 
//        
//        OutputStreamWriter writer = new OutputStreamWriter(output, "UTF-8");
//        writer.write(responseObject.toJSONString());  
//        writer.close();
    	 try {
         	System.out.println("Trying Reading the File from S3 Bucket");
         	LambdaLogger awsLogger = context.getLogger();
         	awsLogger.log(" Logging to CloudWatch  ");
         	//S3Object s3 = readFromS3("poc-dmi-reference-data", "DMIReferenceData.json");
         	JSONObject s3 = readFromS3("pk-poc-ec-templates", "DMIReferenceData.json");
         	//sendFiletoQueue(s3);
         	org.json.simple.JSONObject responObj = new org.json.simple.JSONObject();
         	responObj.put("body", s3.toString());
         	OutputStreamWriter writer = new OutputStreamWriter(output, "UTF-8");
         	writer.write(responObj.toJSONString());
         	writer.close();
         	} catch (Exception e) {
         		
         	System.err.println("Error Occurred! Bucket not found: " + e);
         }
        
        
    }
    
    private static JSONObject readFromS3(String bucketName, String key) throws IOException {
    	S3Object s3object = null;
    	JSONObject json = null;
    	try {
		
    		String clientRegion = "us-east-1";
    		System.out.println("1");
    		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion).build();
    		//GetObjectRequest objReq = new GetObjectRequest(bucketName, key).withRange(0, 9);
    		//System.out.println(" Get Bucket Name"+objReq.getBucketName()+"  TimeOut"+objReq.getSdkClientExecutionTimeout().intValue());
    		//objReq.setSdkRequestTimeout(60000);
    		//s3object = s3Client.getObject(new GetObjectRequest(bucketName, key));
    		//System.out.println(" Getting Object Now String with Range Obj Req  "+objReq);
    		//System.out.println(" Get bucket Name "+objReq.getBucketName());
    		//System.out.println(" Get bucket NameReq Time Out-Setting Time Out "+objReq.getSdkRequestTimeout());
    		//objReq.setSdkRequestTimeout(50000);
    		//String objectString = s3Client.getObjectAsString(bucketName, key);
    		//System.out.println(" Getting Object Now "+objectString);
    		//s3object = s3Client.getObject(objReq);
    		
    		System.out.println("3");
        	InputStream is = s3Client.getObject(bucketName, key).getObjectContent();
        	System.out.println("4");
        	String jsonTxt = IOUtils.toString(is);
        	//System.out.println("JSON TEXT"+jsonTxt);
        	json = new JSONObject(jsonTxt);
		} catch (Exception e) {
			System.out.println(" Exception " +e);
			e.printStackTrace();
		}

    	    	return json;
    	}


}
