package com.amazonaws.lambda.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;

public class LamdaStreamHandler1 implements RequestStreamHandler {

    JSONParser parser = new JSONParser();

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {

        // TODO: Implement your stream handler. See https://docs.aws.amazon.com/lambda/latest/dg/java-handler-io-type-stream.html for more information.
    	// TODO: https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-create-api-as-simple-proxy-for-lambda.html#api-gateway-proxy-integration-lambda-function-java
        // This demo implementation capitalizes the characters from the input stream.
        int letter = 0;
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        JSONObject responseObject = new JSONObject();
        
        try {
        	JSONObject baseEventObj = (JSONObject)parser.parse(reader);
        	
        	JSONObject respBodyObj = new JSONObject();
        	respBodyObj.put("statusCode", "200");
        	respBodyObj.put("message", new String("Hello World"));
        	responseObject.put("body", "Hello World Lamda Response Test ");
        	
		} catch (Exception e) {
			e.printStackTrace();
			// TODO: handle exception
		} 
        
        OutputStreamWriter writer = new OutputStreamWriter(output, "UTF-8");
        writer.write(responseObject.toJSONString());  
        writer.close();
        
    }
    
//    private static S3Object readFromS3(String bucketName, String key) throws IOException {
//    	String clientRegion = "us-east-1";
//
//    	AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion).build();
//
//    	S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, key));
//
//    	InputStream is = s3Client.getObject(bucketName, key).getObjectContent();
//
//    	String jsonTxt = IOUtils.toString(is);
//    	System.out.println(jsonTxt);
//
//    	JSONObject json = new JSONObject(jsonTxt);
//    	String a = json.toString();
//    	System.out.println(a);
//
//    	System.out.println(s3object.getObjectMetadata().getContentType());
//    	System.out.println(s3object.getObjectMetadata().getContentLength());
//
//    	return s3object;
//    	}


}
