package com.amazonaws.lambda.demo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.util.IOUtils;

public class LambdaFunctionHandler implements RequestHandler<Object, String> {

	public String handleRequest(Object input, Context context) {
		JSONObject s3 = null;
		System.out.println("Calling Function handler Method Again");
		try {
			s3 = readFromS3("pk-poc-ec-templates", "DMIReferenceData.json");
			
			//sendFiletoQueue(s3);

		} catch (Exception e) {
			System.err.println("Error Occurred! Bucket not found: " + e);
			//return "Error";
		}
		org.json.simple.JSONObject respBodyObj = new org.json.simple.JSONObject();
    	respBodyObj.put("statusCode", "200");
    	respBodyObj.put("contentType", "application/json");
    	respBodyObj.put("body", "Hello World Lamda Response Test ");
//    	
    	System.out.println(" Returning JSON Object To caller Boy " + s3.toString());
		return respBodyObj.toJSONString();
	}

	private static JSONObject readFromS3(String bucketName, String key) throws IOException {
		String clientRegion = "us-east-1";

		System.out.println("Creating S3 Client ");
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion).build();

		System.out.println("Account Owner" + s3Client.getS3AccountOwner());

		
		S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, key));

		System.out.println(" Account Number ");
		InputStream is = s3Client.getObject(bucketName, key).getObjectContent();

		String jsonTxt = IOUtils.toString(is);
		System.out.println(jsonTxt);

		JSONObject json = new JSONObject(jsonTxt);
		String a = json.toString();
		System.out.println(a);

		System.out.println(s3object.getObjectMetadata().getContentType());
		System.out.println(s3object.getObjectMetadata().getContentLength());

		return json;
	}

	private void sendFiletoQueue(JSONObject inputFile) {

		String queueName = "POCInboundQ";

		AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
		String queue_url = sqs.getQueueUrl(queueName).getQueueUrl();

		// Send a message
		System.out.println("Sending a message to POCInboundQ\n");
		final SendMessageRequest sendMessageRequest = new SendMessageRequest(queue_url, inputFile.toString());

		final SendMessageResult sendMessageResult = sqs.sendMessage(sendMessageRequest);
		final String sequenceNumber = sendMessageResult.getSequenceNumber();
		final String messageId = sendMessageResult.getMessageId();
		System.out.println(
				"SendMessage succeed with messageId " + messageId + ", sequence number " + sequenceNumber + "\n");

	}

	
	public void handleRequest1(InputStream input, OutputStream output, Context context) throws IOException {
		// TODO Auto-generated method stub
		System.out.println("Calling StreamHandler Method");
		handleRequest(input, context);
		
		org.json.simple.JSONObject respBodyObj = null;
		try {
		respBodyObj = new org.json.simple.JSONObject();
    	respBodyObj.put("statusCode", "200");
    	respBodyObj.put("message", new String("Hello World"));
    	respBodyObj.put("body", "Hello World Lamda Response Test ");
    	
	} catch (Exception e) {
		e.printStackTrace();
		// TODO: handle exception
	} 
    
    OutputStreamWriter writer = new OutputStreamWriter(output, "UTF-8");
    writer.write(respBodyObj.toJSONString());  
    writer.close();
		
	}

}