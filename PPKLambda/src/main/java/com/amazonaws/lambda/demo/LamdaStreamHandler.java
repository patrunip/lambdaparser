package com.amazonaws.lambda.demo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.util.IOUtils;
import com.pwc.aws.poc.model.AircraftTypes;

public class LamdaStreamHandler implements RequestStreamHandler {

	// JSONParser parser = new JSONParser();

	private final String BUCKET_NAME = "pk-poc-ec-templates";//"pk-poc-ec-templates or app-test-poc-bucket";
	private final String OBJECT_ID = "DMIReferenceData.json";
	private static String REGION = "us-east-1";

	public static void main(String ara[]) {
		LamdaStreamHandler objStream = new LamdaStreamHandler();
		InputStream ioStream = new InputStream() {
			
			@Override
			public int read() throws IOException {
				// TODO Auto-generated method stub
				return 0;
			}
		};
		OutputStream outStream = new OutputStream() {
			
			@Override
			public void write(int arg0) throws IOException {
				// TODO Auto-generated method stub
				
			}
		};
		try {
			objStream.handleRequest(ioStream, outStream, null);	
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	@Override
	@SuppressWarnings("unchecked")
	public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {

		try {
			sop(" Reading DMI from S3 Bucket and Displaying Aircraft ID");
			if(context!=null){
				LambdaLogger awsLogger = context.getLogger();
				awsLogger.log(" Logging to CloudWatch  ");
			}
			
			// Get JSON from S3 Bucket
			JSONObject s3 = readFromS3(BUCKET_NAME, OBJECT_ID);
			// sendFiletoQueue(s3);

			// Create JSON Object using the JSON String
			org.json.simple.JSONObject responObj = new org.json.simple.JSONObject();
			responObj.put("body", s3.toString());

			// Stream out the JSON response
			OutputStreamWriter writer = new OutputStreamWriter(output, "UTF-8");
			writer.write(responObj.toJSONString());
			writer.close();
		} catch (Exception e) {
			System.err.println("Exception in Handle Request : " + e);
		}

	}

	/**
	 * 
	 * @param bucketName
	 * @param key
	 * @return
	 * @throws IOException
	 */
	private JSONObject readFromS3(String bucketName, String key) throws IOException {
		JSONObject json = null;
		JSONObject aircraftJSON = null;
		try {
			sop("Fetching Object Details ");
			String clientRegion = REGION;
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion).build();
			
			sop(" Fetching S3Client ");
			InputStream is = s3Client.getObject(bucketName, key).getObjectContent();
			
			sop(" Fetching S3Client ");
			String jsonTxt = IOUtils.toString(is);
			json = new JSONObject(jsonTxt);
			aircraftJSON =getAircraftTypes(json);
			
		} catch (Exception e) {
			sop(" Exception " + e);
			e.printStackTrace();
		}

		return aircraftJSON;
	}
	
	private JSONObject getAircraftTypes(JSONObject incomingJSON) {
		
		List<AircraftTypes> listAircraft = new ArrayList<AircraftTypes>();
		Map<Integer, AircraftTypes> aircraftTypesMap = new HashMap<Integer, AircraftTypes>();
		List<JSONObject> aircraftTypesList = parseJsonData(incomingJSON, "aircraftTypes");
		for (JSONObject j : aircraftTypesList) {
			sendMessageToQueue(j);
			AircraftTypes at = new AircraftTypes(j.get("aircraftTypeId").toString(), j.getString("label"),
					j.getString("description"), j.getString("iataCode"), j.getString("icaoCode"),
					j.get("typeOpEmptyWt").toString(), j.get("typeZeroFuelWt").toString(), j.get("typeMaxTaxiWt").toString(),
					j.get("typeMaxTow").toString(), j.get("typeMaxLandingWt").toString(), j.get("seats").toString(),
					j.getString("defaultNavCode"), j.get("defaultHoldFuel").toString(), j.get("typeFuelCapacity").toString(),
					j.get("acTypeCertId").toString(), j.get("typeMaxMainTankWt").toString());
			listAircraft.add(at);
			aircraftTypesMap.put(Integer.parseInt(at.getAircraftTypeId()), at);
		}
		
		return aircraftTypesList.get(0);
		
	}
	
	public List<JSONObject> parseJsonData(JSONObject obj, String pattern) throws JSONException {

		List<JSONObject> listObjs = new ArrayList<JSONObject>();
		JSONArray geodata = obj.getJSONArray(pattern);
		for (int i = 0; i < geodata.length(); ++i) {
			final JSONObject site = geodata.getJSONObject(i);
			listObjs.add(site);
		}
		return listObjs;
	}
/**
 * 
 * @param jsonObj
 */
	
	private void sendMessageToQueue(JSONObject jsonObj) {
		String queueName = "POCInboundQ";
		sop(" Creating SQS Objects ");
		String clientRegion = REGION;
		AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(clientRegion).build();
		sop(" Creating SQS Objects1 "+sqs);
		String queue_url = sqs.getQueueUrl(queueName).getQueueUrl();
		// Send a message
		sop("Sending a message to POCInboundQ\n");
		final SendMessageRequest sendMessageRequest = new SendMessageRequest(queue_url, jsonObj.toString());
		final SendMessageResult sendMessageResult = sqs.sendMessage(sendMessageRequest);
		final String sequenceNumber = sendMessageResult.getSequenceNumber();
		final String messageId = sendMessageResult.getMessageId();
		sop("SendMessage succeed with messageId " + messageId + ", sequence number " + sequenceNumber + "\n");

	}
	
	private void sop(String message) {
		System.out.println(message);
	}

}
