package com.amazonaws.lambda.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import org.apache.commons.mail.util.MimeMessageParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.lambda.runtime.events.ext.SESEvent;
import com.amazonaws.lambda.runtime.events.ext.SESEvent.SES;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class SESHandler implements RequestHandler<Map<String,Object>, String> {
	Gson gson = new GsonBuilder().setPrettyPrinting().create();

	private static final String BUCKET_NAME = "SampleBucket";
	private static final Logger logger = LoggerFactory.getLogger(SESHandler.class);

	/**
	 * handle lambda request
	 * 
	 * @param request SES message
	 * @param context Lambda context
	 * @return deserialized SESEvent
	 * @author Ahmet Alp
	 */
	@Override
	public String handleRequest(Map<String,Object> request, Context context) {
		String jsonString = gson.toJson(request,LinkedHashMap.class);
		SESEvent sesEvent = SESEvent.parseJson(jsonString);

		return handleRequest(sesEvent);
	}
	
	public String handleRequest(SESEvent request) {
		SES ses = null;
		try {
			ses = request.getRecords().get(0).getSES();
			StringBuilder srcKey = new StringBuilder();
			srcKey.append("S3_PREFIX/");
			srcKey.append(ses.getMail().getMessageId());

			//Get incoming email from S3
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
			S3Object s3Object = s3Client.getObject(new GetObjectRequest(BUCKET_NAME, srcKey.toString()));
			InputStream objectData = s3Object.getObjectContent();
			//TODO get mail S3 file and parse

			MimeMessage mimeMessageObj = new MimeMessage(null, objectData);
			MimeMessageParser mimeParser = new MimeMessageParser(mimeMessageObj);
			mimeParser.parse();
			
			//Work with DynamoDB
			AmazonDynamoDB ddbClient = AmazonDynamoDBClientBuilder.standard().build();
			DynamoDB dynamoDB = new DynamoDB(ddbClient);
			Table table = dynamoDB.getTable("ATable");
			//TODO Insert same data from email for example 

		} catch (IOException e) {
			logger.error("Unable to deserialize ses event", e);
		} catch (MessagingException e) {
			logger.error("Handler error:MessagingException-", e); 
		} catch (Exception e) {
			logger.error("Handler error:Exception-", e);
		} 
		return "sesEvent OK";		
	}
}
