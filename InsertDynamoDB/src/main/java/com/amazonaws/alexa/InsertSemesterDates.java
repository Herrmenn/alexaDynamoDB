package com.amazonaws.alexa;

import java.io.IOException;
import java.sql.SQLException;
 
import org.jsoup.*;
import org.jsoup.helper.*;
import org.jsoup.internal.*;
import org.jsoup.nodes.*;
import org.jsoup.parser.*;
import org.jsoup.safety.*;
import org.jsoup.select.*;
 
import java.sql.*; // for standard JDBC programs
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.math.*; // for BigDecimal and BigInteger support
import java.util.HashMap;
import java.util.Map;
 
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
 
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
/*
 * Copyright 2012-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
/**
 * This sample demonstrates how to perform a few simple operations with the
 * Amazon DynamoDB service.
 */
public class InsertSemesterDates {
 
 
    static AmazonDynamoDB dynamoDB;
    static final String SEMESTER_DATES_ARCHIVE =  "https://www.htwsaar.de/studium/organisation/semestertermine";
    static long timeBefore = System.currentTimeMillis();
    static int count = 0;
    /**
     * The only information needed to create a client are security credentials
     * consisting of the AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoints, are performed
     * automatically. Client parameters, such as proxies, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.ProfilesConfigFile
     * @see com.amazonaws.ClientConfiguration
     */
    private static void init() throws Exception {
   
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\herrmenn\\.aws\\credentials), and is in valid format.",
                    e);
        }
        dynamoDB = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion("eu-central-1")
            .build();
    }
 
    public static void main(String[] args) throws Exception {
        init();
 
        try {
            String tableName = "semester_dates";
 
            // Create a table with a primary hash key named 'name', which holds a string
            CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                .withKeySchema(new KeySchemaElement().withAttributeName("semester").withKeyType(KeyType.HASH))
                .withAttributeDefinitions(new AttributeDefinition().withAttributeName("semester").withAttributeType(ScalarAttributeType.S))
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
 
            // Create table if it does not exist yet
            TableUtils.createTableIfNotExists(dynamoDB, createTableRequest);
            // wait for the table to move into ACTIVE state
            TableUtils.waitUntilActive(dynamoDB, tableName);
 
            try {
               
                // Persons
                Document semesterArchive = Jsoup.connect(SEMESTER_DATES_ARCHIVE).get();
               
                Elements infoBlocks = semesterArchive.select("table.listing");
               
                Map<String, AttributeValue> item;
                PutItemRequest putItemRequest;
                PutItemResult putItemResult;
               
                for (Element block: infoBlocks) {
                                       
                    //Elements tableElements = block.children();
                               
                    String semester          = block.previousElementSibling().text();
                    String semester_start    = block.getElementsContainingOwnText("Beginn des").next().text();
                    String lectures_start    = block.getElementsContainingOwnText("Beginn der").next().text();
                    String lectures_end      = block.getElementsContainingOwnText("Ende der").next().text();
                    String semester_end      = block.getElementsContainingOwnText("Ende des").next().text();
                    String lecture_free_time = block.getElementsContainingOwnText("Vorlesungsfreie").next().text();
                    String closing           = block.select("td.linksbundig[align=center]").html();
                   
                    if(lecture_free_time.isEmpty()) {
                        lecture_free_time = " ";
                    }
                    if (closing.isEmpty()) {
                        closing = " ";
                    }
                   
                   // Add another item
                    item = newItem(semester,semester_start,lectures_start,lectures_end,semester_end,lecture_free_time, closing);
                    putItemRequest = new PutItemRequest(tableName, item);
                    putItemResult = dynamoDB.putItem(putItemRequest);
                    System.out.println("Result: " + putItemResult);
                   
                count++;
                }
 
        } catch (IOException e) {
            System.out.println(e);
        }
               
        // print needed time
        long timeNeeded = (System.currentTimeMillis() - timeBefore);
        System.out.println("\nTotal time needed: " + timeNeeded + " ms.");
       
    System.out.println("Connection closed.\n" + count + " semester dates have been inserted.\n\n");
 
 
            // Scan items for movies with a year attribute greater than 1985
           /* HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
            Condition condition = new Condition()
                .withComparisonOperator(ComparisonOperator.GT.toString())
                .withAttributeValueList(new AttributeValue().withN("1985"));
            scanFilter.put("year", condition);
            ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
            ScanResult scanResult = dynamoDB.scan(scanRequest);
            System.out.println("Result: " + scanResult);*/
 
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to AWS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with AWS, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
 
    private static Map<String, AttributeValue> newItem(String semester, String semester_start, String lectures_start,
                                                       String lectures_end, String semester_end, String lecture_free_time,
                                                       String closure_days) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("semester", new AttributeValue(semester));
        item.put("semester_start", new AttributeValue(semester_start));                        //.withN(Integer.toString(year)));
        item.put("lectures_start", new AttributeValue(lectures_start));
        item.put("lectures_end", new AttributeValue(lectures_end));                            //.withSS(fans));
        item.put("semester_end", new AttributeValue(semester_end));
        item.put("lecture_free_time", new AttributeValue(lecture_free_time));
        item.put("closure_days", new AttributeValue(closure_days));
        return item;
    }
 
}