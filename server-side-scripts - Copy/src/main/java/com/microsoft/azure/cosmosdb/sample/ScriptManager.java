/**
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.cosmosdb.sample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.swing.JSlider;
import javax.tools.JavaFileObject;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.PartitionKey;
import com.microsoft.azure.cosmosdb.PartitionKeyDefinition;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.SqlParameter;
import com.microsoft.azure.cosmosdb.SqlParameterCollection;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.StoredProcedure;
import com.microsoft.azure.cosmosdb.StoredProcedureResponse;
import com.microsoft.azure.cosmosdb.Trigger;
import com.microsoft.azure.cosmosdb.TriggerOperation;
import com.microsoft.azure.cosmosdb.TriggerType;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;

import org.apache.commons.io.FilenameUtils;

import rx.Observable;

public class ScriptManager {

    public static AsyncDocumentClient client;
    
    // Assign a variable for your database & collection 
    private static final String databaseName = "ServerSideScripts";
    private static final String collectionName = "jsexamples";

	private static final String Int = null;
    private String collectionLink;
    private FeedOptions queryOptions = new FeedOptions();
    final CountDownLatch successfulCompletionLatch = new CountDownLatch(1);
    ClassLoader classLoader = getClass().getClassLoader();

    public ScriptManager() throws InterruptedException{
        client = new AsyncDocumentClient.Builder()
        .withServiceEndpoint(AccountSettings.HOST)
        .withMasterKey(AccountSettings.MASTER_KEY)
        .withConnectionPolicy(ConnectionPolicy.GetDefault())
        .withConsistencyLevel(ConsistencyLevel.Session)
        .build();

        cleanUpGeneratedDatabases();

        collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        
        Database createdDatabase = new Database();
        createdDatabase.setId(databaseName);
        createdDatabase = client.createDatabase(createdDatabase, null).toBlocking().single().getResource(); 
        TimeUnit.SECONDS.sleep(3);
   
        //Create a new collection
        PartitionKeyDefinition partitionkeyDefinition = new PartitionKeyDefinition();
        List<String> paths = Arrays.asList("/DeviceId");
        partitionkeyDefinition.setPaths(paths);
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(collectionName);
        collectionDefinition.setPartitionKey(partitionkeyDefinition);
        
        Observable<ResourceResponse<DocumentCollection>> createCollectionObservable = 
        client.createCollection("/dbs/" + databaseName, collectionDefinition, null);

        createCollectionObservable.single() // We know there is only single result
        .subscribe(collectionResourceResponse -> {           
            successfulCompletionLatch.countDown();
        }, error -> {
            System.err.println(
                    "an error occurred while creating the collection: actual cause: " + error.getMessage());
        });

        successfulCompletionLatch.await();
    }

    public void RunSimpleScript() throws IOException, InterruptedException{
        // 1. Create stored procedure for script.
        File file = new File(classLoader.getResource("JS/SimpleScript.js").getFile());
        file.getPath();

        String scriptBody = new String(Files.readAllBytes(Paths.get(file.getPath())), StandardCharsets.UTF_8);
        String scriptId = file.getName();

        StoredProcedure sproc = new StoredProcedure();
        sproc.setId(FilenameUtils.removeExtension(scriptId));
        sproc.setBody(scriptBody);

        TryDeleteStoredProcedure(collectionLink + "/sprocs/" + sproc.getId(), sproc.getId());

        client.createStoredProcedure(collectionLink, sproc, null).subscribe(spResultPage -> {
            System.out.println("\n " + "stored procedure created " + spResultPage.getActivityId() + "\n");
        });
        // 2. Create a document.

        String jsonString = "{'LastName':'Estel','Headquarters':'Russia','Locations': {'Country':'Russia','City':'Novosibirsk'},'Income':'50000'}";
        Document document = new Document(jsonString);
        
        Observable<ResourceResponse<Document>> createDocumentObservable = 
        client.createDocument(collectionLink, document, null, false);

        createDocumentObservable.single() // We know there is only single result
        .subscribe(documentResourceResponse -> {
            System.out.println("Document created " + documentResourceResponse.getActivityId() + "\n");
        }, error -> {
            System.err.println(
                    "an error occurred while creating the document: actual cause: " + error.getMessage());
        });
        // 3. Run the script. Pass "Hello, " as parameter. 
        // The script will take the 1st document and echo: Hello, <document as json>.
        Object[] storedProcedureArgs = new Object[] { "Hello" };
        TimeUnit.SECONDS.sleep(5);
        client.executeStoredProcedure(collectionLink + "/sprocs/" + sproc.getId(), storedProcedureArgs)
        .subscribe(storedProcedureResponse -> {
            String storedProcResultAsString = storedProcedureResponse.getResponseAsString();
            successfulCompletionLatch.countDown();
            System.out.println(storedProcResultAsString);
        }, error -> {
            System.err.println("an error occurred while executing the stored procedure: actual cause: "
                    + error.getMessage());
        });
    }
    
    /*
    public void BulkImport() throws IOException, InterruptedException{
        ClassLoader classLoader = getClass().getClassLoader();
        int maxFiles = 2000;
        int maxScriptSize = 50000;

        // 1. Get the files.
        File folder = new File(classLoader.getResource("Data").getFile());
        File[] fileNames = folder.listFiles();

        // 2. Prepare for import.
        int currentCount = 0;
        int fileCount = maxFiles != 0 ? Math.min(maxFiles, fileNames.length) : fileNames.length;


        // 3. Create stored procedure for this script.
        
        File file = new File(classLoader.getResource("JS/BulkImport.js").getFile());

        String scriptBody = new String(Files.readAllBytes(Paths.get(file.getPath())), StandardCharsets.UTF_8);
        String scriptId = file.getName();

        StoredProcedure sproc = new StoredProcedure();
        sproc.setId(FilenameUtils.removeExtension(scriptId));
        sproc.setBody(scriptBody);

        client.createStoredProcedure(collectionLink, sproc, null).subscribe(spResultPage -> {
            System.out.println("\n " + "stored procedure created " + spResultPage.getActivityId() + "\n");
        });
        TimeUnit.SECONDS.sleep(5);
        // 4. Create a batch of docs (MAX is limited by request size (2M) and to script for execution.           
            // We send batches of documents to create to script.
            // Each batch size is determined by MaxScriptSize.
            // MaxScriptSize should be so that:
            // -- it fits into one request (MAX reqest size is 16Kb).
            // -- it doesn't cause the script to time out.
            // -- it is possible to experiment with MaxScriptSize to get best perf given number of throttles, etc.
            ObjectMapper objectMapper = new ObjectMapper();
            int currentlyInserted = 0;
            
                
            // 5. Create args for current batch.
            //    Note that we could send a string with serialized JSON and JSON.parse it on the script side,
            //    but that would cause script to run longer. Since script has timeout, unload the script as much
            //    as we can and do the parsing by client and framework. The script will get JavaScript objects.
            CreateBulkInsertScriptArguments(fileNames, currentCount, fileCount, maxScriptSize);
            // 6. execute the batch.
            

            
        
       
            TimeUnit.SECONDS.sleep(3);
            client.queryDocuments(collectionLink,
            "SELECT * FROM root r", null).subscribe(queryResultPage -> {
                System.out.println("Found " + queryResultPage.getResults().size() + " documents in the collection. There were originally  " + fileCount + "  files in the Data directory\r\n");
            });

    }
    */
    public void RunPostTrigger() throws IOException, InterruptedException{

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("JS/UpdateMetadata.js").getFile());

        String triggerBody = new String(Files.readAllBytes(Paths.get(file.getPath())), StandardCharsets.UTF_8);
        String triggerId = file.getName();


        Trigger trigger = new Trigger();
        trigger.setId(triggerId);
        trigger.setBody(triggerBody);
        trigger.setTriggerOperation(TriggerOperation.Create);
        trigger.setTriggerType(TriggerType.Post);

        TryDeleteTrigger(collectionLink + "/trigger/" + trigger.getId(), trigger.getId());

        client.createTrigger(collectionLink, trigger, null).subscribe(spResultPage -> {
            System.out.println("stored procedure deleted.");
        });

        // 2. Create the metadata document.
        TimeUnit.SECONDS.sleep(2);
        String jsonString = "{'Id':'Device001_metadata','IsMetadata':true, 'MinSize':0, 'MaxSize':0, 'TotalSize':0, 'DeviceId':'Device001'}";
        Document document = new Document(jsonString);

        client.createDocument(collectionLink, document, null, false).subscribe(docResultPage -> {
            System.out.println(docResultPage + " document created.");
        }); 

        List<String> triggers = Arrays.asList("UpdateMetadata.js");

        RequestOptions docRequestOptions = new RequestOptions(){};
        docRequestOptions.setPostTriggerInclude(triggers);
        TimeUnit.SECONDS.sleep(5);
        for (int i=0; i < 4; i++)
        {
            String jsonStringObj = "{'DeviceId':'Device001', 'Size': " + (int)(Math.random() * 1000) + "}";
            Document documentObj = new Document(jsonStringObj);

            client.createDocument(
                collectionLink,
                documentObj,
                docRequestOptions,false).subscribe(docResultPage -> {
                    System.out.println(docResultPage + " document created.");
                }); 
        }

        FeedOptions feedOptions = new FeedOptions();
        PartitionKey partitionkey = new PartitionKey("Device001");
        feedOptions.setPartitionKey(partitionkey);

        client.queryDocuments(collectionLink, 
            "SELECT * FROM root r WHERE r.isMetadata = true", feedOptions).subscribe(queryResultPage -> {
                //queryResultPage.getResults().get(0).get("TotalSize");
            });
/*
        Console.WriteLine("Document statistics: min size: {0}, max size: {1}, total size: {2}", 
            aggregateEntry.MinSize, 
            aggregateEntry.MaxSize, 
            aggregateEntry.TotalSize);

        Console.WriteLine("Press enter key to continue********************************************");
        Console.ReadKey();*/
    }


    private void cleanUpGeneratedDatabases() {
        
        String[] allDatabaseIds = { databaseName };

        for (String id : allDatabaseIds) {
            try {
                List<FeedResponse<Database>> feedResponsePages = client
                        .queryDatabases(new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                                new SqlParameterCollection(new SqlParameter("@id", id))), null)
                        .toList().toBlocking().single();

                if (!feedResponsePages.get(0).getResults().isEmpty()) {
                    Database res = feedResponsePages.get(0).getResults().get(0);
                    client.deleteDatabase("dbs/" + res.getId(), null).toBlocking().single();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    private void TryDeleteStoredProcedure(String sprocLink, String storedProcedureName){
        Observable<ResourceResponse<StoredProcedure>> sprocObservable = client.deleteStoredProcedure(sprocLink, null);
        sprocObservable.subscribe(spResultPage -> {
            System.out.println("stored procedure deleted.");
        });
    }

    private void TryDeleteTrigger(String triggerLink, String triggerId){
        Observable<ResourceResponse<Trigger>> sprocObservable = client.deleteTrigger(triggerLink, null);
        sprocObservable.subscribe(spResultPage -> {
            System.out.println("trigger deleted.");
        });
    }
    /*
    private Object[] CreateBulkInsertScriptArguments(File[] docFileNames, int currentIndex, int maxCount, int maxScriptSize) throws IOException{
        ArrayList<String> jsonObjects = new ArrayList<String>();
        //jsonDocumentArray.append("[");

        int i = 1;
        while ((currentIndex + i) < maxCount)
        {
            StringBuilder jsonDocumentArray = new StringBuilder();
            jsonDocumentArray.append(new String(Files.readAllBytes(Paths.get(docFileNames[currentIndex + i].getPath())), StandardCharsets.UTF_8));
            Object[] args = {jsonDocumentArray.toString()};
            client.executeStoredProcedure(collectionLink + "/sprocs/BulkImport", args)
            .subscribe(storedProcedureResponse -> {
                successfulCompletionLatch.countDown();
                //currentlyInserted = storedProcResultAsInt;
            }, error -> {
                System.err.println("an error occurred while executing the stored procedure: actual cause: "
                        + error.getMessage());
            });

            jsonObjects.add(jsonDocumentArray.toString());
            jsonDocumentArray.setLength(0);
            i++;
            
        }

        //jsonDocumentArray.append("]");
        Object[] jsonArrayObjects = jsonObjects.toArray();
        System.out.println(jsonArrayObjects[2]);
        return jsonArrayObjects;
    }*/
}