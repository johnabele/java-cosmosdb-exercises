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

import java.util.List;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.SqlParameter;
import com.microsoft.azure.cosmosdb.SqlParameterCollection;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.StoredProcedure;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;

import org.apache.commons.io.FilenameUtils;

import rx.Observable;

public class ScriptManager {

    public static AsyncDocumentClient client;
    
    // Assign a variable for your database & collection 
    private static final String databaseName = "ServerSideScripts";
    private static final String collectionName = "jsexamples";

    private String collectionLink;
    final CountDownLatch successfulCompletionLatch = new CountDownLatch(1);
    
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
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(collectionName);
        
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
        ClassLoader classLoader = getClass().getClassLoader();        
        File file = new File(classLoader.getResource("JS/SimpleScript.js").getFile());

        String scriptBody = new String(Files.readAllBytes(Paths.get(file.getPath())), StandardCharsets.UTF_8);
        String scriptId = file.getName();

        StoredProcedure sproc = new StoredProcedure();
        sproc.setId(FilenameUtils.removeExtension(scriptId));
        sproc.setBody(scriptBody);

        TryDeleteStoredProcedure(collectionLink + "/sprocs/" + sproc.getId(), sproc.getId());

        client.createStoredProcedure(collectionLink, sproc, null).subscribe(spResultPage -> {
            System.out.println("\n" + "Stored procedure created " + spResultPage.getActivityId() + "\n");
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
}