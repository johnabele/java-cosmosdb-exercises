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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.swing.text.DocumentFilter;

import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.ExcludedPath;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.IncludedPath;
import com.microsoft.azure.cosmosdb.IndexingDirective;
import com.microsoft.azure.cosmosdb.IndexingMode;
import com.microsoft.azure.cosmosdb.IndexingPolicy;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.SqlParameter;
import com.microsoft.azure.cosmosdb.SqlParameterCollection;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import rx.Observable;

public class IndexManager {
    // CosmosDB will, by default, create a HASH index for every numeric and string field
    // This default index policy is best for;
    // - Equality queries against strings
    // - Range & equality queries on numbers
    // - Geospatial indexing on all GeoJson types. 
    //
    // This sample project demonstrates how to customize and alter the index policy on a DocumentCollection.
    //
    // 1. Exclude a document completely from the Index
    // 2. Use manual (instead of automatic) indexing
    // 3. Use lazy (instead of consistent) indexing
    // 4. Exclude specified paths from document index

    public static AsyncDocumentClient client;
    private static final String databaseName = "IndexManagement";
    public String databaseUri = "dbs/" + databaseName;
    final CountDownLatch successfulCompletionLatch = new CountDownLatch(1);
    private FeedOptions queryOptions = new FeedOptions();

    public IndexManager(){   
        client = new AsyncDocumentClient.Builder()
        .withServiceEndpoint(AccountSettings.HOST)
        .withMasterKey(AccountSettings.MASTER_KEY)
        .withConnectionPolicy(ConnectionPolicy.GetDefault())
        .withConsistencyLevel(ConsistencyLevel.Session)
        .build();

        cleanUpGeneratedDatabases();
        
        Database createdDatabase = new Database();
        createdDatabase.setId(databaseName);
        createdDatabase = client.createDatabase(createdDatabase, null).toBlocking().single().getResource();
        
        queryOptions.setMaxItemCount(500);
        queryOptions.setEnableCrossPartitionQuery(true);   
    }

    public void ExplicitlyExcludeFromIndex() throws IOException, InterruptedException{  
        //Create a collection with Automatic Indexing enabled (default).
        String collectionUri = CreateCollection("ExplicitlyExcludeFromIndex", null);
        
        //Create a document with default indexing
        String doc1JsonString = "{'id':'doc1','order':'001','contact':'email'}";
        CreateDocument(collectionUri, doc1JsonString, null);

        //Create a second document with IndexingDirective.Exclude request options
        /*
            Add your code here for exercise 1.4.1
        */

        //Test a query for the excluded document.
        OutputResults(collectionUri, "SELECT * FROM root r WHERE r.contact='email'");
    }

    public void UseManualIndexing() throws IOException, InterruptedException{
        //Create a collection with Automatic Indexing disabled (default). 
        IndexingPolicy indexingPolicy = new IndexingPolicy();
        indexingPolicy.setAutomatic(false);
        String collectionUri = CreateCollection("UseManualIndexing", indexingPolicy);
        
        //Create a document with default indexing
        TimeUnit.SECONDS.sleep(3);
        String doc1JsonString = "{'id':'doc3','order':'003','contact':'phone'}";
        CreateDocument(collectionUri, doc1JsonString, null);

        //Create a second document with IndexingDirective.Include  
        /*
            Add your code here for exercise 1.4.2
        */

        OutputResults(collectionUri, "SELECT * FROM root r WHERE r.contact='phone'");
    }

    public void UseLazyIndexing() throws IOException, InterruptedException{
        IndexingPolicy indexingPolicy = new IndexingPolicy();
        //Create a collection with Lazy Indexing enabled (default).
        /*
            Add your code here for exercise 1.4.3
        */ 
        String collectionUri = CreateCollection("UseLazyIndexing", indexingPolicy);

        TimeUnit.SECONDS.sleep(3);
        String doc1JsonString = "{'id':'doc5','order':'005','contact':'text'}";
        CreateDocument(collectionUri, doc1JsonString, null);

        OutputResults(collectionUri, "SELECT * FROM root r WHERE r.contact='text'");
    }

    public void ExcludePathsFromIndex() throws InterruptedException{
        IndexingPolicy indexingPolicy = new IndexingPolicy();
        //Create a collection with Excluded paths.
        /*
            Add your code here for exercise 1.4.4
        
        */ 
        
        String collectionUri = CreateCollection("ExcludePathsFromIndex", indexingPolicy);
        TimeUnit.SECONDS.sleep(5);
        String doc1JsonString = "{'id':'doc6','order':'006','contact':'text','searchable':'value1','nonsearchable':'value2'}";
        CreateDocument(collectionUri, doc1JsonString, null);

        OutputResults(collectionUri, "SELECT * FROM root r WHERE r.contact='text'");
        OutputResults(collectionUri, "SELECT * FROM root r WHERE r.nonsearchable='value1'");
    }

    private void OutputResults(String collectionLink, String queryText) throws InterruptedException{
        TimeUnit.SECONDS.sleep(2);
        Observable<FeedResponse<Document>> queryObservable = client.queryDocuments(collectionLink,
        queryText, queryOptions);

        String callingMethodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        
        queryObservable.subscribe(queryResultPage -> {
            System.out.println(callingMethodName + ": Got a page of query result with " + 
            queryResultPage.getResults().size() + " document(s)" + " and request charge of \u001B[46m \u001B[30m " + 
            queryResultPage.getRequestCharge() + " \u001B[0m \u001B[40m");
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
    private void CreateDocument(String collectionUri, String jsonString, RequestOptions documentRequestOptions) throws InterruptedException{
        //Create a new document.  
        Document document = new Document(jsonString);
        
        Observable<ResourceResponse<Document>> createDocumentObservable = 
        client.createDocument(collectionUri, document, documentRequestOptions, false);

        createDocumentObservable.single() // We know there is only single result
        .subscribe(documentResourceResponse -> {
            System.out.println(documentResourceResponse.getActivityId() + " has been created.");
        }, error -> {
            System.err.println(
                    "an error occurred while creating the document: actual cause: " + error.getMessage());
        });
    }
    private String CreateCollection(String collectionName, IndexingPolicy indexingPolicy) throws InterruptedException{
 
        String collectionUri = databaseUri + "/colls/" + collectionName;
        //Create a new collection
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(collectionName);
        if(indexingPolicy!=null){
            collectionDefinition.setIndexingPolicy(indexingPolicy);
        }
        
        client.createCollection(databaseUri, collectionDefinition, null).subscribe(collectionResourceResponse -> {
            System.out.println(collectionDefinition.getId() + " has been created with indexing policy " + collectionDefinition.getIndexingPolicy().getIndexingMode());            
            successfulCompletionLatch.countDown();
        }, error -> {
            System.err.println(
                    "an error occurred while creating the collection: actual cause: " + error.getMessage());
        });

        successfulCompletionLatch.await();

        return collectionUri;
    }
}