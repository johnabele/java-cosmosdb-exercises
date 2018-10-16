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
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import rx.Observable;

public class QueryManager {

    public static AsyncDocumentClient client;
    
    // Assign a variable for your database & collection 
    private static final String databaseId = "<databaseId>";
    private static final String collectionId = "<collectionId>";
    private String collectionLink;
    private FeedOptions queryOptions = new FeedOptions();

    public QueryManager(){
        client = new AsyncDocumentClient.Builder()
        .withServiceEndpoint(AccountSettings.HOST)
        .withMasterKey(AccountSettings.MASTER_KEY)
        .withConnectionPolicy(ConnectionPolicy.GetDefault())
        .withConsistencyLevel(ConsistencyLevel.Session)
        .build();

        collectionLink = String.format("/dbs/%s/colls/%s", databaseId, collectionId);
        
        queryOptions.setMaxItemCount(500);
        queryOptions.setEnableCrossPartitionQuery(true);   
    }

    public void QueryWithOneFilter() throws IOException{
        //TODO Add one filter query text below
        String queryText = "";
        
        OutputResults(queryText);
    }
    public void QueryWithTwoFilters() throws IOException{
        //TODO Add two filter query text below
        String queryText = "";
        
        OutputResults(queryText);
    }
    public void QueryWithRangeOperator() throws IOException{
        //TODO Add range operator query text below
        String queryText = "";

        OutputResults(queryText);
    }
    public void QueryWithSingleJoin() throws IOException{
        //TODO Add single join query text below
        String queryText = "";
        
        OutputResults(queryText);
    }
    public void QueryWithDoubleJoin() throws IOException{
        //TODO Add double join query text below
        String queryText = "";
        
        OutputResults(queryText);
    }

    private void OutputResults(String queryText){
        Observable<FeedResponse<Document>> queryObservable = client.queryDocuments(collectionLink,
        queryText, queryOptions);

        String callingMethodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        
        queryObservable.subscribe(queryResultPage -> {
            System.out.println(callingMethodName + ": Got a page of query result with " + 
            queryResultPage.getResults().size() + " document(s)" + " and request charge of \u001B[46m \u001B[30m " + 
            queryResultPage.getRequestCharge() + " \u001B[0m \u001B[40m");
        });
    }
}