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
    private static final String databaseName = "<DatabaseName>";
    private static final String collectionName = "<CollectionName>";
    private String collectionLink;
    private FeedOptions queryOptions = new FeedOptions();

    public QueryManager(){
        client = new AsyncDocumentClient.Builder()
        .withServiceEndpoint(AccountSettings.HOST)
        .withMasterKey(AccountSettings.MASTER_KEY)
        .withConnectionPolicy(ConnectionPolicy.GetDefault())
        .withConsistencyLevel(ConsistencyLevel.Session)
        .build();

        collectionLink = String.format("/dbs/%s/colls/%s", databaseName, collectionName);
        
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