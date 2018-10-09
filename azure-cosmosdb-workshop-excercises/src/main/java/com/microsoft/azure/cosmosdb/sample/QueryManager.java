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
    private static final String databaseName = "AzureSampleFamilyDB";
    private static final String collectionName = "FamilyCollection";
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
        String queryText = "SELECT * FROM c WHERE c.name = 'Dorothy Romero'";
        
        OutputResults(queryText);
    }
    public void QueryWithTwoFilters() throws IOException{
        //TODO Add two filter query text below
        String queryText = "SELECT * FROM c WHERE c.name = 'Dorothy Romero' OR c.balance = '$1,444.96'";
        
        OutputResults(queryText);
    }
    public void QueryWithRangeOperator() throws IOException{
        //TODO Add range operator query text below
        String queryText = "SELECT * FROM c WHERE c.registered >= '2017-02-18T05:49:33 +4:00'";

        OutputResults(queryText);
    }
    public void QueryWithSingleJoin() throws IOException{
        //TODO Add single join query text below
        String queryText = "SELECT f.id FROM customers f JOIN c IN f.companies";
        
        OutputResults(queryText);
    }
    public void QueryWithDoubleJoin() throws IOException{
        //TODO Add double join query text below
        String queryText = "SELECT f.name AS customer, c.companyName AS company, p.location AS location FROM customers f JOIN c IN f.companies JOIN p IN c.locations";
        
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