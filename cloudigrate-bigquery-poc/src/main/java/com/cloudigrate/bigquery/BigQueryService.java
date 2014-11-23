package com.cloudigrate.bigquery;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.services.GoogleClientRequestInitializer;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest;
import com.google.api.client.googleapis.services.json.CommonGoogleJsonClientRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Datasets;
import com.google.api.services.bigquery.Bigquery.Projects;
import com.google.api.services.bigquery.Bigquery.Tables;
import com.google.api.services.bigquery.BigqueryRequest;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.ProjectList;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;

public class BigQueryService {

	 private static TableReference tableRef;
	 private static TableSchema tableSchema;
	  
	public static void main(String[] args) throws GeneralSecurityException, IOException {
		// TODO Auto-generated method stub
		final HttpTransport TRANSPORT = new NetHttpTransport();
		JsonFactory JSONFACTORY = new JacksonFactory();
		
		GoogleCredential credential = new GoogleCredential.Builder()
				.setTransport(TRANSPORT)
				.setJsonFactory(JSONFACTORY)
				.setServiceAccountId("")
			    .setServiceAccountPrivateKeyFromP12File(new File("/Users/rohietkothari/Downloads/big.p12"))
				.setServiceAccountScopes(Collections.singleton(BigqueryScopes.BIGQUERY))
				.build();
		
//		Bigquery bigquery = null;
//		GoogleClientRequestInitializer initializer = new CommonGoogleJsonClientRequestInitializer() {
//	        public void initialize(AbstractGoogleJsonClientRequest request) {
//	          BigqueryRequest bigqueryRequest = (BigqueryRequest) request;
//	          bigqueryRequest.setPrettyPrint(true);
//	        }
//	      };
//	    
//	    bigquery = new Bigquery.Builder(
//	    		TRANSPORT, JSONFACTORY, credential).setHttpRequestInitializer(credential)
//	              .setGoogleClientRequestInitializer(initializer)
//	              .setApplicationName("CIBQ")
//	              .build();
//		
	    
		Bigquery bigQuery = new Bigquery(TRANSPORT, JSONFACTORY, credential);
//		Bigquery biqQueryBuilder = new Bigquery.Builder(TRANSPORT, JSONFACTORY, credential)
//										.setApplicationName("CIBQ")
//										.build();
		//System.out.println("Executed:"+bigQuery.getApplicationName());
		
		//PRINTING BIGQUERY PROJECTS
		Projects.List projectListRequest = bigQuery.projects().list();
	    ProjectList projectList = projectListRequest.execute();
	    List<ProjectList.Projects> projects = projectList.getProjects();
	    System.out.println("Available projects\n----------------\n");
	    String projectId = "";
	    //int count = 0;
	    for (ProjectList.Projects project : projects) {
	    	projectId = project.getProjectReference().getProjectId();
	    	System.out.format("Project ID: %s\n", projectId);
	    	//count++;
	    }
		
	    //PRINTING DATASETS IN PARTICULAR BIGQUERY PROJECT
	    Datasets.List datasetListRequest = bigQuery.datasets().list(projectId);
	    DatasetList datasetList = datasetListRequest.execute();
	    List<DatasetList.Datasets> datasets= datasetList.getDatasets();
	    String datasetId=null;
	    //count = 0;
	    for (DatasetList.Datasets dataset : datasets) {
	    	datasetId = dataset.getDatasetReference().getDatasetId();
	    	System.out.format("Dataset ID: %s\n", dataset.getDatasetReference().getDatasetId());
	    }
	    
//	    Tables.List listTablesReply = bigQuery.tables().list(projectId, datasetId);
//	    TableList tableList = listTablesReply.execute();
//	    if (tableList.getTables() != null) {
//
//	        List tables = tableList.getTables();
//
//	        System.out.println("Tables list:");
//
//	        for (TableList.Tables table : tables) {
//	          System.out.format("%s\n", table.getId());
//	        }
//
//	      }
	}

}
