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

/*	Main purpose: To try and explore Google BigQuery Java Client API Library
	Pre-requisite: Make sure you have manually created a Google Cloud project at Cloud Console

*/
public class BigQueryService {

	private static TableReference tableRef;
	private static TableSchema tableSchema;
	  
	public static void main(String[] args) throws GeneralSecurityException, IOException {
		// TODO Auto-generated method stub
		
		
		/*	Purpose of snippet: Authenticate to Google BigQuery service
			Required parameters:
				1. serviceAccountId: Please find this in your created project at Google Cloud console
				2. serviceAccountPrivateKeyFromP12File: Path to your downloaded a .p12 file
				3. serviceAccountScopes: Provide a scope for this project, i.e. BIGQUERY
		*/
		final HttpTransport TRANSPORT = new NetHttpTransport();
		JsonFactory JSONFACTORY = new JacksonFactory();
		GoogleCredential credential = new GoogleCredential.Builder()
				.setTransport(TRANSPORT)
				.setJsonFactory(JSONFACTORY)
				.setServiceAccountId("")
			    .setServiceAccountPrivateKeyFromP12File(new File("/Users/rohietkothari/Downloads/big.p12"))
				.setServiceAccountScopes(Collections.singleton(BigqueryScopes.BIGQUERY))
				.build();
		
		
		/*	Purpose of snippet: Create an instance of Bigquery client to connect to your project
			Required parameters:
				1. TRANSPORT
				2. JSONFACTORY
				3. credential
				
		*/	    
		Bigquery bigQuery = new Bigquery(TRANSPORT, JSONFACTORY, credential);


		/*	Purpose of snippet: To list all bigquery projects in an account
			Note: Bigquery APIs have defined their own 'List' for 'Projects', 'Datasets', etc.
			      Please observe the following snippet carefully. It represents a sample set of steps
			      on how to go about 'Lists'
		*/
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
		
		
	    	/*	Purpose of snippet: To list all datasets in a BigQuery instance
			Note: Bigquery APIs have defined their own 'List' for 'Projects', 'Datasets', etc.
			      Please observe the following snippet carefully. It represents a sample set of steps
			      on how to go about 'Lists'
		*/
	    	Datasets.List datasetListRequest = bigQuery.datasets().list(projectId);
	    	DatasetList datasetList = datasetListRequest.execute();
	    	List<DatasetList.Datasets> datasets= datasetList.getDatasets();
	    	String datasetId=null;
	    	//count = 0;
	    
	    	for (DatasetList.Datasets dataset : datasets) {
	    		datasetId = dataset.getDatasetReference().getDatasetId();
	    		System.out.format("Dataset ID: %s\n", dataset.getDatasetReference().getDatasetId());
	    	}
	    
	}

}
