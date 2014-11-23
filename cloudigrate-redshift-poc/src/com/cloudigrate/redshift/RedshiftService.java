package com.cloudigrate.redshift;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.redshift.AmazonRedshiftClient;
import com.amazonaws.services.redshift.model.Cluster;
import com.amazonaws.services.redshift.model.CreateClusterRequest;
import com.amazonaws.services.redshift.model.DescribeClustersRequest;
import com.amazonaws.services.redshift.model.DescribeClustersResult;

public class RedshiftService {

	BasicAWSCredentials creds;
	AmazonRedshiftClient client;
	//Global declarations
	public String dbURL;
	public String MasterUsername;
	public String MasterUserPassword = "CloudIgrate4";
	
	public static void main(String[] args) throws InterruptedException, IOException {
		// TODO Auto-generated method stub
		
		
	    
		RedshiftService redshift = new RedshiftService();
		
		//Code for displaying menu - referred Vaibhav's POC
		String userInputString = null;
		int userChoice=0;
		Scanner userIn = new Scanner(System.in);
		InputStreamReader istream = new InputStreamReader(System.in) ;
		BufferedReader bufRead = new BufferedReader(istream) ;
		System.out.println("AWS Redshift Operations");
		do
		{
			System.out.println("1. Create Redshift Cluster");
			System.out.println("2. Describe the cluster");
			System.out.println("3. Test connectivity");
			//System.out.println("4. Create AMI from currently running instance");
			userChoice = userIn.nextInt();
			switch(userChoice)
			{
				case 1:
					redshift.authenticateRedshift();
					redshift.createRedshift();
					break;
				
				case 2:
					redshift.authenticateRedshift();
					//redshift.createRedshift();
					redshift.describeRedshiftClusters();
					break;
					
				case 3: 
					redshift.authenticateRedshift();
					redshift.connectToRedshift();
					break;
			}
			System.out.println("Do you want to continue? (y/n)");
			userInputString= bufRead.readLine();
		}while(userInputString.equals("y"));
		

	}
	
	public void authenticateRedshift() {
		creds = new BasicAWSCredentials("", "");
		
		client = new AmazonRedshiftClient(creds);
		client.setEndpoint("https://redshift.us-east-1.amazonaws.com/");
	}
	
	public void createRedshift() {
		CreateClusterRequest request = new CreateClusterRequest();
		request.withClusterIdentifier("ciusingsdk")
				.withMasterUsername("cloudigrate")
				.withMasterUserPassword("CloudIgrate4")
				.withNodeType("dw2.large")
				.withClusterType("single-node")
				.withVpcSecurityGroupIds("sg-f8e65a9d");
		Cluster createResponse = client.createCluster(request);
		System.out.println("Created cluster " + createResponse.getClusterIdentifier());
	}
	
	public void describeRedshiftClusters() {
		DescribeClustersRequest describeClusterRequest = new DescribeClustersRequest();
		
		describeClusterRequest.withClusterIdentifier("ciusingsdk");
		//DescribeClustersRequest dcr = new DescribeClustersRequest();
		DescribeClustersResult describeClustersResponse = client.describeClusters(describeClusterRequest);
		int index=0;
		//while(!describeClustersResponse.getClusters().isEmpty()){
			//System.out.println(describeClustersResponse.getClusters().get(index));
			String clusterIdentifier = describeClustersResponse.getClusters().get(index).getClusterIdentifier();
			String nodeType = describeClustersResponse.getClusters().get(index).getNodeType();
			MasterUsername = describeClustersResponse.getClusters().get(index).getMasterUsername();
			String dbName = describeClustersResponse.getClusters().get(index).getDBName();
			String endpointAddress = describeClustersResponse.getClusters().get(index).getEndpoint().getAddress();
			int endpointPort = describeClustersResponse.getClusters().get(index).getEndpoint().getPort();
			String clusterVPCSecurityGroupName = describeClustersResponse.getClusters().get(index)
												.getVpcSecurityGroups().get(index).getVpcSecurityGroupId();
			String clusterStatus = describeClustersResponse.getClusters().get(index).getClusterStatus();
			
			StringBuilder URLBuilder = new StringBuilder("jdbc:postgresql://");
			URLBuilder.append(endpointAddress).append(":").append(endpointPort).append("/").append(dbName);
			dbURL = URLBuilder.toString();
			System.out.println("Following are the details of the cluster:");
			System.out.println("ClusterIdentifier: "+clusterIdentifier
								+"\nNode Type: "+nodeType
								+"\nMaster Username: "+MasterUsername
								+"\nDB Name: "+dbName
								+"\nURL: "+dbURL
								+"\nSecurity Group: "+clusterVPCSecurityGroupName
								+"\nCluster Status: "+clusterStatus
						);

	}
	
	public void connectToRedshift() {
		Connection conn = null;
        Statement stmt = null;
        try{
           //Dynamically load postgresql driver at runtime.
           Class.forName("org.postgresql.Driver");

           //Open a connection and define properties.
           System.out.println("Connecting to database...");
           Properties props = new Properties();

           //Uncomment the following line if using a keystore.
           //props.setProperty("ssl", "true");  
           System.out.println("Test: "+MasterUsername);
           System.out.println("Test: "+dbURL);
           props.setProperty("user", MasterUsername);
           
           // NOTE: One disadvantage of Redshift API is that there is no API to describe the MasterPassword of cluster.
           props.setProperty("password", MasterUserPassword);
           conn = DriverManager.getConnection(dbURL, props);
         //Try a simple query.
           System.out.println("Listing system tables...");
           stmt = conn.createStatement();
           String sql;
           sql = "select * from information_schema.tables;";
           ResultSet rs = stmt.executeQuery(sql);
           
           //Get the data from the result set.
           while(rs.next()){
              //Retrieve two columns.
              String catalog = rs.getString("table_catalog");
              String name = rs.getString("table_name");

              //Display values.
              System.out.print("Catalog: " + catalog);
              System.out.println(", Name: " + name);
           }
           rs.close();
           stmt.close();
           conn.close();
        }catch(Exception ex){
           //For convenience, handle all errors here.
           ex.printStackTrace();
        }finally{
            //Finally block to close resources.
            try{
               if(stmt!=null)
                  stmt.close();
            }catch(Exception ex){
            }// nothing we can do
            try{
               if(conn!=null)
                  conn.close();
            }catch(Exception ex){
               ex.printStackTrace();
            }
         }
         System.out.println("Finished connectivity test.");
	}

}

//Commented code - Rough work
//redshift.authenticateRedshift();
//redshift.createRedshift();
//redshift.describeRedshiftClusters();
//Authentication to AWS
//		BasicAWSCredentials creds = new BasicAWSCredentials("", "");
//
//		AmazonRedshiftClient client = new AmazonRedshiftClient(creds);
//		client.setEndpoint("https://redshift.us-east-1.amazonaws.com/");
//
//		CreateClusterRequest request = new CreateClusterRequest();
//		request.withClusterIdentifier("ciusingsdk")
//				.withMasterUsername("cloudigrate")
//				.withMasterUserPassword("CloudIgrate4")
//				.withNodeType("dw2.large")
//				.withClusterType("single-node")
//				.withVpcSecurityGroupIds("sg-f8e65a9d");
//		Cluster createResponse = client.createCluster(request);
//		System.out.println("Created cluster " + createResponse.getClusterIdentifier());
//
//		DescribeClustersRequest describeClusterRequest = new DescribeClustersRequest();
//
//		describeClusterRequest.withClusterIdentifier("ciusingsdk");
//		//DescribeClustersRequest dcr = new DescribeClustersRequest();
//		DescribeClustersResult describeClustersResponse = client.describeClusters(describeClusterRequest);
//		int index=0;
//		//while(!describeClustersResponse.getClusters().isEmpty()){
//			//System.out.println(describeClustersResponse.getClusters().get(index));
//			String clusterIdentifier = describeClustersResponse.getClusters().get(index).getClusterIdentifier();
//			String nodeType = describeClustersResponse.getClusters().get(index).getNodeType();
//			String masterUSername = describeClustersResponse.getClusters().get(index).getMasterUsername();
//			String dbName = describeClustersResponse.getClusters().get(index).getDBName();
//			System.out.println("Following are the details of the cluster:");
//			System.out.println("ClusterIdentifier: "+clusterIdentifier
//								+"\nNode Type: "+nodeType
//								+"\nMaster Username: "+masterUSername
//								+"\nDB Name: "+dbName
//						);


//}

//client.describeClusters(dcr);
//		boolean isWaiting = true;
//		while(isWaiting){
//			Thread.sleep(5000);
//			System.out.println("Created cluster " + createResponse.getAvailabilityZone());
//		}
