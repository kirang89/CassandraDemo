package com.examples.cassandra;

import java.util.List;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

public class CassandraExample {

	//The string serializer translates the byte[] to and from String using utf-8 encoding
    private static StringSerializer stringSerializer = StringSerializer.get();
	//Create a cluster object from your existing Cassandra cluster
    static Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");
    //Create a keyspace object from the existing keyspace we created using CLI
    static Keyspace keyspace = HFactory.createKeyspace("dummy", cluster);
    //Create a mutator object for this keyspace using utf-8 encoding
    static Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
	
    public static void insertData(String cf, String uname, String password, String email) 
    {
    	try
    	{
            //Use the mutator object to insert a column and value pair to an existing key
            mutator.insert(cf, "users", HFactory.createStringColumn("username", uname));
            mutator.insert(cf, "users", HFactory.createStringColumn("password", password));
            mutator.insert(cf, "users", HFactory.createStringColumn("email", email));
          //  System.out.println("Data Inserted");
    	}
    	catch (Exception ex) 
    	{
    		System.out.println("Error encountered while inserting data!!");
    		ex.printStackTrace() ;
    	}
    }
    
    public static void retrieveCustomData(String cf) 
    {
    	try
    	{
            Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");
            Keyspace keyspace = HFactory.createKeyspace("dummy", cluster);
    		SliceQuery<String, String, String> sliceQuery = HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
            sliceQuery.setColumnFamily("users").setKey(cf);
            sliceQuery.setRange("", "", false, 3);
            
            QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute(); 
            System.out.println("\nInserted data \n" + result.get());
            System.out.println(result.getQuery());
            System.out.println(result.getExecutionTimeMicro());
            System.out.println(result.toString());
    	}
    	catch (Exception ex) 
    	{
    		System.out.println("Error encountered while retrieving data!!");
    		ex.printStackTrace() ;
    	}
    }
    
    public static void retrieveData(String cf) 
    {
    	try
    	{
    		SliceQuery<String, String, String> sliceQuery = HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
            sliceQuery.setColumnFamily("users").setKey(cf);
            sliceQuery.setRange("", "", false, 4);
            
            QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute();
            ColumnSlice<String, String> columnList = result.get();
            List<HColumn<String, String>> column = columnList.getColumns();
            //System.out.println("--NAME--" + "--Value--");
            for(int i=0;i<column.size();i++)
            {
            	System.out.println(column.get(i).getName() + ": " + column.get(i).getValue());
            	System.out.println();
            }
    	}
    	catch (Exception ex) 
    	{
    		System.out.println("Error encountered while retrieving data!!");
    		ex.printStackTrace() ;
    	}
    }
    
    public static void updateData() {
    	try {

            
            //Use the mutator object to update a column and value pair to an existing key
            mutator.insert("sample", "users", HFactory.createStringColumn("username", "administrator"));
            
            //Check if data is updated
            MultigetSliceQuery<String, String, String> multigetSliceQuery = HFactory.createMultigetSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
            multigetSliceQuery.setColumnFamily("users");
            multigetSliceQuery.setKeys("sample");
        
            //The 3rd parameter returns the columns in reverse order if true
            //The 4th parameter in setRange determines the maximum number of columns returned per key
            multigetSliceQuery.setRange("username", "", false, 1);
            QueryResult<Rows<String, String, String>> result = multigetSliceQuery.execute();
            System.out.println("Updated data..." +result.get());
    		
    	} catch (Exception ex) {
    		System.out.println("Error encountered while updating data!!");
    		ex.printStackTrace() ;
    	}
    }

    public static void deleteData() {
    	try {
    
    	       //Use the mutator object to delete row
    	       mutator.delete("sample", "users",null, stringSerializer);
    	       
    	       System.out.println("Data Deleted!!");
    	       
    	       //try to retrieve data after deleting
    	       SliceQuery<String, String, String> sliceQuery = HFactory.createSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
    	       sliceQuery.setColumnFamily("users").setKey("sample");
    	       sliceQuery.setRange("", "", false, 4);
    	       
    	       QueryResult<ColumnSlice<String, String>> result = sliceQuery.execute(); 
    	       System.out.println("\nTrying to Retrieve data after deleting the key 'sample':\n" + result.get());
    	       
    	       //close connection
    	       cluster.getConnectionManager().shutdown();
    
		} catch (Exception ex) {
			System.out.println("Error encountered while deleting data!!");
			ex.printStackTrace() ;
		}
	}
    
    
	public static void main(String[] args) 
	{
		for(int i = 0 ; i < 10000; i++)
		{
			String cf = "user"+i;
			String name = "kiran"+i;
			String password = "kiran123";
			String email = name + "@gmail.com";
			insertData(cf, name, password, email) ;
		}
		System.out.println("DONE");

		for(int i=0; i < 10000 ; i++)
			retrieveData("user"+1);
		
		
		/*updateData() ;
		System.out.println("----------------Retrieval after update------------------");
		retrieveData() ;*/
		//deleteData() ;
        
	}
	
	
}
