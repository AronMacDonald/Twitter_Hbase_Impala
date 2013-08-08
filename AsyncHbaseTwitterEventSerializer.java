package flume_hdfs.hbase.util;
//original package name
//package com.hbase.log.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.SimpleHbaseEventSerializer.KeyType;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;

//testing
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;

//needed
import java.io.*;
import java.net.*;
import java.sql.Timestamp;
import java.net.URI;
// end testing

// json 
import com.google.gson.Gson;
import java.util.List;

import twitter4j.Status;
import twitter4j.json.DataObjectFactory;
import twitter4j.TwitterException;
//

import com.google.common.base.Charsets;
/**
 * A serializer for the AsyncHBaseSink, which splits the event body into
 * multiple columns and inserts them into a row whose key is available in
 * the headers
 *
 * Originally from https://blogs.apache.org/flume/entry/streaming_data_into_apache_hbase
 * 1st Version from Dan Sandler https://github.com/DataDanSandler/log_analysis/blob/master/flume-sinks-src/com/hbase/log/util/AsyncHbaseLogEventSerializer.java
 * Modified from reading Logs to read Twitter JSON by Aron MacDonald
 */
public class AsyncHbaseTwitterEventSerializer implements AsyncHbaseEventSerializer {
  private byte[] table;
  private byte[] colFam;
  private Event currentEvent;
  private byte[][] columnNames;
  private final List<PutRequest> puts = new ArrayList<PutRequest>();
  private final List<AtomicIncrementRequest> incs = new ArrayList<AtomicIncrementRequest>();
  private byte[] currentRowKey;
  private final byte[] eventCountCol = "eventCount".getBytes();
  private String delim;
  
  // test
  private String g_cols;
  // test end

  //HANA 
  hanaConnection hCon = new hanaConnection();
  hanaTweet hTweet    = new hanaTweet();
  
  
  @Override
  public void initialize(byte[] table, byte[] cf) {
    this.table = table;
    this.colFam = cf;
  }

  @Override
  public void setEvent(Event event) {
    // Set the event and verify that the rowKey is not present
    this.currentEvent = event;
    String rowKeyStr = currentEvent.getHeaders().get("rowKey");
    //if (rowKeyStr == null) {
    //  throw new FlumeException("No row key found in headers!");
    //}
    //currentRowKey = rowKeyStr.getBytes();
  }

  @Override
  public List<PutRequest> getActions() {
    // Split the event body and get the values for the columns
    String eventStr = new String(currentEvent.getBody());
    //String[] cols = eventStr.split(",");
    //String[] cols = eventStr.split(regEx);
    //String[] cols = eventStr.split("\\s+");
    //String[] cols = eventStr.split("\\t");
    String[] cols = eventStr.split(delim);
    puts.clear();
    String[] columnFamilyName;
    byte[] bCol;
    byte[] bFam;
    byte[] bVal;

// test   write file needs be in try
    try {
      File file = new File("/home/ubuntu/hbasetest.log");
      String content;
 
      // if file doesnt exists, then create it
      if (!file.exists()) {
           file.createNewFile();
      } 
 
      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);

      bw.write("g_cols:");
      bw.newLine();

      content = g_cols;
      bw.write(content);

      bw.newLine();

      bw.write("evendStr:");
      bw.newLine();

      content = eventStr;
      bw.write(content);

      bw.newLine();

      //key dofKey = '1';
      //DataObjectFactory dof = new DataObjectFactory(); 
      try { 
        //Object obj = DataObjectFactory.createObject(eventStr);
        Status status = DataObjectFactory.createStatus(eventStr);
        List<Status> tweets;

         bw.write("Status details");
         bw.newLine();
         bw.write("status.getId()");
         bw.newLine();
         bw.write(status.getText());
         bw.newLine();

      }
      catch ( TwitterException  e){
        bw.write("eventStr not in JSON");
        bw.newLine();

      }       
   
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

// end test

    // Insert Relevant Parts of Tweet
    try {
    	
      // Status documentation http://twitter4j.org/javadoc/twitter4j/Status.html 	
      Status tweet = DataObjectFactory.createStatus(eventStr);
      Status retweet = tweet.getRetweetedStatus();

      currentRowKey = String.valueOf(tweet.getId()).getBytes();
      // Hbase Column Family Tweet
      bFam = "tweet".getBytes();

      bCol = "id_str".getBytes();
      bVal = String.valueOf(tweet.getId()).getBytes();
      PutRequest req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
      puts.add(req);

      bCol = "text".getBytes();
      bVal = tweet.getText().getBytes(); 
      req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
      puts.add(req);

      bCol = "created_at".getBytes();
      bVal = String.valueOf(new Timestamp(tweet.getCreatedAt().getTime())).getBytes();
      req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
      puts.add(req);

      /* // current twitter4j missing "lang"  tweet.getIsoLanguageCode()
      bCol = "lang".getBytes();
      bVal = tweet.getIsoLanguageCode().getBytes(); //Twitters best effort
      req = new PutRequest(table, currentRowKey, bFam,
              bCol, bVal);
      puts.add(req);
      */

      if (tweet.getGeoLocation() != null) { 
        bCol = "geo_latitude".getBytes();
        bVal = String.valueOf(tweet.getGeoLocation().getLatitude()).getBytes();
        req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
        puts.add(req);

        bCol = "geo_longitude".getBytes();
        bVal = String.valueOf(tweet.getGeoLocation().getLongitude()).getBytes();
        req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
        puts.add(req);

      }

      bCol = "json_str".getBytes();
      bVal = eventStr.getBytes();
      req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
      puts.add(req);


      // Hbase Column Family User - Info about user at time tweet created
      bFam = "user".getBytes();
     
      bCol = "screen_name".getBytes();
      bVal = tweet.getUser().getScreenName().getBytes();
      req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
      puts.add(req);

      bCol = "location".getBytes();
      bVal = String.valueOf(tweet.getUser().getLocation()).getBytes();
      req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
      puts.add(req);
      
      bCol = "followers_count".getBytes();
      bVal = String.valueOf(tweet.getUser().getFollowersCount()).getBytes();
      req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
      puts.add(req);

      bCol = "profile_image_url".getBytes();
      bVal = String.valueOf(tweet.getUser().getProfileImageURL()).getBytes();
      req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
      puts.add(req);

      if (retweet != null) {
          bFam = "retweet".getBytes();
    	  
          bCol = "rt_id_str".getBytes();
          bVal = String.valueOf(retweet.getId()).getBytes();
          req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
          puts.add(req);
          
          bCol = "rt_screen_name".getBytes();
          bVal = retweet.getUser().getScreenName().getBytes();
          req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
          puts.add(req);
          
          bCol = "rt_profile_image_url".getBytes();
          bVal = String.valueOf(retweet.getUser().getProfileImageURL()).getBytes();
          req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
          puts.add(req);

          bCol = "rt_followers_count".getBytes();
          bVal = String.valueOf(retweet.getUser().getFollowersCount()).getBytes();
          req = new PutRequest(table, currentRowKey, bFam, bCol, bVal);
          puts.add(req);         
          
      }
      
      
      
      // Send to HANA
      //hTweet.setId(tweet.getId());   // Long didn't work
      hTweet.setId(String.valueOf(tweet.getId()));
      hTweet.setCreatedAt(String.valueOf(new Timestamp(tweet.getCreatedAt().getTime())));  
      if (tweet.getGeoLocation() != null) {
    	  hTweet.setGeo(String.valueOf(tweet.getGeoLocation().getLatitude()),
    			        String.valueOf(tweet.getGeoLocation().getLongitude())); 
      } else {
    	  hTweet.setGeo(null,null);
      }
      if (retweet != null) {
    	  hTweet.setReTweetInfo(String.valueOf(retweet.getId()),
    			        retweet.getUser().getScreenName(),
    			        String.valueOf(retweet.getUser().getFollowersCount())); 
      } else {
    	  hTweet.setReTweetInfo(null,null,null);
      }
      hTweet.setUsername(tweet.getUser().getScreenName());  
      hTweet.setContent(tweet.getText()); 
      hTweet.setFollowers(String.valueOf(tweet.getUser().getFollowersCount())); 
      hTweet.setLocation(tweet.getUser().getLocation());  
      
      try {
        hCon.sendHana(hTweet.getJsonStr());

      } catch (Exception e) {
          e.printStackTrace();
      }
    }
    catch ( TwitterException  e){
 
    }
    


    for (int i = 0; i < cols.length; i++) {
      //Generate a PutRequest for each column.
      columnFamilyName = new String(columnNames[i]).split(":");
      bFam = columnFamilyName[0].getBytes();
      bCol = columnFamilyName[1].getBytes();

      if (i == 0) {
         currentRowKey = cols[i].getBytes();
      }
      //PutRequest req = new PutRequest(table, currentRowKey, colFam,
              //columnNames[i], cols[i].getBytes());
      PutRequest req = new PutRequest(table, currentRowKey, bFam,
              bCol, cols[i].getBytes());
      
      //puts.add(req);
    }
    return puts;
  }

  @Override
  public List<AtomicIncrementRequest> getIncrements() {
    incs.clear();
    //Increment the number of events received
    incs.add(new AtomicIncrementRequest(table, "totalEvents".getBytes(), colFam, eventCountCol));
    return incs;
  }

  @Override
  public void cleanUp() {
    table = null;
    colFam = null;
    currentEvent = null;
    columnNames = null;
    currentRowKey = null;
  }

  @Override
  public void configure(Context context) {
    //Get the column names from the configuration
    String cols = new String(context.getString("columns"));
    String[] names = cols.split(",");
    columnNames = new byte[names.length][];
    int i = 0;
    for(String name : names) {
      columnNames[i++] = name.getBytes();
    }
    delim = new String(context.getString("delimiter"));
    // test
    g_cols = cols;
    // end test
    
    // Get Hana Settings  from /etc/flume-ng/conf/flume.conf
    
    hCon.setConnection(   new String(context.getString("hanaServer")),
    		              Integer.parseInt(context.getString("xsPort")),
    		              new String(context.getString("xsJs")),
    		              new String(context.getString("hUser")),
    		              new String(context.getString("hPass"))
    		);
    
  }

  @Override
  public void configure(ComponentConfiguration conf) {
  }
}

// The following is only required if also planning to send Tweet to a HANA Database
class hanaConnection {
    // Connection Settings
    private String hanaServer;  //e.g. ec2-XX-XXX-XXX-XXX.compute-1.amazonaws.com
    private int    xsPort;      //e.g. 8000
    private String xsJs;        //e.g. /test1/Test1/insert.xsjs
    private String hUser;       //e.g. AWS default    SYSTEM
    private String hPass;       //e.g. AWS default    manager

    public void setConnection(String hanaServer, int xsPort, String xsJs, String hUser, String hPass ) {
        this.hanaServer = hanaServer;
        this.xsPort= xsPort;
        this.xsJs = xsJs;
        this.hUser = hUser;
        this.hPass = hPass;
    };
    
    
    public void sendHana(String jsonStr) throws Exception{     

        try {
            File file = new File("/home/ubuntu/hanatest.log");
            String content;
       
            // if file doesnt exists, then create it
            if (!file.exists()) {
                	file.createNewFile();
            } 
       
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);  
            
            bw.write(jsonStr);
            bw.newLine();
            
////////// 	
    	
    	
    	
    	String jsParam    = "jsonStr=" + jsonStr;
    	URI uri = new URI("http", null, 
    			           this.hanaServer, 
    			           this.xsPort, 
    			           this.xsJs,
    			           jsParam, 
    			           null);

    	
    	bw.write(uri.toString());
        bw.newLine();
        bw.write("got this far1");
        bw.newLine(); 
 
        URL url = uri.toURL();
        
        URLConnection uc = url.openConnection();
        
        String userpass = this.hUser + ":" + this.hPass;   //e.g. SYSTEM:manager  -  Default for AWS
        String basicAuth = "Basic " + javax.xml.bind.DatatypeConverter.printBase64Binary(userpass.getBytes());

        uc.setRequestProperty ("Authorization", basicAuth);
        InputStream in = uc.getInputStream();
    	 
//////////	   	
        
        bw.write("got this far22");
        bw.newLine();
              
        
        bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }         	
  
  }
    
}  
	
class hanaTweet {

      
	  // Tweet Fields
	  private String id;  // tried 'long', but JSON parser in HANA XS seems to round up last 3 digits
	  private String createdAt;
	  private String latitude = "0";
	  private String longitude = "0";
	  private String username;
      private String content;
      private String location;
      private String followers;
      private String reTweet = "";
      private String rtId = "0";  // ReTweet Id
      private String rtUsername = ""; // Re Tweet Username
      private String rtFollowers = "0"; // Re Tweet Username
      
      public void setId(String id) {
            this.id = id; 
      };
      
      public void setCreatedAt(String createdAt) {
          this.createdAt = createdAt;
      }

      public void setReTweetInfo(String rtId, String rtUsername, String rtFollowers ) {
    	  if (rtId != null) {
    		  this.reTweet    = "X";
    		  this.rtId       = rtId;
              this.rtUsername = rtUsername;
              this.rtFollowers = rtFollowers;
    	  } else {
    		  this.reTweet    = "";
    		  this.rtId       = "0";
    		  this.rtUsername = "";
    		  this.rtFollowers = "0";
    	  }
          
      }      

      public void setGeo(String latitude, String longitude ) {
    	  if (latitude != null) {
              this.latitude  = latitude;
              this.longitude = longitude;
    	  } else {
              this.latitude    = "0";
		      this.longitude   = "0";
    	  }
      }         
      
      public void setUsername(String username) {
            this.username = username;
      }
      
      public void setContent(String content) {
          this.content = content;
     }

      public void setLocation(String location) {
          if (location != null) {
        	  this.location = location;
          } else {
        	  this.location = "";
          }
    	  
      }

      public void setFollowers(String followers) {
          if (followers != null) {
        	  this.followers = followers;
          } else {
        	  this.location = "0";
          }
    	  
      }      
      
      public String getJsonStr(){
            Gson gson = new Gson();
            return gson.toJson(this);
      }

      
      hanaTweet() {
        // no-args constructor
      }
    }


