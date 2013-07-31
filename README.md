Twitter_Hbase_Impala
====================

Real-time Twitter Firehose using Hadoop HBASE &amp; Impala



WIP:  Code to be uploaded and detailled steps to be included 


Taking inspiration from the below sources of inspiration I've developed a Flume Custom Event Serilizer for Twitter, to write Tweets directly
to Hadoop Hbase improving how these tweets are stored in HDFS rather than as small HDFS files per tweet.

The added benefit is that the Hbase Tweets table can be connected to Hive, removing the need for Ozzie, 
and enabling Realtime reporting via Impala.


Primary Sources of Inspiration:
--------------------------------

Cloudera provided an exellent example of how to use Flume & Ozzie to stream live twitter data to Hadoop HDFS & Hive.
https://github.com/cloudera/cdh-twitter-example

In Clouders'a example:
Flume writes each tweet as a seperate files on HDFS (e.g. 25K files size), in Directories defined by Year/Month/Day/Hour.
Ozzie is then used to schedule a job which adds the new directoies as new Partitions on the HIVE tweet table. 
Only once the directory information has been added to the Hive table partition  can the data be reported on via HIVE sql. 
Depending on scheduling this could leave up to an hour before the new tweets could be reported in Hive.



Data Dan Sandler (DDS) enhanced it further to update the Ozzie process to enable the Hive table partitions to be create very 
shortly after Flume creates a new directory (e.g. for tweets in the current Hour).  DDS enhanced it further to included streaming 
Facebook information.
https://github.com/DataDanSandler/sentiment_analysis

In another Github repository DDS has also demonstrated how to develop a Flume Custom Event Serilizer to write to Hbase, for Apache logs.
https://github.com/DataDanSandler/log_analysis



Steps to Create:
----------------
1)  Follow either Cloudera's or DDS steps for setting up the Twitter data stream to Hadoop.

https://github.com/cloudera/cdh-twitter-example
and/or
http://www.datadansandler.com/2013/03/making-clouderas-twitter-stream-real.html

2)  Download my Custom Event Serilizer
... tbc

3)  Create Hbase table for Tweets
... tbc

4)  Configure Flume to use Hbase Sink  and Custom Event Serilizer
... tbc

5)  Start Flume and Check flow
... tbc

6)  Create Hive table reading Hbase table
... tbc

7)  Run Queries via Impala
... tbc


Small Warning:  The code that I have loaded on GitHub workw in my environment but is still in true ‘protype state’ and is not production ready,  has no error handling and has not been stress tested with high volumes of data.




