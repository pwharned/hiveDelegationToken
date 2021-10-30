# hiveDelegationToken
how to get a hive delegation token in scala


Sometimes you need to connect to kerberized Hive from a remote Spark Cluster. Generating a Delegation Token to do this can be challenging, and there is little documentation - it also varies depending on the Hadoop service you are trying to access. 

You will need:

keytab file -> comes from running kinit
principal -> the user
core-site.xml -> hadoop config
hive-site.xml -> hive config
krb5.conf -> kerberos configuration file


With these you can authenticate to the Hive server and generate a delegation token. 
