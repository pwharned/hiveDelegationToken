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

# If your Hive server has ssl enabled

If the Hive server is ssl enabled, the client must also use ssl. If the Hive server uses a self-signed cert or a CA not recognized by your JVM
you may see this message:

```
Caused by: javax.net.ssl.SSLHandshakeException: sun.security.validator.ValidatorException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target
```

To fix it do the following:

1. Get the public certificate of the Hive server

```
openssl s_client -showcerts -connect hive-host.domain.com:10000 </dev/null 2>/dev/null|openssl x509 -outform PEM > cert.pem
```

2. Create a keystore using that cert

```
keytool -import -keystore myLocalTrustStore.jks -file cert.pem
```

You will be promoted for a password to create the .jks file.

3. Edit the jdbc connect URL with the following parameters

```
sslTrustStore=/path/to/myLocalTrustStore.jks;trustStorePassword=password
```

( Note that the password will unlock a jks with only a publicly available cert)

An example URI with ssl will appear as follows:

`jdbc:hive2://hive-host.domain.com:10000/;sslTrustStore=/path/to/myLocalTrustStore.jks;trustStorePassword=changeit`


This should resolve your SSL errors
