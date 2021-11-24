import java.io.File

import java.security.PrivilegedExceptionAction

import java.sql.Connection

import java.sql.DriverManager

import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.security.UserGroupInformation

import org.apache.hive.jdbc.HiveConnection

import org.apache.hadoop.security.Credentials

import org.apache.hadoop.fs.Path

import org.apache.hadoop.security.token.Token

import org.apache.hadoop.io.Text

import org.apache.hadoop.fs.FileSystem

/*

import org.apache.hadoop.hive.shims.Utils

import scala.io.Source

*/

 

/*

Example usage:

val args = Array("jdbc:hive2://host.unix.domain.com:2181,host.unix.domain.com:2181,host.unix.domain.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2", "principal", "/home/user/hadoop/krb5.conf", "CORP.DOMAIN.COM", "/home/user/hadoop/principal.keytab")

TokenWriter.main(args)

 

*/

 

object TokenWriter  {

 

 

    def main(args: Array[String]): Unit = {

     

 

      

      val connectURL = args(0)

      val principal = args(1)

      val krb5conf = args(2)

      val realm = args(3)

      val keytab = args(4)

     

      args.foreach(println)

     

      System.setProperty("java.security.krb5.conf", krb5conf)

     

      Class.forName("org.apache.hive.jdbc.HiveDriver")

   

 

      val conf = new Configuration()

 

      conf.set("hadoop.security.authentication", "kerberos")

      conf.set("hive.metastore.kerberos.principal", "hive/"+principal+"@"+realm)

      conf.set("hive.metastore.sasl.enabled", "true")

      conf.addResource("/home/cdsw/drift/hadoop/hdfs-site.xml")

     

 

      UserGroupInformation.setConfiguration(conf)

       

      write(connectURL, principal, realm, keytab, conf)

       

      

    }

 

 

    private def write(connectURL: String, principal:String,  realm: String, keytab:String, conf:Configuration): Unit = {

     

      val user = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal + "@" + realm, keytab)

      val credentials = new Credentials()

 

      user.doAs(new PrivilegedExceptionAction[Unit] {

 

        override def run: Unit = {

         

          var con = DriverManager.getConnection(connectURL)

            

          val token = con.asInstanceOf[HiveConnection].getDelegationToken(principal, "hive/_HOST@" + realm)

        

         

          val path = new Path("file:///home/cdsw/drift/token.dt")

 

          val delegationToken = new Token()

         

          delegationToken.decodeFromUrlString(token)

         

          delegationToken.setService(new Text("hms"))

         

          delegationToken.setKind(new Text("HIVE_DELEGATION_TOKEN"))

         

          credentials.addToken(new Text("hms"), delegationToken)

         

          

          val fs =  FileSystem.get(conf)

         

          val tokens = fs.addDelegationTokens("hdfs", credentials)

         

          

          credentials.writeTokenStorageFile(path, conf) 

          

          println("Token written to " + path.toString)

         

          con.close()

      

        }

      })

    }

   

 

}
