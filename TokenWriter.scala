import java.io.File
import java.io.FileWriter
import java.io.PrintStream
import java.io.PrintWriter
import java.io.StringWriter
import java.io.Writer
import java.security.PrivilegedExceptionAction
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.jdbc.HiveConnection
import scala.io.Source
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.fs.Path
import java.io.DataOutputStream
import java.io.ByteArrayOutputStream
import org.apache.hadoop.security.token.Token
import org.apache.hadoop.io.Text


object TokenWriter  {

  object JDBCTest {

    val driverName = "org.apache.hive.jdbc.HiveDriver"
    var connectURL = "jdbc:hive2://udoddlmstr02.unix.rgbk.com:2181,udoddlmstr01.unix.rgbk.com:2181,udoddlmstr03.unix.rgbk.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
   
    var path: String = "/home/spark/spark-events/hadoop/"
   
    var directory = new File(path)
    if (!directory.exists){
      path = "/home/cdsw/drift/hadoop/"
    }
   
    val schemaName = "dm_wm"
    val principal = "svc_pwm_ret"
   
    var krb5conf = path + "krb5.conf"

    val realm = "CORP.RGBK.COM"
   
    var keytab: String = path + "svc_pwm_ret.keytab"
   
   
    val conf = new Configuration()
       


    def main(): Unit = {
     
      System.out.println("connectURL: " + connectURL)
      System.out.println("schemaName: " + schemaName)
      System.out.println("principal: " + principal)
      System.out.println("krb5conf: " + krb5conf)
      System.out.println("realm: " + realm)
      System.out.println("keytab: " + keytab)
     
      try {
        Class.forName("org.apache.hive.jdbc.HiveDriver")
        System.out.println("Found HiveServer2 JDBC driver")
      } catch {
        case e: ClassNotFoundException =>
          System.out.println("Couldn't find HiveServer2 JDBC driver")
      }
     
      try {
        val conf = new Configuration()
       
        System.setProperty("java.security.krb5.conf", krb5conf)
         
        conf.set("hadoop.security.authentication", "kerberos")
        conf.set("hive.metastore.kerberos.principal", "hive/"+principal+"@"+realm)
        conf.set("hive.metastore.sasl.enabled", "true")
     


        UserGroupInformation.setConfiguration(conf)
       
        dtTest()
       
      } catch {
        case e: Throwable =>
          val stackString = new StringWriter
          e.printStackTrace(new PrintWriter(stackString))
          System.out.println(e)
          System.out.println("Error occurred when connecting to HiveServer2 with " + Seq(connectURL, e.getMessage, stackString.toString).mkString(",") )
      }
    }

 
   
   
    @throws[Exception]
    private def dtTest(): Unit = {
      val user = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal + "@" + realm, keytab)
      val credentials = new Credentials()

      user.doAs(new PrivilegedExceptionAction[Unit] {
        @throws[Exception]
        override def run: Unit = {
         
          System.out.println("In doas: " + UserGroupInformation.getLoginUser)
          var con = DriverManager.getConnection(JDBCTest.connectURL)
          System.out.println("Connected to HiveServer2")
          System.out.println("Getting delegation token for user")
         
          val token = con.asInstanceOf[HiveConnection].getDelegationToken(JDBCTest.principal, "hive/_HOST@" + JDBCTest.realm)
         
          System.out.println("Got token: " + token)
          System.out.println("Closing original connection")
         
         
          val path = new Path("file:///home/cdsw/drift/token.dt")

          val delegationToken = new Token()
         
          delegationToken.decodeFromUrlString(token)
          delegationToken.setService(new Text("hms"))
          delegationToken.setKind(new Text("HIVE_DELEGATION_TOKEN"))
         
          credentials.addToken(new Text("hms"), delegationToken)
         
          credentials.writeTokenStorageFile(path, JDBCTest.conf)
         
          println()
         
         
         
          val fileWriter =  new FileWriter("/home/cdsw/drift/hive_token.txt")
          fileWriter.write(token)
          fileWriter.flush()

           
           
     
         
          con.close()
           
           
     

           
        }
      })
    }
   


   
 


  }
 

}

TokenWriter.JDBCTest.main()
