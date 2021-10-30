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

object TokenWriter  {

  object JDBCTest {

    val driverName = "org.apache.hive.jdbc.HiveDriver"
    var connectURL = "jdbc:hive2://<host>10000/dm_wm"
    val path: String = "/home/spark/spark-events/hadoop/"
   
    connectURL="jdbc:hive2://<host>0.:2181,<host>2.:2181,<host>1.:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"

    val schemaName = "schemaname"
    val principal = "principal"
    var krb5conf = path + "krb5.conf"
    val realm = "CORP.RGBK.COM"
    var keytab: String = path + "principal.keytab"

    //keytab = "/home/cdsw/drift/hadoop-files/svc_pwm_ret.keytab"
    //krb5conf = "/home/cdsw/drift/hadoop-files/krb5.conf"
     
    var hadoop_conf: String = path + "core-site.xml"
    var hive_conf: String =path + "hive-site.xml"

    def main(): Unit = {
      System.out.println("connectURL: " + connectURL)
      System.out.println("schemaName: " + schemaName)
      System.out.println("verticaUser: " + principal)
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
        conf.addResource(hadoop_conf)
        conf.addResource(hive_conf)
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
      val user = UserGroupInformation.loginUserFromKeytabAndReturnUGI(verticaUser + "@" + realm, keytab)
      user.doAs(new PrivilegedExceptionAction[Unit] {
        @throws[Exception]
        override def run: Unit = {
          System.out.println("In doas: " + UserGroupInformation.getLoginUser)
          var con = DriverManager.getConnection(JDBCTest.connectURL)
          System.out.println("Connected to HiveServer2")
          JDBCTest.showUser(con)
          System.out.println("Getting delegation token for user")
          val token = con.asInstanceOf[HiveConnection].getDelegationToken(JDBCTest.principal, "hive/_HOST@" + JDBCTest.realm)
          System.out.println("Got token: " + token)
          System.out.println("Closing original connection")
          con.close()
          /*
         
          System.out.println("Setting delegation token in UGI")
          Utils.setTokenStr(Utils.getUGI, token, "hiveserver2ClientToken")
          con = DriverManager.getConnection(JDBCTest.connectURL + ";auth=delegationToken")
          System.out.println("Connected to HiveServer2 with delegation token")
          JDBCTest.showUser(con)
          con.close()
          */
          JDBCTest.writeDelegationToken(token)
        }
      })
    }

    @throws[Exception]
    private def showUser(con: Connection): Unit = {
      val sql = "select current_user()"
      val stmt = con.createStatement
      val res = stmt.executeQuery(sql)
      val result = new StringBuilder
      while ( {
        res.next
      }) result.append(res.getString(1))
      System.out.println("\tcurrent_user: " + result.toString)
    }

    private def writeDelegationToken(token: String): Unit = {
      try {
        val fileWriter = new FileWriter("hive_token.txt")
        fileWriter.write(token)
        fileWriter.flush()
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }
 

}

TokenWriter.JDBCTest.main()
