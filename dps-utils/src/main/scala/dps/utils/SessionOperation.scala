package dps.utils
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import scala.collection.immutable.List
import scala.collection.mutable.Map

class SessionOperation {
  private var conn: Connection = _
  def this(driver: String, ip: String,port:String, user: String, password: String,dbType:String,dbName:String) {
    this()
    Class.forName(driver)
    val url = dbType.toLowerCase() match{
      case "mysql" => s"jdbc:mysql://${ip}:${port}/${dbName}?useUnicode=true&characterEncoding=UTF-8&useSSL=false&autoReconnect=true&failOverReadOnly=false"
      case "postgres" => s"jdbc:postgresql://${ip}:${port}/${dbName}"
    }
    this.conn = DriverManager.getConnection(url, user, password)
  }
  def executeUpdate(sql:String, params:Array[Any]){
    println("execute update")
    println(s"\tSQL: ${sql}\n\tparams:${params.mkString(",")}")
    val prepareStatement = this.conn.prepareStatement(sql)
    for (i <- 0 until params.length) {
      prepareStatement.setObject(i + 1, params.apply(i))
    }
    prepareStatement.executeUpdate()
  }
  def executeQuery(sql: String, params: Array[Any]): List[Map[String, Any]] = {
    println("execute query")
    println(s"\tSQL: ${sql}\n\tparams:${params.mkString(",")}")
    val prepareStatement = this.conn.prepareStatement(sql)
    for (i <- 0 until params.length) {
      prepareStatement.setObject(i + 1, params.apply(i))
    }
    val resultSet = prepareStatement.executeQuery()
    val resultSetMetaData = resultSet.getMetaData()
    var columnNames = List[String]()

    print("Query resultset fields:")
    for (i <- 1 to resultSetMetaData.getColumnCount()) {
      columnNames = resultSetMetaData.getColumnName(i) :: columnNames
    }
    columnNames = columnNames.reverse
    println(columnNames)
    var datas = List[Map[String, Any]]()
    while (resultSet.next()) {
      val data = Map[String, Any]()
      columnNames.reverse.foreach(columnName => {
        val columnValue = resultSet.getObject(columnName)
        data.put(columnName, columnValue)
      })
      //列表第一个元素前插入元素
      datas = data :: datas
    }
    resultSet.close()
    prepareStatement.close()
    return datas.reverse
  }
  def close() {
    if (conn != null) {
      conn.close()
    }
  }

}