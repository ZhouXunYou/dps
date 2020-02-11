package data.process.util
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import scala.collection.immutable.List
import scala.collection.mutable.Map

class SessionOperation {
  private var conn: Connection = _
  def this(driver: String, url: String, user: String, password: String) {
    this()
    Class.forName(driver)
    this.conn = DriverManager.getConnection(url, user, password)
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