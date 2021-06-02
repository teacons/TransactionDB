import org.intellij.lang.annotations.Language
import java.sql.Connection
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.*
import kotlin.random.Random


enum class Type {
    SELECT, INSERT, UPDATE
}

private const val url = "jdbc:postgresql://10.0.0.100:5433/db_catalog"
private val props = Properties().apply {
    setProperty("user", "db_catalog")
    setProperty("password", "B8RCcsgy0")
    setProperty("ssl", "false")
}

val connection: Connection = DriverManager.getConnection(url, props)

fun main(args: Array<String>) {
    val num = args[0].toInt()
    
    heating()

    println("Time start: ${SimpleDateFormat("dd/M/yyyy hh:mm:ss").format(Date())}\"")

    println(
        "%20s %20s %20s %20s %20s %20s %20s %20s"
            .format("ISOLATION LEVEL", "SELECT", "", "INSERT", "", "UPDATE", "", "ALL")
    )
    println("%20s %20s %20s %20s %20s %20s %20s %20s".format("", "All", "Avg", "All", "Avg", "All", "Avg", ""))

    readCommitted(num)
    repeatableRead(num)
    serializable(num)

    println("Time finish: ${SimpleDateFormat("dd/M/yyyy hh:mm:ss").format(Date())}")

    removeTemp()


}

fun heating() {
    for (i in 1..100) {
        connection.createStatement().execute("SELECT * FROM db.people")
    }
}

fun readCommitted(num: Int) {
    queries(num, Connection.TRANSACTION_READ_COMMITTED)
}

fun repeatableRead(num: Int) {
    queries(num, Connection.TRANSACTION_REPEATABLE_READ)
}

fun serializable(num: Int) {
    queries(num, Connection.TRANSACTION_SERIALIZABLE)
}

fun queries(num: Int, connectionType: Int) {

    val start = System.currentTimeMillis()


    val type = when (connectionType) {
        Connection.TRANSACTION_READ_COMMITTED -> "READ COMMITTED"
        Connection.TRANSACTION_REPEATABLE_READ -> "REPEATABLE READ"
        Connection.TRANSACTION_SERIALIZABLE -> "SERIALIZABLE"
        else -> throw IllegalArgumentException("Wrong connectionType")
    }

    @Language("PostgreSQL")
//    val selectThread = Query(connectionType, "SELECT * FROM db.people", Type.SELECT, num)
    val selectThread = Query(connectionType, "START TRANSACTION ISOLATION LEVEL $type; SELECT * FROM db.people; COMMIT;", Type.SELECT, num)

    @Language("PostgreSQL")
    val insertThread =
//        Query(connectionType, "INSERT INTO db.people (fullname, year_of_birth) VALUES (?, ?)", Type.INSERT, num)
        Query(connectionType, "START TRANSACTION ISOLATION LEVEL $type; INSERT INTO db.people (fullname, year_of_birth) VALUES (?, ?); COMMIT;", Type.INSERT, num)

    @Language("PostgreSQL")
    val updateThread =
//        Query(connectionType, "UPDATE db.people SET year_of_birth = 1000 WHERE fullname = ?", Type.UPDATE, num)
        Query(connectionType, "START TRANSACTION ISOLATION LEVEL $type; UPDATE db.people SET year_of_birth = 1000 WHERE fullname = ?; COMMIT;", Type.UPDATE, num)

    selectThread.start()
    insertThread.start()
    updateThread.start()
    selectThread.join()
    insertThread.join()
    updateThread.join()

    val end = System.currentTimeMillis()

    val timeAll = end - start

    val selAll = selectThread.times.sum()
    val selAvg = selectThread.times.average()
    val insAll = insertThread.times.sum()
    val insAvg = insertThread.times.average()
    val updAll = updateThread.times.sum()
    val updAvg = updateThread.times.average()


    println("%20s %20s %20s %20s %20s %20s %20s %20s".format(type, selAll, selAvg, insAll, insAvg, updAll, updAvg, timeAll))

}

private fun removeTemp() {
//    val connection: Connection = DriverManager.getConnection(url, props)

    connection.createStatement().executeUpdate("DELETE FROM db.people WHERE fullname LIKE ('test%')")

    connection.close()
}

class Query(private val connectionType: Int, private val query: String, val type: Type, private val num: Int) :
    Thread() {
    val times = mutableListOf<Long>()

    override fun run() {
//        val connection: Connection = DriverManager.getConnection(url, props).apply {
//            autoCommit = false
//            transactionIsolation = connectionType
//        }


        for (i in 1..num) {
            val startTime = System.currentTimeMillis()

            if (type == Type.SELECT)
                connection.createStatement().execute(query)
            else
                connection.prepareStatement(query).apply {
                    setString(1, "test$i")
                    if (type == Type.INSERT)
                        setInt(2, Random.nextInt(1500, 2021))
                }.execute()

            val endTime = System.currentTimeMillis()
            val time = endTime - startTime
            times.add(time)
        }
//        connection.commit()
//        connection.close()
    }
}

