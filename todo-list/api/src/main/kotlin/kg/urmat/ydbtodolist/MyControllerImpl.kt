package kg.urmat.ydbtodolist

import org.springframework.web.bind.annotation.RestController
import tech.ydb.auth.iam.CloudAuthHelper
import tech.ydb.core.grpc.GrpcTransport
import tech.ydb.table.Session
import tech.ydb.table.SessionRetryContext
import tech.ydb.table.TableClient
import tech.ydb.table.description.TableDescription
import tech.ydb.table.transaction.TxControl
import tech.ydb.table.values.PrimitiveType


@RestController
class MyControllerImpl : MyController {
    private val connectionString: String = "grpc://localhost:2136/?database=/local"
    private val transport: GrpcTransport = GrpcTransport.forConnectionString(connectionString)
        .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
        .build()
    private val tableClient: TableClient = TableClient.newClient(transport).build()
    private val database: String = transport.database
    private val retryCtx: SessionRetryContext = SessionRetryContext.create(tableClient).build()
    override fun getAll(): List<Todo> {
        val query = ("SELECT id, title, text "
            + "FROM todos;")

        // Begin new transaction with SerializableRW mode

        // Begin new transaction with SerializableRW mode
        val txControl: TxControl<*> = TxControl.serializableRw().setCommitTx(true)

        // Executes data query with specified transaction control settings.

        // Executes data query with specified transaction control settings.
        val result = retryCtx.supplyResult { session: Session ->
            session.executeDataQuery(
                query,
                txControl
            )
        }
            .join().value


        val rs = result.getResultSet(0)
        val res = mutableListOf<Todo>()
        while (rs.next()) {
            res.add(
                Todo(
                rs.getColumn("id").uint64,
                rs.getColumn("title").text,
                rs.getColumn("text").text,
                )
            )
        }
        return res
    }

    override fun createTables() {
        val seriesTable = TableDescription.newBuilder()
            .addNonnullColumn("id", PrimitiveType.Uint64)
            .addNullableColumn("title", PrimitiveType.Text)
            .addNullableColumn("text", PrimitiveType.Text)
            .setPrimaryKey("id")
            .build()
        retryCtx.supplyStatus { session: Session ->
            session.createTable(
                "$database/todos",
                seriesTable
            )
        }
            .join().expectSuccess("Can't create table /series")
    }

    override fun createTodo(id: Long, title: String, text: String) {
        val query = ("UPSERT INTO todos (id, title, text) "
            + "VALUES (${id}, \"${title}\", \"${text}\")")
        val txControl: TxControl<*> = TxControl.serializableRw().setCommitTx(true)

        // Executes data query with specified transaction control settings.

        // Executes data query with specified transaction control settings.
        retryCtx.supplyResult { session: Session ->
            session.executeDataQuery(
                query,
                txControl
            )
        }
            .join().value
    }

    data class Todo(
        val id: Long,
        val title: String,
        val text: String,
    )
}
