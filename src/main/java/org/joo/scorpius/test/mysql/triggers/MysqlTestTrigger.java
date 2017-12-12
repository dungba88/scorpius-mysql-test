package org.joo.scorpius.test.mysql.triggers;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.joo.scorpius.support.exception.TriggerExecutionException;
import org.joo.scorpius.test.mysql.dto.MysqlTestRequest;
import org.joo.scorpius.test.mysql.dto.MysqlTestResponse;
import org.joo.scorpius.trigger.TriggerExecutionContext;
import org.joo.scorpius.trigger.impl.AbstractTrigger;

import io.vertx.core.AsyncResult;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

public class MysqlTestTrigger extends AbstractTrigger<MysqlTestRequest, MysqlTestResponse> {

    private ExecutorService executor = Executors.newFixedThreadPool(7);

    @Override
    public void execute(TriggerExecutionContext executionContext) throws TriggerExecutionException {
        executionContext.finish(null);
        getUserAsync(executionContext);
    }

    public void getUserAsync(TriggerExecutionContext executionContext) {
        CompletableFuture.runAsync(() -> getUserSync(executionContext), executor);
    }

    public void getUserSync(TriggerExecutionContext executionContext) {
        SQLClient client = executionContext.getApplicationContext().getInstance(SQLClient.class);
        client.getConnection(res -> openConnection(res, executionContext));
    }

    private void openConnection(AsyncResult<SQLConnection> res, TriggerExecutionContext executionContext) {
        if (res.failed())
            executionContext.fail(new TriggerExecutionException(res.cause()));
        else {
            SQLConnection connection = res.result();
//            connection.query("select * from users where id < 10", queryRes -> {
//                List<User> users = new ArrayList<>();
//                ResultSet rs = queryRes.result();
//                List<JsonArray> results = rs.getResults();
//                for (JsonArray arr : results) {
//                    int id = arr.getInteger(0);
//                    String name = arr.getString(1);
//                    users.add(new User(id, name));
//                }
//                executionContext.finish(new MysqlTestResponse(users.toArray(new User[0])));
//            });
            connection.execute("update users set name = 'a' where id = 1", res2 -> {
                connection.close();
            });
        }
    }
}
