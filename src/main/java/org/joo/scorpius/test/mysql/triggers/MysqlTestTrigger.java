package org.joo.scorpius.test.mysql.triggers;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.pool.ObjectPool;
import org.joo.scorpius.support.exception.TriggerExecutionException;
import org.joo.scorpius.test.mysql.dto.MysqlTestRequest;
import org.joo.scorpius.test.mysql.dto.MysqlTestResponse;
import org.joo.scorpius.test.mysql.dto.User;
import org.joo.scorpius.trigger.TriggerExecutionContext;
import org.joo.scorpius.trigger.impl.AbstractTrigger;

public class MysqlTestTrigger extends AbstractTrigger<MysqlTestRequest, MysqlTestResponse> {

    private ExecutorService executor = Executors.newFixedThreadPool(7);

    @Override
    public void execute(TriggerExecutionContext executionContext) throws TriggerExecutionException {
        getUserAsync(executionContext);
    }

    public void getUserAsync(TriggerExecutionContext executionContext) {
        CompletableFuture.runAsync(() -> getUserSync(executionContext), executor);
    }

    @SuppressWarnings("unchecked")
    public void getUserSync(TriggerExecutionContext executionContext) {
        ObjectPool<Connection> dbPool = executionContext.getApplicationContext().getInstance(ObjectPool.class);
        Connection conn;
        try {
            conn = dbPool.borrowObject();
        } catch (Exception e) {
            executionContext.fail(new TriggerExecutionException(e));
            return;
        }
        try {
            List<User> users = new ArrayList<>();
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("select * from users");
                while (rs.next()) {
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    users.add(new User(id, name));
                }
            } catch (SQLException e) {
                executionContext.fail(new TriggerExecutionException(e));
            }
            executionContext.finish(new MysqlTestResponse(users.toArray(new User[0])));
        } finally {
            try {
                dbPool.returnObject(conn);
            } catch (Exception e) {
                executionContext.fail(new TriggerExecutionException(e));
            }
        }
    }
}
