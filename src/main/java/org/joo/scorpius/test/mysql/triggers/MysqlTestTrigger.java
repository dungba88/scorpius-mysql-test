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

import javax.sql.DataSource;

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

    public void getUserSync(TriggerExecutionContext executionContext) {
        DataSource dataSource = executionContext.getApplicationContext().getInstance(DataSource.class);
        Connection conn;
        try {
            conn = dataSource.getConnection();
        } catch (SQLException ex) {
            executionContext.fail(new TriggerExecutionException(ex));
            return;
        }
        try {
            List<User> users = new ArrayList<>();
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("select * from users where id < 10")) {
                    while (rs.next()) {
                        int id = rs.getInt("id");
                        String name = rs.getString("name");
                        users.add(new User(id, name));
                    }
                }
                // stmt.executeUpdate("insert into users (name) values('hello')");
            } catch (SQLException e) {
                executionContext.fail(new TriggerExecutionException(e));
            }
            executionContext.finish(new MysqlTestResponse(users.toArray(new User[0])));
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                // do nothing
            }
        }
    }
}
