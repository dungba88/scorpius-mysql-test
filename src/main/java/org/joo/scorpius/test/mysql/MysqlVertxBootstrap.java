package org.joo.scorpius.test.mysql;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.joo.scorpius.Bootstrap;
import org.joo.scorpius.support.bootstrap.AbstractBootstrap;
import org.joo.scorpius.support.bootstrap.CompositionBootstrap;
import org.joo.scorpius.support.exception.BootstrapInitializationException;
import org.joo.scorpius.support.typesafe.TypeSafeBootstrap;
import org.joo.scorpius.support.vertx.VertxBootstrap;
import org.joo.scorpius.test.mysql.triggers.MysqlTestTrigger;

import com.typesafe.config.Config;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;

public class MysqlVertxBootstrap extends CompositionBootstrap {

    @Override
    protected void configureBootstraps(List<Bootstrap<?>> bootstraps) {
        bootstraps.add(new TypeSafeBootstrap());
        bootstraps.add(AbstractBootstrap.from(this::configureTriggers));
        bootstraps.add(new VertxBootstrap(new VertxOptions(), new HttpServerOptions(), 1912) {

            @Override
            protected Router configureRoutes(final Vertx vertx) {
                Router restAPI = super.configureRoutes(vertx);
                restAPI.get("/").handler(rc -> rc.response().end());
                return restAPI;
            }
        });
        bootstraps.add(AbstractBootstrap.from(this::configureMysql));
    }

    private void configureMysql() {
        try {
            Config applicationConfig = applicationContext.getInstance(Config.class);
            Vertx vertx = applicationContext.getInstance(Vertx.class);
            
            JsonObject sqlConfig = new JsonObject();
            sqlConfig.put("url", applicationConfig.getString("jdbc.connectionString"));
            sqlConfig.put("user", applicationConfig.getString("jdbc.user"));
            sqlConfig.put("password", applicationConfig.getString("jdbc.passwd"));
            sqlConfig.put("user", applicationConfig.getString("jdbc.user"));
            sqlConfig.put("max_statements", 1);
            sqlConfig.put("max_statements_per_connection", 1);
            sqlConfig.put("initial_pool_size", 15);
            sqlConfig.put("initial_pool_size", 200);
            SQLClient client = JDBCClient.createShared(vertx, sqlConfig);

            applicationContext.override(SQLClient.class, client);
            prepareDb(client);
        } catch (Exception ex) {
            throw new BootstrapInitializationException(ex);
        }
    }

    private void prepareDb(SQLClient client) throws Exception {
        String query;
        try (InputStream is = new FileInputStream("src/main/resources/table.sql")) {
            query = IOUtils.toString(is, "utf-8");
        }
        client.getConnection(res -> {
            if (res.succeeded()) {
                SQLConnection connection = res.result();
                connection.update(query, null);
            }
        });
    }

    private void configureTriggers() {
//        triggerManager.setHandlingStrategy(new DisruptorHandlingStrategy(65536, new BusySpinWaitStrategy()));
        triggerManager.registerTrigger("greet_java").withAction(MysqlTestTrigger::new);
    }
    
    @Override
    public void shutdown() {
        SQLClient client = applicationContext.getInstance(SQLClient.class);
        client.close();
        super.shutdown();
    }
}