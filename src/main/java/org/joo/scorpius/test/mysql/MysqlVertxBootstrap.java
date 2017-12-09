package org.joo.scorpius.test.mysql;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
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
import io.vertx.ext.web.Router;

public class MysqlVertxBootstrap extends CompositionBootstrap {

    protected void configureBootstraps(List<Bootstrap> bootstraps) {
        bootstraps.add(new TypeSafeBootstrap());
        bootstraps.add(AbstractBootstrap.from(this::configureMysql));
        bootstraps.add(AbstractBootstrap.from(this::configureTriggers));
        bootstraps.add(new VertxBootstrap(new VertxOptions(), new HttpServerOptions().setUseAlpn(true), 1912) {

            @Override
            protected Router configureRoutes(final Vertx vertx) {
                Router restAPI = super.configureRoutes(vertx);
                restAPI.get("/").handler(rc -> rc.response().end());
                return restAPI;
            }
        });
    }

    private void configureMysql() {
        try {
            Config applicationConfig = applicationContext.getInstance(Config.class);

            BasicDataSource dataSource = new BasicDataSource();
            dataSource.setUrl(applicationConfig.getString("jdbc.connectionString"));
            dataSource.setUsername(applicationConfig.getString("jdbc.user"));
            dataSource.setPassword(applicationConfig.getString("jdbc.passwd"));
            dataSource.setMaxTotal(200);

            applicationContext.override(DataSource.class, dataSource);
            prepareDb(dataSource);
        } catch (Exception ex) {
            throw new BootstrapInitializationException(ex);
        }
    }

    private void prepareDb(BasicDataSource dataSource) throws Exception {
        Connection connection = dataSource.getConnection();
        String query = null;
        try (InputStream is = new FileInputStream("src/main/resources/table.sql")) {
            query = IOUtils.toString(is, "utf-8");
        }
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(query);
        }
    }

    private void configureTriggers() {
//        triggerManager.setHandlingStrategy(new DisruptorHandlingStrategy(65536, new BusySpinWaitStrategy()));
        triggerManager.registerTrigger("greet_java").withAction(MysqlTestTrigger::new);
    }
}