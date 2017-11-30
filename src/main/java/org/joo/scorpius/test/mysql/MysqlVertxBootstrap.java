package org.joo.scorpius.test.mysql;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.util.NoSuchElementException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPoolFactory;
import org.joo.scorpius.support.vertx.VertxBootstrap;
import org.joo.scorpius.trigger.handle.disruptor.DisruptorHandlingStrategy;

import com.lmax.disruptor.YieldingWaitStrategy;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.vertx.core.VertxOptions;

public class MysqlVertxBootstrap extends VertxBootstrap {

    private Config config;

    public void run() {
        configureConfiguration();

        try {
            configureMysql();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        configureTriggers();

        VertxOptions options = new VertxOptions().setEventLoopPoolSize(8);
        configureServer(options, 8080);
    }

    private void configureConfiguration() {
        config = ConfigFactory.load();
        applicationContext.override(Config.class, config);
    }

    private void configureMysql() throws NoSuchElementException, IllegalStateException, Exception {
        SimpleMysqlPoolFactory dbPoolFactory = new SimpleMysqlPoolFactory(config.getString("jdbc.connectionString"),
                config.getString("jdbc.user"), config.getString("jdbc.passwd"));

        GenericObjectPool.Config config = new GenericObjectPool.Config();
        config.maxActive = 10;
        config.testOnBorrow = true;
        config.testWhileIdle = true;
        config.timeBetweenEvictionRunsMillis = 10000;
        config.minEvictableIdleTimeMillis = 60000;

        GenericObjectPoolFactory<Connection> genericObjectPoolFactory = new GenericObjectPoolFactory<>(dbPoolFactory,
                config);
        ObjectPool<Connection> dbPool = genericObjectPoolFactory.createPool();

        applicationContext.override(ObjectPool.class, dbPool);
        prepareDb(dbPool);
    }

    private void prepareDb(ObjectPool<Connection> dbPool)
            throws NoSuchElementException, IllegalStateException, Exception {
        Connection connection = dbPool.borrowObject();
        try {
            String query = null;
            try (InputStream is = new FileInputStream("src/main/resources/table.sql")) {
                query = IOUtils.toString(is, "utf-8");
            }
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate(query);
            }
        } finally {
            dbPool.returnObject(connection);
        }
    }

    private void configureTriggers() {
        triggerManager.setHandlingStrategy(new DisruptorHandlingStrategy(1024, new YieldingWaitStrategy()));
        triggerManager.registerTrigger("greet_java").withAction(MysqlTestTrigger::new);
    }
}