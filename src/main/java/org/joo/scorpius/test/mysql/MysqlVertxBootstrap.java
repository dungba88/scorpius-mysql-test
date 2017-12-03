package org.joo.scorpius.test.mysql;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.pool.impl.GenericObjectPoolFactory;
import org.joo.scorpius.Bootstrap;
import org.joo.scorpius.support.bootstrap.AbstractBootstrap;
import org.joo.scorpius.support.bootstrap.CompositionBootstrap;
import org.joo.scorpius.support.exception.BootstrapInitializationException;
import org.joo.scorpius.support.typesafe.TypeSafeBootstrap;
import org.joo.scorpius.support.vertx.VertxBootstrap;
import org.joo.scorpius.test.mysql.triggers.MysqlTestTrigger;
import org.joo.scorpius.trigger.handle.disruptor.DisruptorHandlingStrategy;

import com.lmax.disruptor.YieldingWaitStrategy;
import com.typesafe.config.Config;

import io.vertx.core.VertxOptions;

public class MysqlVertxBootstrap extends CompositionBootstrap {

    protected void configureBootstraps(List<Bootstrap> bootstraps) {
        bootstraps.add(new TypeSafeBootstrap());
        bootstraps.add(AbstractBootstrap.from(this::configureMysql));
        bootstraps.add(AbstractBootstrap.from(this::configureTriggers));
        bootstraps.add(new VertxBootstrap(new VertxOptions().setEventLoopPoolSize(8), 8080));
    }

    private void configureMysql() {
        try {
            Config applicationConfig = applicationContext.getInstance(Config.class);
            SimpleMysqlPoolFactory dbPoolFactory = new SimpleMysqlPoolFactory(
                    applicationConfig.getString("jdbc.connectionString"), applicationConfig.getString("jdbc.user"),
                    applicationConfig.getString("jdbc.passwd"));

            GenericObjectPool.Config config = new GenericObjectPool.Config();
            config.maxActive = 10;
            config.testOnBorrow = true;
            config.testWhileIdle = true;
            config.timeBetweenEvictionRunsMillis = 10000;
            config.minEvictableIdleTimeMillis = 60000;

            GenericObjectPoolFactory<Connection> genericObjectPoolFactory = new GenericObjectPoolFactory<>(
                    dbPoolFactory, config);
            ObjectPool<Connection> dbPool = genericObjectPoolFactory.createPool();

            applicationContext.override(ObjectPool.class, dbPool);
            prepareDb(dbPool);
        } catch (Exception ex) {
            throw new BootstrapInitializationException(ex);
        }
    }

    private void prepareDb(ObjectPool<Connection> dbPool) throws Exception {
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
        triggerManager.setHandlingStrategy(new DisruptorHandlingStrategy(8192, new YieldingWaitStrategy()));
        triggerManager.registerTrigger("greet_java").withAction(MysqlTestTrigger::new);
    }
}