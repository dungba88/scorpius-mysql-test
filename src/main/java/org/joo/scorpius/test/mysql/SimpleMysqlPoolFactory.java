package org.joo.scorpius.test.mysql;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.commons.pool.BasePoolableObjectFactory;

public class SimpleMysqlPoolFactory extends BasePoolableObjectFactory<Connection> {

    private String connection;

    private String user;

    private String password;

    public SimpleMysqlPoolFactory(String connection, String user, String password) {
        this.connection = connection;
        this.user = user;
        this.password = password;
    }

    @Override
    public Connection makeObject() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
        return DriverManager.getConnection(connection, user, password);
    }
}
