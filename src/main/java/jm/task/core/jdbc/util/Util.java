package jm.task.core.jdbc.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class Util {

    private static final String URL = "jdbc:mysql://localhost:3306/maindb";
    private static final String USER = "root";
    private static final String PASSWORD = "root";
    private static final Logger LOGGER = Logger.getLogger("jm/task/core/jdbc/util/Util.java");
    private final static int DEFAULT_POOL_SIZE = 4;
    private static Util instance = new Util();
    private static boolean isCreated;
    private static Lock locker = new ReentrantLock(true);
    private BlockingDeque<Connection> freeConnections;
    private Queue<Connection> givenAwayConnections;


    private Util() {
        freeConnections = new LinkedBlockingDeque<>(DEFAULT_POOL_SIZE);
        givenAwayConnections = new ArrayDeque<>(DEFAULT_POOL_SIZE);
        for (int i = 0; i < DEFAULT_POOL_SIZE; i++) {
            Connection connection = getConnectionToDB();
            ProxyConnection proxyConnection = new ProxyConnection(connection);
            freeConnections.add(proxyConnection);

        }
    }

    public Connection getConnection() {
        locker.lock();
        Connection connection = null;
        try {
            connection = freeConnections.take();
            givenAwayConnections.add(connection);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } finally {
            locker.unlock();
        }
        return connection;
    }

    public static Util getInstance() {
        if (!isCreated) {
            locker.lock();
            if (instance == null) {
                instance = new Util();
                isCreated = true;
            }
//            LOGGER.info("Instance is created - " + instance);
            locker.unlock();
        }

        return instance;
    }

    public static Connection getConnectionToDB() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        return conn;
    }

    void closeConnection(Connection connection) {
        locker.lock();
        try {
            if (connection instanceof ProxyConnection) {
                givenAwayConnections.remove(connection);
                freeConnections.add(connection);
            } else {
                LOGGER.warning("Connection is not proxy or null!");
            }
        } finally {
            locker.unlock();
        }
    }
}
