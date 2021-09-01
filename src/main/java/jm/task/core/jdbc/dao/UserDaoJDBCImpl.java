package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    private final static String CREATE_USERS_TABLE = "CREATE TABLE IF NOT EXISTS `maindb`.`user` (\n" +
            "  `id` BIGINT NOT NULL AUTO_INCREMENT,\n" +
            "  `name` VARCHAR(45) NULL,\n" +
            "  `lastName` VARCHAR(45) NULL,\n" +
            "  `age` TINYTEXT NULL,\n" +
            "  PRIMARY KEY (`id`));";
    private final static String FILL_TABLE_USER = "INSERT INTO `maindb`.`user` VALUES (" + "null,?,?,?)";
    private static final String DROP_USERS_TABLE = "DROP TABLE IF EXISTS `maindb`.`user`;";
    private static final String CLEAN_TABLE_USER = "DELETE FROM `maindb`.`user`;";
    private static final String REMOVE_USER_BY_ID = "DELETE FROM `maindb`.`user` WHERE id=?;";
    private static final String GET_ALL_USERS = "SELECT * FROM `maindb`.`user`;";

    private Util connectionPool = Util.getInstance();

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        try (Connection connection = connectionPool.getConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(CREATE_USERS_TABLE);
        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
        }
    }

    public void dropUsersTable() {
        try (Connection connection = connectionPool.getConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(DROP_USERS_TABLE);
        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(FILL_TABLE_USER);
            statement.setString(1, name);
            statement.setString(2, lastName);
            statement.setString(3, String.valueOf(age));
            statement.executeUpdate();
        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
        }
        System.out.printf("User с именем – %s добавлен в базу данных \n", name);
    }

    public void removeUserById(long id) {
        try (Connection connection = connectionPool.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(REMOVE_USER_BY_ID);
            statement.setString(1, String.valueOf(id));
            statement.executeUpdate();
        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
        }
    }

    public List<User> getAllUsers() {
        List<User> userList = new ArrayList<>();
        try (Connection connection = connectionPool.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(GET_ALL_USERS);
            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong(1));
                user.setName(resultSet.getString(2));
                user.setLastName(resultSet.getString(3));
                user.setAge((byte) resultSet.getInt(4));
                userList.add(user);
            }
        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
        }
        return userList;
    }

    public void cleanUsersTable() {
        try (Connection connection = connectionPool.getConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(CLEAN_TABLE_USER);
        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
        }
    }
}
