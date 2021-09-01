package jm.task.core.jdbc;

import jm.task.core.jdbc.service.UserService;
import jm.task.core.jdbc.service.UserServiceImpl;

public class Main {
    private final UserService userService = new UserServiceImpl();

    public static void main(String[] args) {
        Main main = new Main();
        main.makeActionDb();
    }

    private void makeActionDb() {
        userService.createUsersTable();
        userService.saveUser("Ivan", "Afanasiev", (byte) 25);
        userService.saveUser("Vasili", "Ivanov", (byte) 45);
        userService.saveUser("Olga", "Petrova", (byte) 16);
        userService.saveUser("Marina", "Borisova", (byte) 34);
        System.out.println(userService.getAllUsers());
        userService.cleanUsersTable();
        userService.dropUsersTable();
    }
}
