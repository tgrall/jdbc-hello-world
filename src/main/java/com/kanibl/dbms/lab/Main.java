package com.kanibl.dbms.lab;


public class Main {

    public static void main(String[] args) throws Exception {

        DatabaseService databaseService = new DatabaseService("jdbc:derby:/tmp/my-db\n;create=true");

        databaseService.createSchema();

        System.out.println("Print all users");
        System.out.println(
                databaseService.getUserAllAsMap()
        );
        System.out.println("========\n");

        System.out.println("Print one user");
        System.out.println(
                databaseService.getUserById(1)
        );
        System.out.println("========\n");

        databaseService.closeConnection();


    }

}
