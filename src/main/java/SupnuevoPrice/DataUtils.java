package SupnuevoPrice;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class DataUtils {

    final static String dataPath = "/home/schong/Documents/data";
    final static String newDataPath = "/home/schong/Documents/newData.data";
    final static String arffCodigoPath = "/home/schong/Documents/codigoARFF/";

    Connection openDb() {
        Connection conn = null;
        do {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                conn = DriverManager.getConnection(
                        "jdbc:mysql://localhost:3306/supnuevo_statistics?useSSL=true&serverTimezone=UTC&useUnicode=true&characterEncoding=UTF-8",
                        "root",
                        "root");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException sqle) {
                try {
                    sqle.printStackTrace();
                    Thread.sleep(1000);
                    System.out.println("try to connect to statistics Database");
                } catch (InterruptedException e) {
                    System.out.println("Database Error");
                }
            }
        } while (conn == null);
        return conn;
    }


    Connection opensupDb() {
        Connection conn = null;
        do {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                conn = DriverManager.getConnection(
                        "jdbc:mysql://localhost:3306/supnuevo?useSSL=true&serverTimezone=UTC&useUnicode=true&characterEncoding=UTF-8",
                        "root",
                        "root");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException sqle) {
                try {
                    sqle.printStackTrace();
                    Thread.sleep(1000);
                    System.out.println("try to connect to sup Database");
                } catch (InterruptedException e) {
                    System.out.println("Database Error");
                }
            }
        } while (conn == null);
        return conn;
    }

    static void Backupdbfromsql() {
        try {
            String dbName = " supnuevo_db supnuevo_common_commodity";
            String dbUser = "supnuevo_root";
            String dbPass = "mysql.root";

            String folderPath = "./backup";

            File f1 = new File(folderPath);
            f1.mkdir();

            String savePath = folderPath + "/backup.sql";

            String executeCmd = "mysqldump -P " + "3306 -h " + "158.69.137.173 -u " + dbUser + " -p" + dbPass + dbName + " -r " + savePath;

            Process runtimeProcess = Runtime.getRuntime().exec(executeCmd);
            int processComplete = runtimeProcess.waitFor();

            if (processComplete == 0) {
                System.out.println("Backup DataBase Complete");
            } else {
                System.out.println("Backup DataBase Failure");
            }

        } catch (IOException | InterruptedException ex) {
            ex.printStackTrace();
            System.out.println("Error at Backuprestore" + ex.getMessage());
        }
    }

    static void Restoredbtosql() {
        try {
            String dbName = "supnuevo_db";
            String dbUser = "root";
            String dbPass = "root";

            String restorePath = " ./backup/backup.sql";

            String[] executeCmd = new String[]{"mysql", "--user=" + dbUser, "--password=" + dbPass, dbName, "-e", " source " + restorePath};

            Process runtimeProcess = Runtime.getRuntime().exec(executeCmd);
            int processComplete = runtimeProcess.waitFor();

            if (processComplete == 0) {
                System.out.println("Successfully restored ");
            } else {
                System.out.println("Error at restoring");
            }

        } catch (IOException | InterruptedException ex) {
            ex.printStackTrace();
            System.out.println("Error at Restoredbfromsql" + ex.getMessage());
        }

    }

    /*public static void Restoredbtosql() {
        try {
            String dbName = "supnuevo_db";
            String dbUser = "root";
            String dbPass = "root";

            String restorePath = " ./backup/backup.sql";

            String[] executeCmd = new String[]{"mysql", "--user=" + dbUser, "--password=" + dbPass, dbName, "-e", " source " + restorePath};

            Process runtimeProcess = Runtime.getRuntime().exec(executeCmd);
            int processComplete = runtimeProcess.waitFor();

            if (processComplete == 0) {
                System.out.println("Successfully restored ");
            } else {
                System.out.println("Error at restoring");
            }

        } catch (IOException | InterruptedException ex) {
            ex.printStackTrace();
            System.out.println("Error at Restoredbfromsql" + ex.getMessage());
        }
    }*/

    /*public Connection openDb() {
        Connection conn = null;
        do {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                conn = DriverManager.getConnection(
                        "jdbc:mysql://127.0.0.1:3306/supnuevo_statistics?useSSL=true", "root",
                        "root");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException sqle) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.out.println("Database Error");
                }
            }
        } while (conn == null);
        return conn;
    }

    public Connection opensupDb() {
        Connection conn = null;
        do {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                conn = DriverManager.getConnection(
                        "jdbc:mysql://127.0.0.1:3306/supnuevo_db?useSSL=true", "root",
                        "root");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException sqle) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.out.println("Database Error");
                }
            }
        } while (conn == null);
        return conn;
    }*/

    /*
    public Connection openDb() {
        Connection conn = null;
        do {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                conn = DriverManager.getConnection(
                        "jdbc:mysql://personal-mysql.mysql.database.azure.com:3306/supnuevo_statistics?useSSL=true", "SqlAdmin@personal-mysql",
                        "TAuWcGN2eU#h4B#@");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException sqle) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.out.println("Database Error");
                }
            }
        } while (conn == null);
        return conn;
    }

    public Connection opensupDb() {
        Connection conn = null;
        do {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                conn = DriverManager.getConnection(
                        "jdbc:mysql://158.69.137.173:3306/supnuevo_db?useSSL=true", "supnuevo_root",
                        "mysql.root");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException sqle) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.out.println("Database Error");
                }
            }
        } while (conn == null);
        return conn;
    }
    */
}
