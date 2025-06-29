package eu.bosteels;

import org.junit.jupiter.api.Test;

import java.sql.*;

public class DuckDbTest {

    @Test
    public void readSecrets() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        readSecrets(conn);
    }

    @Test
    public void setVariable() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        setVariable(conn);
        readVariable(conn);
    }

    @Test
    public void readSecretsAndSetVariable() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        readSecrets(conn);
        setVariable(conn);
        readVariable(conn);
    }

    @Test
    public void readItems() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        createTableItems(conn);
        insertItem(conn);
        readItems(conn);
        setVariable(conn);
        readVariable(conn);
       // readSecrets(conn);
        setVariable(conn);
    }


    public void readSecrets(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        try (ResultSet rs = stmt.executeQuery("SELECT * FROM duckdb_secrets()")) {
            while (rs.next()) {
                String name = rs.getString("name");
                String provider = rs.getString("type");
                System.out.printf("name=%s provider=%s%n", name, provider);
            }
        }
        System.out.println("=====");
    }

    public void createTableItems(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE or replace TABLE items (item VARCHAR, value DECIMAL(10, 2), count INTEGER)");
            System.out.println("table created");
        }
    }

    public void insertItem(Connection conn) throws SQLException {
        try (PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO items VALUES (?, ?, ?);")) {
            preparedStatement.setString(1, "chainsaw");
            preparedStatement.setDouble(2, 500.0);
            preparedStatement.setInt(3, 42);
            preparedStatement.execute();
        }
    }

    public void readItems(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT item, value, count FROM items")) {
            while (rs.next()) {
                String item = rs.getString("item");
                String value = rs.getString("value");
                System.out.printf("item=%s value=%s%n", item, value);
            }
        }
    }

    public void setVariable(Connection conn) {
        try (PreparedStatement preparedStatement = conn.prepareStatement("set variable my_var = ?")) {
            preparedStatement.setString(1, "my value");
            preparedStatement.execute();
            System.out.println("setVariable done");
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
    }

    public void readVariable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT getvariable('my_var') as my_var")) {
            while (rs.next()) {
                String my_var = rs.getString("my_var");
                System.out.printf("my_var=%s %n", my_var);
            }
        }
    }

    @Test
    public void setVariable_v1() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        PreparedStatement preparedStatement = conn.prepareStatement("set variable my_var = ?");
        preparedStatement.setString(1, "my value");
        preparedStatement.execute();
        System.out.println("setVariable done");
    }

    @Test
    public void setVariable_v4() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        PreparedStatement preparedStatement = conn.prepareStatement("set variable my_var = ?");
        preparedStatement.setString(1, "my value");
        preparedStatement.execute();
        System.out.println("setVariable done");
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT getvariable('my_var') as my_var")) {
            while (rs.next()) {
                String my_var = rs.getString("my_var");
                System.out.printf("my_var=%s %n", my_var);
            }
        }
    }

    @Test
    public void setVariable_v2() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        System.out.println("connected");
        //conn.createStatement().execute("CREATE or replace PERSISTENT SECRET (TYPE postgres, HOST 'localhost', database 'anything', user 'anyone', password 'youchoose')");
        conn.createStatement().execute("CREATE or replace PERSISTENT SECRET (TYPE postgres)");
        System.out.println("secret created");
        conn.createStatement().executeQuery("SELECT * FROM duckdb_secrets()");
        System.out.println("SELECT * FROM duckdb_secrets() done");
        PreparedStatement preparedStatement = conn.prepareStatement("set variable my_var = ?");
        System.out.println("stmt prepared");
        preparedStatement.setString(1, "my value");
        System.out.println("setString called");
        preparedStatement.execute();
        System.out.println("setVariable done");
    }

    @Test
    public void setVariable_v3() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        conn.createStatement().execute("CREATE or replace PERSISTENT SECRET (TYPE postgres, HOST 'localhost', database 'anything', user 'anyone', password 'youchoose')");
        //conn.createStatement().execute("CREATE or replace PERSISTENT SECRET (TYPE postgres)");
        //conn.createStatement().executeQuery("SELECT * FROM duckdb_secrets()");
        PreparedStatement preparedStatement = conn.prepareStatement("set variable my_var = ?");
        preparedStatement.setString(1, "my value");
        preparedStatement.execute();
    }

}
