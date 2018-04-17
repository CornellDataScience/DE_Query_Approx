import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class Query1
{
    public static void main(String[] args)
    {
        //TODO Fill in server username, password, and the tpch database name
        
        String server_username = ""; //The name of your server username
        String server_password = ""; //The password for the server above
        String database_name = ""; //The name of your TPCH database
        
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception ex) {
            // handle the error
            System.out.println("Error!");
        }
        
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        
        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + database_name + "?user=" + server_username + "&password=" + server_password);
            String selectst = "SELECT l_returnflag, l_linestatus, " +
            "sum(l_quantity) as sum_qty, " +
            "sum(l_extendedprice) as sum_base_price, " +
            "sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, " +
            "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, " +
            "avg(l_quantity) as avg_qty, " +
            "avg(l_extendedprice) as avg_price, " +
            "avg(l_discount) as avg_disc, " +
            "count(*) as count_order " +
            "FROM lineitem " +
            "WHERE l_shipdate <= date \'1998-12-01\' - interval \'90\' day " +
            "GROUP BY l_returnflag, l_linestatus " +
            "ORDER BY l_returnflag, l_linestatus";
            stmt = conn.createStatement();
            rs = stmt.executeQuery(selectst);
            ResultSetMetaData rsmd = rs.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++)
            {
                String tabs = "\t";
                if (i == 3 || (i >= 5 && i <= 7)) // Format tab spacing for nice table output
                    tabs += "\t";
                System.out.print(rsmd.getColumnLabel(i) + tabs);
            }
            System.out.println("");
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++)
                {
                    String columnValue = rs.getString(i);
                    String tabs = "\t";
                    if (i <= 2 || (i == 5 && rs.getRow() != 3)) // Format tab spacing for nice table output
                        tabs += "\t";
                    System.out.print(columnValue + tabs);
                }
                System.out.println();
            }
        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) { } // ignore
                
                rs = null;
            }
            
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) { } // ignore
                
                stmt = null;
            }
        }
    }
}
