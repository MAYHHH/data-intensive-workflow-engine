/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import static workflowengine.WorkflowEngine.*;

/**
 *
 * @author udomo
 */
public class DBRecord
{

    private HashMap<String, String> record = new HashMap<>();
    private String table;
    private static Connection con = null;

    public DBRecord()
    {
    }

    public DBRecord(String table, Object... vals)
    {
        if (vals.length % 2 == 1)
        {
            throw new IllegalArgumentException("The number of vals must be even.");
        }
        this.table = table;
        for (int i = 0; i < vals.length; i += 2)
        {
            record.put(vals[i].toString(), vals[i + 1].toString());
        }
    }

    public static void prepareConnection()
    {
        if (con == null)
        {
            try
            {
                String url = "jdbc:mysql://" + PROP.getProperty("DBHost") + ":" + PROP.getProperty("DBPort") + "/" + PROP.getProperty("DBName");
//                System.out.println(url);
                Class.forName("com.mysql.jdbc.Driver");
                con = DriverManager.getConnection(
                        url,
                        PROP.getProperty("DBUser"),
                        PROP.getProperty("DBPass"));
            }
            catch (ClassNotFoundException | SQLException ex)
            {
                throw new DBException(ex, "");
            }
        }
    }

    public void set(String key, String val)
    {
        record.put(key, val);
    }

    public void set(String key, double val)
    {
        set(key, val + "");
    }

    public String get(String key)
    {
        return record.get(key);
    }
    public double getDouble(String key)
    {
        return Double.parseDouble(record.get(key));
    }
    public int getInt(String key)
    {
        return Integer.parseInt(record.get(key));
    }

    public void unset(String key)
    {
        record.remove(key);
    }

    public void setTable(String table)
    {
        this.table = table;
    }

    public int getFieldCount()
    {
        return record.size();
    }

    public int insert()
    {
            StringBuilder query = new StringBuilder();
        try
        {
            prepareConnection();
            query.append("INSERT INTO ");
            query.append(table);
            query.append(" ( ");
            for (String key : record.keySet())
            {
                query.append(" ").append(key).append(", ");
            }
            query.delete(query.length() - 2, query.length());
            query.append(") VALUES ( ");
            for (String key : record.keySet())
            {
                query.append(" '").append(record.get(key)).append("', ");
            }
            query.delete(query.length() - 2, query.length());
            query.append(" ) ");
//            System.out.println(query);
            Statement smt = con.createStatement();
            smt.executeUpdate(query.toString(), Statement.RETURN_GENERATED_KEYS);
            ResultSet rs = smt.getGeneratedKeys();
            rs.next();
            return rs.getInt(1);
        }
        catch (SQLException ex)
        {
            throw new DBException(ex, query.toString());
        }
    }

    public int delete()
    {
            StringBuilder query = new StringBuilder();
        try
        {
            prepareConnection();
            query.append("DELETE FROM ").append(table).append(" WHERE ");
            for (String key : record.keySet())
            {
                query.append(" ").append(key).append("='").append(record.get(key)).append("', ");
            }
            query.delete(query.length() - 2, query.length());
            return con.createStatement().executeUpdate(query.toString());
        }
        catch (SQLException ex)
        {
            throw new DBException(ex, query.toString());
        }
    }

    public int update(DBRecord where)
    {
        StringBuilder query  = new StringBuilder();
        try
        {
            prepareConnection();
            query.append("UPDATE ").append(table).append(" SET ");
            for (String key : record.keySet())
            {
                query.append(" ").append(key).append("='").append(record.get(key)).append("', ");
            }
            query.delete(query.length() - 2, query.length());

            if (where.getFieldCount() > 0)
            {
                query.append(" WHERE ");
                for (String key : where.record.keySet())
                {
                    query.append(" ").append(key).append("='").append(where.get(key)).append("', ");
                }
                query.delete(query.length() - 2, query.length());
            }
//            System.out.println(query);
            return con.createStatement().executeUpdate(query.toString());
        }
        catch (SQLException ex)
        {
            throw new DBException(ex, query.toString());
        }
    }

    public int update(String[] whereKeys)
    {
        DBRecord where = new DBRecord();
        for (String s : whereKeys)
        {
            where.set(s, this.get(s));
        }
        return update(where);
    }

    public int update()
    {
        return update(new String[0]);
    }

    public static int update(String sql)
    {
        try
        {
            prepareConnection();
            return con.createStatement().executeUpdate(sql);
        }
        catch (SQLException ex)
        {
            throw new DBException(ex, sql);
        }
    }

    public static List<DBRecord> selectAll(String table)
    {
        return select(table, new DBRecord());
    }
    public static List<DBRecord> select(String sql)
    {
        return select("", sql);
    }

    public static List<DBRecord> select(String table, DBRecord where)
    {
        prepareConnection();
        StringBuilder query = new StringBuilder();
        query.append("SELECT * FROM ").append(table);
        query.append(" WHERE ");
        for (String key : where.record.keySet())
        {
            query.append(" ").append(key).append("='").append(where.get(key)).append("', ");
        }
        query.delete(query.length() - 2, query.length());
        return select(table, query.toString());
    }

    public static List<DBRecord> select(String table, String sql)
    {
        prepareConnection();
        try
        {
            ResultSet rs = con.createStatement().executeQuery(sql);
            ResultSetMetaData md = rs.getMetaData();
            ArrayList<DBRecord> results = new ArrayList<>();
            while (rs.next())
            {
                DBRecord r = new DBRecord();
                for (int i = 1; i <= md.getColumnCount(); i++)
                {
                    r.set(md.getColumnLabel(i), rs.getString(i));
                }
                r.setTable(table);
                results.add(r);
            }
            return results;
        }
        catch (SQLException ex)
        {
            throw new DBException(ex, sql);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (String s : record.keySet())
        {
            sb.append(s).append(":").append(record.get(s)).append(", ");
        }
        sb.delete(sb.length() - 2, sb.length());
        return sb.toString();
    }

    public static void main(String[] args)
    {
        PROP.setProperty("DBHost", "localhost");
        PROP.setProperty("DBPort", "3306");
        PROP.setProperty("DBName", "workflow_engine");
        PROP.setProperty("DBUser", "root");
        PROP.setProperty("DBPass", "1234");

        /*Collection<DBRecord> results = select("worker");
         for(DBRecord r:results)
         {
         System.out.println(r.toString());
         r.unset("id");
         r.set("cpu", 1);
         r.update();
         }*/
        new DBRecord("worker", "cpu", "515").update(new DBRecord("worker", "id", "3"));
    }
}
