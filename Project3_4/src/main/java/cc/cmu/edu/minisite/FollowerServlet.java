/*
 * Task 2, Get the followers of a given user
 *
 * AndrewID: haow2
 * Hao Wang
 *
 */

package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.json.JSONArray;

import java.util.*;
import java.sql.*;

import org.json.JSONObject;
import org.json.JSONArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class FollowerServlet extends HttpServlet {
    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = "172.31.12.124";
    /**
     * The name of your HBase table.
     */
    private static String tableName = "linkdata";
    /**
     * HTable handler.
     */
    private static HTableInterface linksTable;
    /**
     * HBase connection.
     */
    private static HConnection connHBase;
    /**
     * MySQL connection.
     */
    private static Connection connMySQL;
    /**
     * Byte representation of column family.
     */
    private static byte[] bColFamily = Bytes.toBytes("data");
    /**
     * Logger.
     */
    private final static Logger logger = Logger.getRootLogger();


    public FollowerServlet() throws Exception {
        /*
            Your initialization code goes here
        */
        // MySQL
        // Define parameters
        final String driver = "com.mysql.jdbc.Driver";
        final String url = "jdbc:mysql://whdawndb2.c7u3dedbqnqt.us-east-1.rds.amazonaws.com:3306/whdawndb2";
        final String user = "root";
        final String password = "15619p34";

        Class.forName(driver);
        connMySQL = DriverManager.getConnection(url, user, password);

        // HBase
        // Remember to set correct log level to avoid unnecessary output.
        logger.setLevel(Level.ERROR);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":60000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("HBase not configured!");
            return;
        }
        connHBase = HConnectionManager.createConnection(conf);
        linksTable = connHBase.getTable(Bytes.toBytes(tableName));
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {

        String id = request.getParameter("id");
        JSONObject result = new JSONObject();
        JSONArray followers = new JSONArray();
        /*
            Task 2:
            Implement your logic to retrive the followers of this user. 
            You need to send back the Name and Profile Image URL of his/her Followers.

            You should sort the followers alphabetically in ascending order by Name. 
            If there is a tie in the followers name, 
	    sort alphabetically by their Profile Image URL in ascending order. 
        */

        // Get result from HBase
        Get get_res = new Get(Bytes.toBytes(id));
        Result res = linksTable.get(get_res);
        KeyValue datas = res.list().get(0);
        String s = Bytes.toString(datas.getValue());
        String[] followerss = s.split("!");

        System.out.println("The followers are: " + s);

        // Get data from MySQL
        Statement stmt = null;
        ArrayList<String[]> list = new ArrayList<String[]>();
        for (int i = 0; i < followerss.length; i++) {
            try {
                stmt = connMySQL.createStatement();
                String sql = "SELECT * FROM userinfo WHERE user_id=" + followerss[i] +";";
                ResultSet res1 = stmt.executeQuery(sql);

                if (res1.next()) {
                    String[] data = new String[2];
                    data[0] = res1.getString(2);
                    data[1] = res1.getString(3);
                    list.add(data);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        // Sort by name in ascending order
        Collections.sort(list, new Comparator<String[]>() {
            @Override  
            public int compare(String[] data1, String[] data2) {  
                return data1[0].compareTo(data2[0]);  
            } 
        });

        // Put the result in JSON
        for (int i = 0; i < list.size(); i++) {
            JSONObject follower = new JSONObject();
            String[] temp = list.get(i);
            follower.put("name", temp[0]);
            follower.put("profile", temp[1]);                   
            followers.put(follower);
        }
        result.put("followers", followers);

        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }   
    
}


