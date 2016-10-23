/*
 * Task 4, Put Task 1-3 together to implement a homepage of social network website
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

import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.text.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class TimelineServlet extends HttpServlet {

    // MySQL
    private static Connection connMySQL;

    // HBase
    private static String zkAddr = "172.31.12.124";
    private static String tableName = "linkdata";
    private static HTableInterface linksTable;
    private static HTableInterface followeeTable;
    private static HConnection connHBase;
    private static byte[] bColFamily = Bytes.toBytes("data");
    private final static Logger logger = Logger.getRootLogger();

    // MongoDB
    private static MongoCollection<Document> collection;

    public TimelineServlet() throws Exception {
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
        followeeTable = connHBase.getTable(Bytes.toBytes("followeedata"));

        // MongoDB
        MongoClient mongoClient = new MongoClient( "ec2-54-85-147-204.compute-1.amazonaws.com",27017);
        MongoDatabase db = mongoClient.getDatabase("test");
        collection = db.getCollection("posts");
    }

    @Override
    protected void doGet(final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        JSONObject result = new JSONObject();
        String id = request.getParameter("id");

        /*
            Task 4 (1):
            Get the name and profile of the user as you did in Task 1
            Put them as fields in the result JSON object
        */
        Statement stmt = null;
        try {
            stmt = connMySQL.createStatement();
            String sql = "SELECT * FROM userinfo WHERE user_id=" + id+";";
            ResultSet res = stmt.executeQuery(sql);

            if (res.next()) {
                result.put("name", res.getString(2));
                result.put("profile", res.getString(3));
                System.out.println("name: " + res.getString(2));
                System.out.println("name: " + res.getString(3));
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

        /*
            Task 4 (2);
            Get the follower name and profiles as you did in Task 2
            Put them in the result JSON object as one array
        */
        JSONArray followers = new JSONArray();

        // Get result from HBase
        Get get_res = new Get(Bytes.toBytes(id));
        Result res = linksTable.get(get_res);
        KeyValue datas = res.list().get(0);
        String s = Bytes.toString(datas.getValue());
        String[] followerss = s.split("!");

        System.out.println("The number of followers is: " + followerss.length + "The followers are:\n" + s);

        // Get data from MySQL
        stmt = null;
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

        Collections.sort(list, new Comparator<String[]>() {
            @Override  
            public int compare(String[] data1, String[] data2) {  
                return data1[0].compareTo(data2[0]);  
            } 
        });

        for (int i = 0; i < list.size(); i++) {
            JSONObject follower = new JSONObject();
            String[] temp = list.get(i);
            follower.put("name", temp[0]);
            follower.put("profile", temp[1]);                   
            followers.put(follower);
        }
        result.put("followers", followers);
        System.out.println("followers:\n" + followers);
        /*
            Task 4 (3):
            Get the 30 LATEST followee posts and put them in the
            result JSON object as one array.

            The posts should be sorted:
            First in ascending timestamp order
            Then numerically in ascending order by their PID (PostID) 
	    if there is a tie on timestamp
        */

        JSONArray posts = new JSONArray();

        // Get result from HBase
        get_res = new Get(Bytes.toBytes(id));
        res = followeeTable.get(get_res);
        datas = res.list().get(0);
        s = Bytes.toString(datas.getValue());
        String[] followees = s.split("!");

        System.out.println("The length of followees is: " + followees.length);
        System.out.println("followees:\n" + s);
        // System.out.println("11111111");

        // Get the latest 30 posts
        final PriorityQueue<Document> queue = new PriorityQueue<Document>(200, new Comparator<Document>() {
            public int compare(Document doc1, Document doc2) {

                try {
                    String time1 = doc1.get("timestamp").toString();
                    String time2 = doc2.get("timestamp").toString();
                    String format = "yyyy-MM-dd HH:mm:ss";
                    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
                    SimpleDateFormat dateFormat = new SimpleDateFormat(format);
                    
                    Date date1 = dateFormat.parse(time1);
                    Date date2 = dateFormat.parse(time2);

                    if (date2.compareTo(date1) != 0) {
                        return date2.compareTo(date1);
                    }

                    // If equals
                    Integer id1 = Integer.parseInt(doc1.get("pid").toString());
                    Integer id2 = Integer.parseInt(doc2.get("pid").toString());
                    return id1.compareTo(id2);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });

        System.out.println("22222222");

        // Add followees to the result
        for (int i = 0; i < followees.length; i++) {
            FindIterable<Document> iterable = collection.find(new Document("uid", Integer.parseInt(followees[i])));
            iterable.forEach(new Block<Document>() {  
                @Override
                public void apply(Document doc) {                  
                    queue.add(doc);
                }
            });
        }

        System.out.println("33333333");
        System.out.println("The size of the queue is: " + queue.size());

        // Reverse the sort of latest 30 posts
        final PriorityQueue<Document> queue2 = new PriorityQueue<Document>(30, new Comparator<Document>() {
            public int compare(Document doc1, Document doc2) {
                try {
                    String time1 = doc1.get("timestamp").toString();
                    String time2 = doc2.get("timestamp").toString();
                    String format = "yyyy-MM-dd HH:mm:ss";
                    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
                    SimpleDateFormat dateFormat = new SimpleDateFormat(format);
                    
                    Date date1 = dateFormat.parse(time1);
                    Date date2 = dateFormat.parse(time2);

                    if (date1.compareTo(date2) != 0) {
                        return date1.compareTo(date2);
                    }

                    // If equals
                    Integer id1 = Integer.parseInt(doc1.get("pid").toString());
                    Integer id2 = Integer.parseInt(doc2.get("pid").toString());
                    return id1.compareTo(id2);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });
        
        // Put data from queue to queue2
        int i = 0;
        while (i < 30 && !queue.isEmpty()) {
            queue2.add(queue.poll());
            i++;
        }

        // Add the queue to the result
        i = 0;
        while (i < 30 && !queue2.isEmpty()) {
            posts.put(queue2.poll());
            i++;
        }
        result.put("posts", posts);

        System.out.println("posts:\n" + posts);
        System.out.println("result:\n" + result);
        
        // Last
        PrintWriter out = response.getWriter();
        out.print(String.format("returnRes(%s)", result.toString()));
        out.close();
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }
    
}

