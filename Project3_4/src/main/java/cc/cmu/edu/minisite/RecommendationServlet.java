/*
 * Task 5 (bonus), Implement a simple user recommendation system
 *
 * AndrewID: haow2
 * Hao Wang
 *
 */

package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;

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

public class RecommendationServlet extends HttpServlet {

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
	
	public RecommendationServlet () throws Exception {
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
	}

	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
			throws ServletException, IOException {

		JSONObject result = new JSONObject();
	        String id = request.getParameter("id");

		/**
		 * Bonus task:
		 * 
		 * Recommend at most 10 people to the given user with simple collaborative filtering.
		 * 
		 * Store your results in the result object in the following JSON format:
		 * recommendation: [
		 * 		{name:<name_1>, profile:<profile_1>}
		 * 		{name:<name_2>, profile:<profile_2>}
		 * 		{name:<name_3>, profile:<profile_3>}
		 * 		...
		 * 		{name:<name_10>, profile:<profile_10>}
		 * ]
		 * 
		 * Notice: make sure the input has no duplicate!
		 */
		JSONArray recommendations = new JSONArray();

		// Get result from HBase
		Map<String, Integer> map = new HashMap<String, Integer>();
        Get get_res = new Get(Bytes.toBytes(id));
        Result res = followeeTable.get(get_res);
        KeyValue datas = res.list().get(0);
        String s = Bytes.toString(datas.getValue());
        String[] followees = s.split("!");

        System.out.println("111111");

        // Mark useless recommendation list
        Map<String, Integer> flag = new HashMap<String, Integer>();
        for (int i = 0; i < followees.length; i++) {
        	if (flag.containsKey(followees[i])) {
        		continue;
        	}
        	flag.put(followees[i], 1);
        }
        flag.put(id, 1);

        System.out.println("22222");

        // Compute the recommendation value
        for (int i = 0; i < followees.length; i++) {
        	Get get_res2 = new Get(Bytes.toBytes(followees[i]));
	        Result res2 = followeeTable.get(get_res2);
	        KeyValue datas2 = res2.list().get(0);
	        String temp = Bytes.toString(datas.getValue());
	        String[] secondLists = temp.split("!");

	        for (int j = 0; j < secondLists.length; j++) {
	        	// Remove useless keys
	        	if (flag.containsKey(secondLists[j])) {
	        		continue;
	        	}
	        	if (!map.containsKey(secondLists[j])) {
	        		map.put(secondLists[j], 1);
	        	} else {
	        		map.put(secondLists[j], map.get(secondLists[j])+1);
	        	}
	        }
        }

        System.out.println("333333");

        // User priority queue to sort the recommendation
        final PriorityQueue<int[]> queue = new PriorityQueue<int[]>(10, new Comparator<int[]>() {
        	public int compare(int[] entry1, int[] entry2) {       		
        		if (entry1[1] != entry2[1]) {
        			return entry1[0] - entry2[0];
        		}
        		// If equal
        		return entry2[1] - entry1[1];
        	}
    	});

    	System.out.println("4444444");

        // Put all elements into priorityqueue
    	for (String key : map.keySet()) {
    		int[] temp = new int[2];
    		temp[0] = Integer.parseInt(key);
    		temp[1] = map.get(key);
    		queue.add(temp);
    	}

    	System.out.println("5555555");

    	// Get the info from MySQL
    	Statement stmt = null;
    	int i = 0;
    	while(i < 10 && queue.isEmpty()) {
    		try {
    			int[] data = queue.poll();
        		stmt = connMySQL.createStatement();
        		String sql = "SELECT * FROM userinfo WHERE user_id=" + data[0] + ";";
        		ResultSet tempRes = stmt.executeQuery(sql);
        		JSONObject info = new JSONObject();
        		if(tempRes.next()) {  
        			info.put("name", tempRes.getString(2));
        			info.put("profile", tempRes.getString(3));
        		} 
        		recommendations.put(info);
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
                i++;
            }
    	}
    	System.out.println("recommendations: " + recommendations);
    	result.put("recommendation", recommendations);

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

