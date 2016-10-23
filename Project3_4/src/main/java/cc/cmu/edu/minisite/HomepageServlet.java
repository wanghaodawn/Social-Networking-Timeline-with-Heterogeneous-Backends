/*
 * Task 3, Get the posts from MongoDB
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

import org.apache.jasper.tagplugins.jstl.core.If;
import org.bson.*;

import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.amazonaws.services.support.model.Category;

import java.util.*;
import java.text.*;

public class HomepageServlet extends HttpServlet {

    private static MongoCollection<Document> collection;

    public HomepageServlet() {
        /*
            Your initialization code goes here
        */
        MongoClient mongoClient = new MongoClient( "ec2-54-85-147-204.compute-1.amazonaws.com",27017);
        MongoDatabase db = mongoClient.getDatabase("test");
        collection = db.getCollection("posts");
    }

    @Override
    protected void doGet(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {

        String id = request.getParameter("id");
        JSONObject result = new JSONObject();
        final JSONArray posts = new JSONArray();

        /*
            Task 3:
            Implement your logic to return all the posts authored by this user.
            Return this posts as-is, but be cautious with the order.

            You will need to sort the posts by Timestamp in ascending order
	     (from the oldest to the latest one). 
        */

        FindIterable<Document> iterable = collection.find(new Document("uid", Integer.parseInt(id)));
        iterable = iterable.sort(new Document("timestamp",1));
        // Objects.sort(iterable, new Comparator<Document>() {
        //     @Override  
        //     public int compare(Document doc1, Document doc2) {
        //         String time1 = doc1.get("timestamp").toString();
        //         String time2 = doc2.get("timestamp").toString();
        //         String format = "yyyy-MM-dd HH-mm-ss";

        //         TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        //         SimpleDateFormat dateFormat = new SimpleDateFormat(format);

        //         Date date1 = dateFormat.parse(time1);
        //         Date date2 = dateFormat.parse(time2);

        //         return date1.compareTo(date2);
        //     }
        // });

        // Store the results in JSON
        iterable.forEach(new Block<Document>() {  
            @Override
            public void apply(Document doc) {
                posts.put(doc);
            }
        });
        result.put("posts", posts);

        PrintWriter writer = response.getWriter();           
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }
}

