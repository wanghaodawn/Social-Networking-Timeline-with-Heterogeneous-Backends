/*
 * Task 1, user authentication
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

import java.sql.*;

import org.json.JSONObject;
import org.json.JSONArray;

public class ProfileServlet extends HttpServlet {

    private static Connection conn;

    public ProfileServlet() {
        /*
            Your initialization code goes here
        */
        // Define parameters
        final String driver = "com.mysql.jdbc.Driver";
        final String url = "jdbc:mysql://whdawndb2.c7u3dedbqnqt.us-east-1.rds.amazonaws.com:3306/whdawndb2";
        final String user = "root";
        final String password = "15619p34";

        // Connect
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        
        JSONObject result = new JSONObject();

        String id = request.getParameter("id");
        String pwd = request.getParameter("pwd");

        System.out.println("id: " + id);
        System.out.println("password: " + pwd);

        /*
            Task 1:
            This query simulates the login process of a user, 
            and tests whether your backend system is functioning properly. 
            Your web application will receive a pair of UserID and Password, 
            and you need to check in your backend database to see if the 
	    UserID and Password is a valid pair. 
            You should construct your response accordingly:

            If YES, send back the user's Name and Profile Image URL.
            If NOT, set Name as "Unauthorized" and Profile Image URL as "#".
        */
        Statement stmt = null;

        try {
            stmt = conn.createStatement();
            String sql = "SELECT * FROM users WHERE user_id=" + id+";";
            ResultSet res = stmt.executeQuery(sql);

            if (res.next()) {
                // user_id and password doesn't match
                if (!pwd.equals(res.getString(2))) {

                    System.out.println("Incorrect: " + res.getString(2));

                    result.put("name", "Unauthorized");
                    result.put("profile", "#");
                    
                    // Close
                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    PrintWriter writer = response.getWriter();
                    writer.write(String.format("returnRes(%s)", result.toString()));
                    writer.close();
                    return;
                } else {
                    System.out.println("Correct: " + res.getString(2));
                }
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

        // If user_id and password matches
        stmt = null;
        try {
            stmt = conn.createStatement();
            String sql = "SELECT * FROM userinfo WHERE user_id=" + id+";";
            ResultSet res = stmt.executeQuery(sql);

            if (res.next()) {
                result.put("name", res.getString(2));
                result.put("profile", res.getString(3));
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
