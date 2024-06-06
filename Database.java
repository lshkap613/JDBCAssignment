package assignment13;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Scanner;

public class Database {

	public static void main(String[] args) {
		final String DATABASE_URL = "jdbc:sqlserver://localhost;"
				+ "databaseName=PREMIERECO;integratedSecurity=true;encrypt=true;TrustServerCertificate=true";
		Connection dbConnection = null;
		Savepoint spBeforeTransaction = null;
		
		try {
			// connect to database
			dbConnection = DriverManager.getConnection(DATABASE_URL);
			dbConnection.setAutoCommit(false);
			System.out.println("connected sucessfully");
			
			// display all tables
			displayAllTables(dbConnection);

			// MAKE TRANSACTION
			printTransactionHeader();
			
			// create scanner
			Scanner scan = new Scanner(System.in);
			
			// Select part
			System.out.println("Enter PART_NUM, full catalog found above in the part table (no validation so get it right)\n");
			System.out.print("Part number: ");
			String part_num = scan.nextLine();
			
			// Get price of part
			double price = getPriceOfPart(dbConnection, part_num);
			
			// Select Quantity
			System.out.println("\nQuantity: ");
			int quantity = scan.nextInt();
			
			// Select customer id
			System.out.println("\nCustomer Id (existing customers only): ");
			int customerID = scan.nextInt();
						
			// create save point
            spBeforeTransaction = dbConnection.setSavepoint("BeforeTransaction");
			
            // insert transaction into orders table
			String insertOrderSQL = "INSERT INTO orders (ORDER_DATE, CUST_NUM) VALUES (?, ?)";
			PreparedStatement insertOrderStmt = dbConnection.prepareStatement(insertOrderSQL, Statement.RETURN_GENERATED_KEYS);
			insertOrderStmt.setDate(1, new java.sql.Date(System.currentTimeMillis()));
			insertOrderStmt.setInt(2, customerID);
			
			insertOrderStmt.executeUpdate();
                     
			// get order number
			int order_num = getOrderNum(insertOrderStmt);
			
            insertOrderStmt.close();
			
            // insert transaction into order_line table
            String insertOrderLineSQL = "INSERT INTO order_line (ORDER_NUM, PART_NUM, QTY_ORDERED, QUOTED_PRICE, Modified_Date) VALUES (?, ?, ?, ?, ?)";
            PreparedStatement insertOrderLineStmt = dbConnection.prepareStatement(insertOrderLineSQL);
            
            insertOrderLineStmt.setInt(1, order_num);
            insertOrderLineStmt.setString(2, part_num);
            insertOrderLineStmt.setInt(3, quantity);
            insertOrderLineStmt.setDouble(4, price);
            insertOrderLineStmt.setDate(5, new java.sql.Date(System.currentTimeMillis()));

            insertOrderLineStmt.executeUpdate();
            insertOrderStmt.close();
			
			// update inventory
			String updatePartSQL = "UPDATE part SET units_on_hand = units_on_hand - ? WHERE PART_NUM = ?";
            PreparedStatement updatePartStmt = dbConnection.prepareStatement(updatePartSQL);

			updatePartStmt.setInt(1, quantity);
            updatePartStmt.setString(2, part_num);
            
            updatePartStmt.executeUpdate();
            updatePartStmt.close();

            // commit changes
            dbConnection.commit();
            System.out.println("Transaction committed successfully.");

			scan.close();

		} catch (Exception e) {
			e.printStackTrace();
			
			if (dbConnection != null) {
                try {
                	// Rollback to savepoint if set, else rollback entire transaction
                    if (spBeforeTransaction != null) {
                        dbConnection.rollback(spBeforeTransaction);
                        System.out.println("Rolled back to the savepoint.");
                    } else {
                        dbConnection.rollback();
                        System.out.println("Transaction rolled back.");
                    }
                } catch (SQLException rollbackEx) {
                    rollbackEx.printStackTrace();
                }
            }
		}  finally {
            if (dbConnection != null) {
                try {
                    dbConnection.setAutoCommit(true);
                    dbConnection.close();
                } catch (SQLException closeEx) {
                    closeEx.printStackTrace();
                }
            }
        }
	}
	
	
	// function that calls printTable function to display ALL tables in database
	private static void displayAllTables(Connection dbConnection) {
		DatabaseMetaData dbMetaData;
		try {
			dbMetaData = dbConnection.getMetaData();
		
			String[] types = { "TABLE" };
			ResultSet tables = dbMetaData.getTables(null, null, "%", types);
	
			while (tables.next()) {
				String tblName = tables.getString("TABLE_NAME");
				String tblSchema = tables.getString("TABLE_SCHEM");
				
	            if (!tblSchema.equalsIgnoreCase("sys") && !tblSchema.equalsIgnoreCase("information_schema")) {
	
					// Select all data from the current table
	                String query = "SELECT * FROM " + tblName;
	                PreparedStatement stmt = dbConnection.prepareStatement(query);
					ResultSet res = stmt.executeQuery();
					
					// get metadata of table
					ResultSetMetaData rsmd = res.getMetaData();				
					
					printTable(res, rsmd, tblName);
					
					stmt.close();
					res.close();	
	            }
			}
        } catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	// method to print specified table from database
	private static void printTable(ResultSet res, ResultSetMetaData rsmd, String tblName) {
		try {
			// print table name
			System.out.println("\n\n\t\t~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
			System.out.println("\t\t~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ " + tblName.toUpperCase() + " TABLE: ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
			System.out.println("\t\t~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n");

			// print column names
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				System.out.printf("%-32s", rsmd.getColumnName(i));
			}
			System.out.println();

			// print pretty divider
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				System.out.print("--------------------------------");
			}
			System.out.println();

			// print row data with data type of each column
			while (res.next()) {
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					Object obj = res.getObject(i);
					String value = (obj != null) ? shortenString(obj.toString(), 10) : "null         ";
					System.out.printf("%-25s\t", value + "  ||  " + rsmd.getColumnTypeName(i));
				}
				System.out.println();
			}
			
			System.out.println();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	// method to format part of data table
	private static String shortenString(String value, int length) {
		if (value.length() > length) {
			return value.substring(0, length) + "...";
		} else {
			int spaces = 13 - value.length();
			StringBuilder sb = new StringBuilder(value);
			for (int i = 0; i < spaces; i++) {
				sb.append(" ");
			}
			return sb.toString();
		}
	}
	
	
	private static void printTransactionHeader() {
		System.out.println("\n\n\n\t\t**************************************************************************************************");
		System.out.println("\t\t*********************************       MAKE A TRANSACTION        ********************************");
		System.out.println("\t\t**************************************************************************************************\n\n");
	}
	
	// method to get price of specified part from data base
	private static double getPriceOfPart(Connection dbConnection, String part_num) {
        double price = 0.0;
        String priceQuery = "SELECT Price FROM part WHERE PART_NUM = ?";
        PreparedStatement priceStmt;
        
        try {
			priceStmt = dbConnection.prepareStatement(priceQuery);
		
	        priceStmt.setString(1, part_num);
	        ResultSet priceRes = priceStmt.executeQuery();
	        
	        if (priceRes.next()) {
	            price = priceRes.getDouble("Price");
	        }
	        priceRes.close();
	        priceStmt.close();
	        	        
        } catch (Exception e) {
			e.printStackTrace();
		}
        
        return price;
	}
	
	
	private static int getOrderNum(Statement insertOrderStmt) {
		ResultSet generatedKeys;
        int order_num = 0;

		try {
			generatedKeys = insertOrderStmt.getGeneratedKeys();
			
	        if (generatedKeys.next()) {
	            order_num = generatedKeys.getInt(1);
	        }
	        generatedKeys.close();
	        
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
        return order_num;
	}

}
