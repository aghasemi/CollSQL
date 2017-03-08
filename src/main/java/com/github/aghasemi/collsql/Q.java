package com.github.aghasemi.collsql;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringBufferInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import javax.swing.plaf.FileChooserUI;

public class Q 
{	
    private static int base=0;
    
	
	private static String newLineChar="\n";
	private static String sepChar="\t"; //«π» To avoid any conflict
	private static String collectionToCSVString(Iterable<Object[]> collection)
	{
		StringBuilder result=new StringBuilder();
		for (Iterator iterator = collection.iterator(); iterator.hasNext();) {
			String[] row = (String[]) iterator.next();
			result.append(String.join(sepChar, row));
			result.append(newLineChar);
		}
		return result.toString();
	}
	
	private static Statement saveCollectionToDatabase(Iterable<Object[]> collection)
	{
		try {
			Connection connection = DriverManager.getConnection("jdbc:sqlite::memory:");
			Statement stmt=connection.createStatement();
			String tableName="T";
			Iterator<Object[]> iterator=collection.iterator();
			int numCols=iterator.next().length;
			
			String[] colNames=IntStream.range(0, numCols).mapToObj(i -> "c"+(i+base)).toArray(String[]::new);
			
				stmt.executeUpdate("drop table if exists "+tableName);	
				String tableCreationString=String.format("create table %s (%s)", tableName,String.join(",",colNames));
				stmt.executeUpdate(tableCreationString);
				
				collection.forEach(new Consumer<Object[]>() {

					@Override
					public void accept(Object[] row) {
						
						 try {
							String questionMarks=String.join(",",Arrays.stream(row).map(val -> "?").toArray(String[]::new));
							String insertStmt=String.format("insert into %s values (%s)", tableName,questionMarks);
							
							PreparedStatement prepStmt=connection.prepareStatement(insertStmt);
							for (int i = 0; i < colNames.length; i++) {
								prepStmt.setString(i+1, row[i].toString());
							}
							prepStmt.executeUpdate();
							prepStmt.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
				});
				return stmt;
		} catch (SQLException e) 
			{
			
			e.printStackTrace();
		}
		throw new IllegalStateException("A SQL exception has occured");
	}
	
	
	private static Statement saveCollectionToDatabase(Object[][] collection)
	{
		return saveCollectionToDatabase(Arrays.asList(collection));
	}
	
	
	
	public static List<String[]> query(Object[][] dataset,String query)
	{
		
		Statement stmt = saveCollectionToDatabase(Arrays.asList(dataset));

		return runSQLQuery(query,stmt);
		
	}
	
	public static List<String[]> query(Iterable<Object[]> dataset,String query)
	{
		
		Statement stmt = saveCollectionToDatabase(dataset);

		
		return runSQLQuery(query,stmt);
	}
	
	private static List<String[]> runSQLQuery(String query,Statement stmt) 
	{
		List<String[]> result=new ArrayList<>();
		try {
			ResultSet rset = stmt.executeQuery(query);
			int numCols=rset.getMetaData().getColumnCount();

			while(rset.next())
			{
				
				result.add(IntStream.range(0, numCols).mapToObj(new IntFunction<String>() {

					@Override
					public String apply(int i) {
						try {
							return rset.getString(i+1);
						} catch (SQLException e) {
							
							e.printStackTrace();
						}
						return null;
					}
				}).map(val -> val).toArray(String[]::new));
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}
	


}
