package com.github.aghasemi;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.github.aghasemi.collsql.Q;

public class QTest {
	
	@Test
	public void testQueries() throws IOException
	{
		System.out.println(System.getProperty("user.dir"));
		String[][] data=new String[][]{{"1","4","11"},{"2","0","6"}};
		
		Q.query(data, "SELECT c0,c1 FROM T").forEach(row -> System.out.println(Arrays.toString(row)));
		
		
		
		
	}

}
