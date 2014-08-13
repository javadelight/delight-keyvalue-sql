package de.mxro.async.map.sql.tests;

import java.sql.Connection;
import java.sql.DriverManager;

import org.junit.Test;

import de.mxro.async.map.sql.SqlAsyncMapDependencies;
import de.mxro.async.map.sql.SqlConnectionConfiguration;
import de.mxro.serialization.Serializer;
import de.mxro.serialization.jre.SerializationJre;
import de.mxro.serialization.jre.StreamDestination;
import de.mxro.serialization.jre.StreamSource;

public class TestThatValuesCanBeWrittenAndRead {

	@Test
	public void test() throws Exception {
		Class.forName("org.h2.Driver");
		Connection conn = DriverManager.
		    getConnection("jdbc:h2:mem:test");
		
		SqlConnectionConfiguration sqlConf = new SqlConnectionConfiguration() {
			
			@Override
			public String getDriverClassName() {
				return "org.h2.Driver";
			}

			@Override
			public boolean supportsInsertOrUpdate() {
				return false;
			}

			@Override
			public boolean supportsMerge() {
				return true;
			}

			@Override
			public String getMergeTemplate() {
				return "MERGE INTO " + getTableName()
						+ " (Id, Value) KEY (Id) VALUES (?, ?)";
			}

			@Override
			public String getConnectionString() {
				return "jdbc:h2:mem:test";
			}

			@Override
			public String getTableName() {
				return "test";
			}
		};
		
		final Serializer<StreamSource, StreamDestination> serializer = SerializationJre.newJavaSerializer();
		new SqlAsyncMapDependencies() {
			
			@Override
			public Serializer<StreamSource, StreamDestination> getSerializer() {
				
				return serializer;
			}
		};
		
		conn.close();
	}

}
