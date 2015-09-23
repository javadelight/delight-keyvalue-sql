package de.mxro.async.map.sql.internal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import de.mxro.async.map.sql.SqlStoreConnectionConfiguration;

public class SqlConnectionFactory {

	public static Connection createConnection(SqlStoreConnectionConfiguration conf) {
		
		try {
			Class.forName(conf.getDriverClassName());
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		
		Connection connection;
		try {
			connection = DriverManager.getConnection(conf
					.getConnectionString());

			connection.setAutoCommit(false);
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
		return connection;
	}
	
}
