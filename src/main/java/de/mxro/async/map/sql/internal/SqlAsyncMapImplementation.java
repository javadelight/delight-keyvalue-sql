package de.mxro.async.map.sql.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import mx.gwtutils.concurrent.SingleInstanceQueueWorker;
import one.utils.jre.OneUtilsJre;
import de.mxro.async.callbacks.SimpleCallback;
import de.mxro.async.callbacks.ValueCallback;
import de.mxro.async.map.AsyncMap;
import de.mxro.async.map.operations.MapOperation;
import de.mxro.async.map.sql.SqlAsyncMapConfiguration;
import de.mxro.async.map.sql.SqlAsyncMapDependencies;
import de.mxro.concurrency.Executor;
import de.mxro.concurrency.Executor.WhenExecutorShutDown;
import de.mxro.fn.Fn;
import de.mxro.serialization.jre.SerializationJre;

public class SqlAsyncMapImplementation<V> implements AsyncMap<String, V> {

	private final static boolean ENABLE_DEBUG = false;

	private final SqlAsyncMapConfiguration conf;
	
	private final SqlAsyncMapDependencies deps;
	
	// internal helper
	private java.sql.Connection connection;
	private final Map<String, Object> pendingInserts;
	private final Set<String> pendingGets;
	private final ExecutorService commitThread;
	private final WriteWorker writeWorker;

	private final static Object DELETE_NODE = Fn.object();

	
	
	
	

	private class WriteWorker extends SingleInstanceQueueWorker<String> {

		@Override
		protected void processItems(final List<String> items) {

			synchronized (pendingInserts) {

				if (ENABLE_DEBUG) {
					System.out.println("SqlConnection: Inserting ["
							+ items.size() + "] elements.");
				}

				for (final String item : items) {
					final String uri = item;

					final Object data;

					if (!pendingInserts.containsKey(uri)) {
						if (ENABLE_DEBUG) {
							System.out
									.println("SqlConnection: Insert has been performed by previous operation ["
											+ uri + "].");
						}
						continue;
					}

					data = pendingInserts.get(uri);

					assertConnection();

					try {
						writeToSqlDatabase(uri, data);
					} catch (final Throwable t) {
						// try reconnecting once if any errors occur
						// in order to deal with mysql automatic disconnect
						initConnection();
						try {
							writeToSqlDatabase(uri, data);
						} catch (final SQLException e) {
							throw new RuntimeException(e);
						}
					}
				}

				try {
					connection.commit();
				} catch (final SQLException e) {
					throw new RuntimeException(e);
				}

				if (ENABLE_DEBUG) {
					System.out.println("SqlConnection: Inserting ["
							+ items.size() + "] elements completed.");
				}

				for (final String item : items) {
					// assert pendingInserts.containsKey(item);
					// might have been done by previous op
					pendingInserts.remove(item);
				}

			}

		}

		private final void writeToSqlDatabase(final String uri,
				final Object data) throws SQLException {

			assert data != null : "Trying to write node <null> to database.\n"
					+ "  Node: " + uri;

			if (data == DELETE_NODE) {
				performDelete(uri);
				return;
			}

			if (conf.sql().supportsMerge()) {
				performMerge(uri, data);
				return;
			}

			if (conf.sql().supportsInsertOrUpdate()) {
				performInsertOrUpdate(uri, data);
				return;
			}

			performInsert(uri, data);

		}

		private void performInsert(final String uri, final Object data)
				throws SQLException {
			final ByteArrayOutputStream os = new ByteArrayOutputStream();
			deps.getSerializer().serialize(data, SerializationJre.createStreamDestination(os));
			final byte[] bytes = os.toByteArray();

			try {
				if (performGet(uri) == null) {
					PreparedStatement insertStatement = null;
					try {
						insertStatement = connection.prepareStatement(conf
								.sql().getInsertTemplate());

						insertStatement.setQueryTimeout(10);

						insertStatement.setString(1, uri);
						insertStatement.setBinaryStream(2,
								new ByteArrayInputStream(bytes));

						if (ENABLE_DEBUG) {
							System.out.println("SqlConnection: Inserting ["
									+ uri + "].");
						}
						insertStatement.executeUpdate();
						// connection.commit();
					} finally {
						if (insertStatement != null) {
							insertStatement.close();
						}
					}
					return;
				} else {

					PreparedStatement updateStatement = null;
					try {
						updateStatement = connection.prepareStatement(conf
								.sql().getUpdateTemplate());
						updateStatement.setQueryTimeout(10);

						updateStatement.setBinaryStream(1,
								new ByteArrayInputStream(bytes));
						updateStatement.setString(2, uri);
						if (ENABLE_DEBUG) {
							System.out.println("SqlConnection: Updating ["
									+ uri + "].");
						}
						updateStatement.executeUpdate();
						// connection.commit();
					} finally {
						if (updateStatement != null) {
							updateStatement.close();
						}
					}

				}
			} catch (final IOException e) {
				throw new RuntimeException(e);
			}
		}

		private void performMerge(final String uri, final Object data)
				throws SQLException {
			final ByteArrayOutputStream os = new ByteArrayOutputStream();
			deps.getSerializer().serialize(data, SerializationJre.createStreamDestination(os));
			final byte[] bytes = os.toByteArray();

			PreparedStatement mergeStatement = null;
			try {
				mergeStatement = connection.prepareStatement(conf.sql()
						.getMergeTemplate());

				mergeStatement.setQueryTimeout(10);

				mergeStatement.setString(1, uri);
				mergeStatement.setBinaryStream(2, new ByteArrayInputStream(
						bytes));

				if (ENABLE_DEBUG) {
					System.out.println("SqlConnection: Merging [" + uri + "].");
				}
				mergeStatement.executeUpdate();
				// connection.commit();
			} finally {
				if (mergeStatement != null) {
					mergeStatement.close();
				}
			}

		}

		private void performInsertOrUpdate(final String uri,
				final Object data) throws SQLException {
			final ByteArrayOutputStream os = new ByteArrayOutputStream();
			deps.getSerializer().serialize(data, SerializationJre.createStreamDestination(os));
			final byte[] bytes = os.toByteArray();

			PreparedStatement insertStatement = null;
			try {
				insertStatement = connection.prepareStatement(conf.sql()
						.getInsertOrUpdateTemplate());
				insertStatement.setQueryTimeout(10);
				insertStatement
						.setString(1, uri);

				// TODO this seems somehow non-optimal, probably the
				// byte data is sent to the database twice ...
				insertStatement.setBinaryStream(2, new ByteArrayInputStream(
						bytes));
				insertStatement.setBinaryStream(3, new ByteArrayInputStream(
						bytes));
				insertStatement.executeUpdate();
				if (ENABLE_DEBUG) {
					System.out.println("SqlConnection: Inserting [" + uri
							+ "].");
				}

				// connection.commit();
			} finally {
				if (insertStatement != null) {
					insertStatement.close();
				}
			}
		}

		private void performDelete(final String uri) throws SQLException {
			PreparedStatement deleteStatement = null;

			try {
				deleteStatement = connection.prepareStatement(conf.sql()
						.getDeleteTemplate());
				deleteStatement.setQueryTimeout(10);

				deleteStatement
						.setString(1, uri);
				deleteStatement.executeUpdate();
				if (ENABLE_DEBUG) {
					System.out
							.println("SqlConnection: Deleting [" + uri + "].");
				}

				// connection.commit();
			} finally {
				if (deleteStatement != null) {
					deleteStatement.close();
				}
			}
		}

		public WriteWorker(final Executor executor, final Queue<String> queue) {
			super(executor, queue, OneUtilsJre.newJreConcurrency());
		}

	}

	private final void scheduleWrite(final String uri) {

		writeWorker.offer(uri);
		writeWorker.startIfRequired();

	}

	@Override
	public void putSync(final String uri, final V node) {

		synchronized (pendingInserts) {

			// System.out.println("Currently pending:  " +
			// pendingInserts.size());
			pendingInserts.put(uri, node);

		}
		scheduleWrite(uri);
	}

	public static class SqlGetResources {
		ResultSet resultSet;
		PreparedStatement getStatement;

	}

	@SuppressWarnings("unchecked")
	@Override
	public V getSync(String key) {
		
		return (V) getNode(key);
	}
	
	public Object getNode(final String uri) {

		synchronized (pendingInserts) {

			if (ENABLE_DEBUG) {
				System.out.println("SqlConnection: Retrieving [" + uri + "].");
			}

			if (pendingInserts.containsKey(uri)) {

				final Object node = pendingInserts.get(uri);

				if (ENABLE_DEBUG) {
					System.out.println("SqlConnection: Was cached [" + uri
							+ "] Value [" + node + "].");
				}

				if (node == DELETE_NODE) {
					return null;
				}

				return node;
			}

			assert !pendingInserts.containsKey(uri);

			pendingGets.add(uri);

			try {

				final Object performGet = performGet(uri);

				assert pendingGets.contains(uri);
				pendingGets.remove(uri);

				return performGet;

			} catch (final Exception e) {

				pendingGets.remove(uri);
				throw new IllegalStateException(
						"SQL connection cannot load node: " + uri, e);
			}

		}
	}

	private Object performGet(final String uri) throws SQLException,
			IOException {
		assertConnection();

		SqlGetResources getResult = null;

		try {

			try {
				getResult = readFromSqlDatabase(uri);
			} catch (final Throwable t) {
				// try reconnecting once if any error occurs
				initConnection();
				try {
					getResult = readFromSqlDatabase(uri);
				} catch (final SQLException e) {
					throw new RuntimeException(e);
				}
			}

			if (!getResult.resultSet.next()) {

				if (ENABLE_DEBUG) {
					System.out.println("SqlConnection: Not found [" + uri
							+ "].");
				}

				return null;
			}

			final InputStream is = getResult.resultSet.getBinaryStream(2);

			final byte[] data = OneUtilsJre.toByteArray(is);
			is.close();
			getResult.resultSet.close();
			assert data != null;

			final Object node = deps.getSerializer().deserialize(SerializationJre.createStreamSource(new ByteArrayInputStream(data)));
			if (ENABLE_DEBUG) {
				System.out.println("SqlConnection: Retrieved [" + node + "].");
			}
			return node;

		} finally {
			if (getResult != null) {
				getResult.getStatement.close();
			}
		}
	}

	private final SqlGetResources readFromSqlDatabase(final String uri)
			throws SQLException {
		PreparedStatement getStatement = null;

		getStatement = connection.prepareStatement(conf.sql().getGetTemplate());

		getStatement.setQueryTimeout(10);

		getStatement.setString(1, uri);

		final ResultSet resultSet = getStatement.executeQuery();

		connection.commit();

		final SqlGetResources res = new SqlGetResources();
		res.resultSet = resultSet;
		res.getStatement = getStatement;

		return res;

	}

	
	
	@Override
	public void removeSync(String key) {
		deleteNode(key);
	}

	public void deleteNode(final String uri) {

		synchronized (pendingInserts) {
			// new Exception("Schedule delete " + uri).printStackTrace();
			pendingInserts.put(uri, DELETE_NODE);
		}
		scheduleWrite(uri);

	}

	public synchronized void waitForAllPendingRequests(
			final SimpleCallback callback) {

		new Thread() {

			@Override
			public void run() {
				if (ENABLE_DEBUG) {
					System.out
							.println("SqlConnection: Waiting for pending requests.\n"
									+ "  Write worker running: ["
									+ writeWorker.isRunning()
									+ "]\n"
									+ "  Pending inserts: ["
									+ pendingInserts.size()
									+ "]\n"
									+ "  Pending gets: ["
									+ pendingGets.size()
									+ "]");
				}
				while (writeWorker.isRunning() || pendingInserts.size() > 0
						|| pendingGets.size() > 0) {
					try {
						Thread.sleep(10);
					} catch (final Exception e) {
						callback.onFailure(e);
					}
					Thread.yield();
				}

				if (ENABLE_DEBUG) {
					System.out
							.println("SqlConnection: Waiting for pending requests completed.");
				}

				callback.onSuccess();
			}

		}.start();

	}

	
	
	
	
	@Override
	public void put(String key, V value, SimpleCallback callback) {
		putSync(key, value);
		
		this.commit(callback);
	}

	@Override
	public void get(final String key, final ValueCallback<V> callback) {
		final V value = getSync(key);
		callback.onSuccess(value);
		
	}

	@Override
	public void remove(String key, SimpleCallback callback) {
		removeSync(key);
		
		this.commit(callback);
		
	}

	@Override
	public void performOperation(MapOperation operation) {
		// nothing
	}

	@Override
	public void stop(final SimpleCallback callback) {
		this.commit(new SimpleCallback() {
			
			@Override
			public void onFailure(Throwable t) {
				callback.onFailure(t);
			}
			
			@Override
			public void onSuccess() {
				writeWorker.getThread().getExecutor()
				.shutdown(new WhenExecutorShutDown() {

					@Override
					public void thenDo() {
						try {

							if (connection != null
									&& !connection.isClosed()) {
								try {
									connection.commit();
									connection.close();
								} catch (final Throwable t) {
									callback
											.onFailure(new Exception(
													"Sql exception could not be closed.",
													t));
									return;
								}
							}

						} catch (final Throwable t) {
							callback.onFailure(t);
							return;
						}

						callback.onSuccess();
					}

					@Override
					public void onFailure(final Throwable t) {
						callback.onFailure(t);
					}
				});
				
			}
		});
	}

	
	

	@Override
	public void commit(final SimpleCallback callback) {
		commitThread.submit(new Runnable() {

			@Override
			public void run() {
				// writeWorker.startIfRequired();
				waitForAllPendingRequests(new SimpleCallback() {

					@Override
					public void onSuccess() {
						callback.onSuccess();
					}

					@Override
					public void onFailure(final Throwable message) {
						callback.onFailure(message);
					}
				});
			}

		});
	}

	

	protected void assertConnection() {
		try {
			if (connection == null || connection.isClosed()) {
				initConnection();
			}
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	protected void initConnection() {
		try {
			connection = DriverManager.getConnection(conf.sql()
					.getConnectionString());

			connection.setAutoCommit(false);
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	

	@Override
	public void start(SimpleCallback callback) {
		try  {
			assertConnection();
			
		} catch (Throwable t) {
			callback.onFailure(t);
			return;
		}
		callback.onSuccess();
	}

	public SqlAsyncMapImplementation(final SqlAsyncMapConfiguration conf, SqlAsyncMapDependencies deps) {
		super();

		this.conf = conf;
		this.deps = deps;

		this.pendingInserts = Collections
				.synchronizedMap(new HashMap<String, Object>(100));
		this.pendingGets = Collections.synchronizedSet(new HashSet<String>());

		this.writeWorker = new WriteWorker(OneUtilsJre.newJreConcurrency()
				.newExecutor().newSingleThreadExecutor(this),
				new ConcurrentLinkedQueue<String>());

		this.commitThread = Executors.newFixedThreadPool(1);

	}

}
