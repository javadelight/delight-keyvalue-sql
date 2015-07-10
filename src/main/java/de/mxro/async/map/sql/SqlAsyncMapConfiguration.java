package de.mxro.async.map.sql;

import delight.factories.Configuration;

import de.mxro.async.map.Store;

/**
 * Configuration for a SQL linked {@link Store}.
 * 
 * @author <a href="http://www.mxro.de">Max Rohde</a>
 *
 */
public interface SqlAsyncMapConfiguration extends Configuration {

	/**
	 * SQL configuration for this map.
	 * 
	 * @return
	 */
	public SqlConnectionConfiguration sql();

}
