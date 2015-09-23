package de.mxro.async.map.sql;

import delight.factories.Configuration;
import delight.keyvalue.Store;

/**
 * Configuration for a SQL linked {@link Store}.
 * 
 * @author <a href="http://www.mxro.de">Max Rohde</a>
 *
 */
public interface SqlStoreConfiguration extends Configuration {

	/**
	 * SQL configuration for this map.
	 * 
	 * @return
	 */
	public SqlStoreConnectionConfiguration sql();

}
