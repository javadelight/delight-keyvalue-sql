package de.mxro.async.map.sql;

import delight.factories.Dependencies;
import delight.keyvalue.Store;

import de.mxro.serialization.Serializer;
import de.mxro.serialization.jre.StreamDestination;
import de.mxro.serialization.jre.StreamSource;

/**
 * Run-time dependencies for SQL backed {@link Store}
 * 
 * @author <a href="http://www.mxro.de">Max Rohde</a>
 *
 */
public interface SqlAsyncMapDependencies extends Dependencies {

	/**
	 * 
	 * @return Serializer used to serialize objects submitted to the map.
	 */
	public abstract Serializer<StreamSource, StreamDestination> getSerializer();

}
