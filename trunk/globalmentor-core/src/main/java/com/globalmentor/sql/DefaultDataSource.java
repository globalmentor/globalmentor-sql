/*
 * Copyright © 1996-2009 GlobalMentor, Inc. <http://www.globalmentor.com/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.globalmentor.sql;

import java.io.PrintWriter;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

import javax.sql.*;

import static com.globalmentor.sql.JDBC.*;

/**
 * A default implementation of a data source that allows direct connections to the database using the {@link DriverManager}.
 * @author Garret Wilson
 * @see DriverManager
 */
public class DefaultDataSource implements DataSource {

	/** The URL identifying the database. */
	private final String url;

	/** @return The URL identifying the database. */
	public String getURL() {
		return url;
	}

	/**
	 * Properties such as <code>user</code> and <code>password</code> for the database connection.
	 */
	private final Properties properties;

	/**
	 * @return Properties such as <code>user</code> and <code>password</code> for the database connection.
	 */
	protected Properties getProperties() {
		return properties;
	}

	/**
	 * Constructor that specifies no username and password.
	 * @param databaseURL A database url of the form <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>.
	 */
	public DefaultDataSource(final String databaseURL) {
		this(databaseURL, null, null); //construct the class with no default username and password
	}

	/**
	 * Username and password constructor. The given username and password will be used in all default database connections.
	 * @param databaseURL A database url of the form <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>.
	 * @param defaultUsername The database user on whose behalf connections are made.
	 * @param defaultPassword The user's password.
	 */
	public DefaultDataSource(final String databaseURL, final String defaultUsername, final String defaultPassword) {
		url = databaseURL; //save the URL to the database
		properties = new Properties(); //create new properties
		if(defaultUsername != null) //if a username is specified
			properties.setProperty(USER_PROPERTY, defaultUsername); //save the username in the properties
		if(defaultPassword != null) //if a password is specified
			properties.setProperty(PASSWORD_PROPERTY, defaultPassword); //save the password in the properties
	}

	/**
	 * Properties constructor. The properties must contain values for at least <code>username</code> and <code>password</code>, and these properties will be used
	 * in all database connections.
	 * @param databaseURL A database url of the form <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>.
	 * @param properties The properties containing JDBC options.
	 */
	public DefaultDataSource(final String databaseURL, final Properties properties) {
		url = databaseURL; //save the URL to the database
		this.properties = properties; //store the properties
	}

	/**
	 * Attempts to establish a database connection using the default username and password, if they were specified when this class was created, or with no
	 * username and password if they were not specified.
	 * @return The new database connection.
	 * @throws SQLException Thrown if a database error occurs.
	 */
	public Connection getConnection() throws SQLException {
		return DriverManager.getConnection(getURL(), getProperties()); //get a connection using the given properties
	}

	/**
	 * Attempts to establish a database connection using the given username and password.
	 * @param username The database user on whose behalf connections are made.
	 * @param password The user's password.
	 * @return The new database connection.
	 * @throws SQLException Thrown if a database error occurs.
	 */
	public Connection getConnection(final String username, final String password) throws SQLException {
		return DriverManager.getConnection(getURL(), username, password); //get a connection and return it
	}

	/**
	 * Retrieves the the global <code>DriverManager</code> log writer as the current log writer.
	 * @return The log writer for this data source, or <code>null</code> if no log writer has been set.
	 * @throws SQLException Thrown if a database error occurs.
	 * @see DriverManager#getLogWriter
	 */
	public PrintWriter getLogWriter() throws SQLException {
		return DriverManager.getLogWriter(); //get the driver manager's log writer
	}

	/**
	 * Retrieves the global <code>DriverManager</code> login timeout value as the current login timeout.
	 * @return The current login timeout, or zero if the system default should be used, if there is one.
	 * @throws SQLException Thrown if a database error occurs.
	 * @see DriverManager#getLoginTimeout
	 */
	public int getLoginTimeout() throws SQLException {
		return DriverManager.getLoginTimeout(); //return the driver manager's login timeout
	}

	/**
	 * Sets the log writer for this data source. This version update the global <code>DriverManager</code> log writer.
	 * @param out The new log writer.
	 * @throws SQLException Thrown if a database error occurs.
	 * @see DriverManager#setLogWriter
	 */
	public void setLogWriter(final PrintWriter out) throws SQLException {
		DriverManager.setLogWriter(out); //update the log writer
	}

	/**
	 * Sets the maximum time in seconds that this data source will wait while attempting to connect to a database. This version updates the global
	 * <code>DriverManager</code> login timeout.
	 * @param seconds The new login timeout.
	 * @throws SQLException Thrown if a database error occurs.
	 * @see DriverManager#setLoginTimeout
	 */
	public void setLoginTimeout(final int seconds) throws SQLException {
		DriverManager.setLoginTimeout(seconds); //update the login timeout
	}

	/**
	 * Returns an object that implements the given interface to allow access to non-standard methods, or standard methods not exposed by the proxy. This
	 * implementation always throws an {@link SQLException}.
	 * @param iface A class defining an interface that the result must implement.
	 * @return An object that implements the interface. May be a proxy for the actual implementing object.
	 * @throws SQLException If no object found that implements the interface
	 */
	public <T> T unwrap(final Class<T> iface) throws SQLException {
		throw new SQLException("No implementation of interface " + iface + " found.");
	}

	/**
	 * Returns <code>true</code> if this either implements the interface argument or is directly or indirectly a wrapper for an object that does. Returns false
	 * otherwise. This implementatoin always returns <code>false</code>.
	 * @param iface A class defining an interface.
	 * @return <code>true</code> if this implements the interface or directly or indirectly wraps an object that does.
	 * @throws SQLException if an error occurs while determining whether this is a wrapper for an object with the given interface.
	 */
	public boolean isWrapperFor(final Class<?> iface) throws SQLException {
		return false; //this class is not a wrapper for any other class
	}

	/**
	 * New JDK 7 requirement. This implementation throws {@link SQLFeatureNotSupportedException}.
	 */
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}
}
