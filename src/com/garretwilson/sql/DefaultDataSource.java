package com.garretwilson.sql;

import java.io.PrintWriter;
import java.sql.*;
import java.util.*;
import javax.sql.*;
import com.garretwilson.util.Debug;

/**A default implementation of a data source that allows direct connections
	to the database using the <code>DriverManager</code>.
@author Garret Wilson
@see DriverManager
*/
public class DefaultDataSource implements DataSource, JDBCConstants
{

	/**The URL identifying the database.*/
	private final String url;

		/**@return The URL identifying the database.*/
		public String getURL() {return url;}

	/**The database user on whose behalf connections are made.*/
//G***del	private final String username;

		/**@return The database user on whose behalf connections are made.*/
//G***del		protected String getUsername() {return username;}

	/**The user's password.*/
//G***del	private final String password;

		/**@return The user's password.*/
//G***del		protected String getPassword() {return password;}

	/**Properties such as <code>user</code> and <code>password</code> for the
		database connection.
	*/
	private final Properties properties;

		/**@return Properties such as <code>user</code> and <code>password</code>
			for the database connection.
		*/
		protected Properties getProperties() {return properties;}

	/**The log writer for this data source, initially <code>nul</code>.*/
//G***del	protected LogWriter logWriter=null;

	/**The login timeout, initially zero.*/
//G***del	protected int loginTimeout=0;

	/**Constructor that specifies no username and password.
	@param databaseURL A database url of the form
		<code>jdbc:<em>subprotocol</em>:<em>subname</em></code>.
	*/
	public DefaultDataSource(final String databaseURL)
	{
		this(databaseURL, null, null); //construct the class with no default username and password
	}

	/**Username and password constructor. The given username and password will
		be used in all default database connections.
	@param databaseURL A database url of the form
		<code>jdbc:<em>subprotocol</em>:<em>subname</em></code>.
	@param defaultUsername The database user on whose behalf connections are made.
	@param defaultPassword The user's password.
	*/
	public DefaultDataSource(final String databaseURL, final String defaultUsername, final String defaultPassword)
	{
		url=databaseURL;  //save the URL to the database
		properties=new Properties();  //create new properties
		if(defaultUsername!=null) //if a username is specified
			properties.setProperty(USER_PROPERTY, defaultUsername); //save the username in the properties
		if(defaultPassword!=null) //if a password is specified
			properties.setProperty(PASSWORD_PROPERTY, defaultPassword); //save the password in the properties
/*G***Del
		username=defaultUsername; //save the username
		password=defaultPassword; //save the password
*/
	}

	/**Properties constructor. The properties must contain values for at least
		<code>username</code> and <code>password</code>, and these properties will
		be used in all database connections.
	@param databaseURL A database url of the form
		<code>jdbc:<em>subprotocol</em>:<em>subname</em></code>.
	@param properties The properties containing JDBC options.
	*/
	public DefaultDataSource(final String databaseURL, final Properties properties)
	{
		url=databaseURL;  //save the URL to the database
		this.properties=properties; //store the properties
/*G***del
		username=defaultUsername; //save the username
		password=defaultPassword; //save the password
*/
	}

	/**Attempts to establish a database connection using the default username and
		password, if they were specified when this class was created, or with
		no username and password if they were not specified.
	@return The new database connection.
	@exception SQLException Thrown if a database error occurs.
	*/
	public Connection getConnection() throws SQLException
	{
//G***del Debug.trace("attempting to get connection to URL: ", getURL()); //G***del
//G***del Debug.trace("connection properties: ", getProperties()); //G***del
		return DriverManager.getConnection(getURL(), getProperties());  //get a connection using the given properties
/*G***del
		if(getUsername()!=null && getPassword()!=null)  //if a default username and password were supplied
		  return getConnection(getUsername(), getPassword());    //get a connection using the default username and password
		else  //if we have no default username and password
			return DriverManager.getConnection(getURL());  //get the default connection
*/
	}

	/**Attempts to establish a database connection using the given username and
		password.
	@param username The database user on whose behalf connections are made.
	@param password The user's password.
	@return The new database connection.
	@exception SQLException Thrown if a database error occurs.
	*/
	public Connection getConnection(final String username, final String password) throws SQLException
	{
		return DriverManager.getConnection(getURL(), username, password);	//get a connection and return it
	}

  /**Retrieves the the global <code>DriverManager</code> log writer as the
		current log writer.
	@return The log writer for this data source, or <code>null</code> if no
		log writer has been set.
	@exception SQLException Thrown if a database error occurs.
	@see DriverManager#getLogWriter
	*/
	public PrintWriter getLogWriter() throws SQLException
	{
		return DriverManager.getLogWriter();  //get the driver manager's log writer
//G***del		return logWriter; //return the log writer
	}

	/**Retrieves the global <code>DriverManager</code> login timeout value as
		the current login timeout.
	@return The current login timeout, or zero if the system default should
		be used, if there is one.
	@exception SQLException Thrown if a database error occurs.
	@see DriverManager#getLoginTimeout
	*/
  public int getLoginTimeout() throws SQLException
	{
		return DriverManager.getLoginTimeout(); //return the driver manager's login timeout
//G***del		return loginTimeout;  //return the login timeout
	}

	/**Sets the log writer for this data source. This version update the global
		<code>DriverManager</code> log writer.
	@param out The new log writer.
	@exception SQLException Thrown if a database error occurs.
	@see DriverManager#setLogWriter
	*/
	public void setLogWriter(final PrintWriter out) throws SQLException
	{
//G***del		logWriter=out;  //update the log writer G***fix to actually update something meaningful
		DriverManager.setLogWriter(out);  //update the log writer
	}

	/**Sets the maximum time in seconds that this data source will wait while
		attempting to connect to a database. This version updates the global
		<code>DriverManager</code> login timeout.
	@param seconds The new login timeout.
	@exception SQLException Thrown if a database error occurs.
	@see DriverManager#setLoginTimeout
	*/
  public void setLoginTimeout(final int seconds) throws SQLException
	{
//G***del		loginTimeout=seconds; //set the login timeout
		DriverManager.setLoginTimeout(seconds); //update the login timeout
	}
}
