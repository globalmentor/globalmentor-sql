package com.garretwilson.sql;

import java.io.File;
import java.sql.*;
import java.text.*;
import java.util.*;
import javax.naming.NamingException;
import javax.sql.*;

import static com.garretwilson.lang.SystemUtilities.*;
import com.garretwilson.util.Debug;

/**A class that facilitates access to JDBC databases.
<p>The database properties expected are the following:</p>
<dl>
	<dt>driver</dt> <dd>The class name of the database driver.</dd>
	<dt>user</dt> <dd>The user name for connecting to the database.</dd>
	<dt>password</dt> <dd>The user password for connecting to the database.</dd>
	<dt>url</dt> <dd>The URL for connecting to the database.</dd>
</dl>
<p>The URL accepts the following replacement parameters:</p>
<dl>
	<dt>{0}</dt> <dd>The supplied database directory, or the user directory
		if none was specified when this class was created.</dd>
</dl>
@author Garret Wilson
*/
public class JDBCManager
{

	/**The property for the JDBC driver class.*/
	final static String DRIVER_PROPERTY="driver";
	/**The property for the JDBC database URL.*/
	final static String URL_PROPERTY="url";

	/**The default class name of the database driver.*/
	final static String DEFAULT_DRIVER="org.hsqldb.jdbcDriver";
	/**The default URL of the database.*/
	final static String DEFAULT_URL="jdbc:hsqldb:defaultdb";
	/**The database username.*/
//G***del	public final static String DATABASE_USERNAME="sa";
	/**The database password.*/
//G***del	public final static String DATABASE_PASSWORD="";

	/**The directory specified for storing the database, if needed.*/
	private final File databaseDirectory;

		/**@return The directory specified for storing the database, if needed.*/
		protected File getDatabaseDirectory() {return databaseDirectory;}

	/**Registers a driver for a database.
	@param driverClassName The class name of the database driver to register.
	*/
	public static void registerDriver(final String driverClassName)
	{
		try
		{
		  Class.forName(driverClassName); //register the driver class
		}
/*G***fix
		catch(ClassNotFoundException classNotFoundException)
		{
			Debug.error(classNotFoundException);  //G***fix
		}
*/
		catch(Exception exception)
		{
			Debug.error(exception);  //G***fix
		}
	}

	/**The JDBC properties that contain information such as driver and username.*/
	private final Properties properties;

	/**Default constructor.*/
	public JDBCManager()
	{
		this(new Properties()); //create default empty properties
	}

	/**Properties constructor.
	@param properties The JDBC properties that contain information such as driver
		and username.
	*/
	public JDBCManager(final Properties properties)
	{
		this(properties, null); //construct the manager with the user directory as a database directory
	}

	/**Properties and directory constructor.
	@param properties The JDBC properties that contain information such as driver
		and username.
	@param directory The directory in which to store the database (if the
		properties files provides a replacement parameter), or <code>null</code> if
		the user directory should be used for the directory.
	*/
	public JDBCManager(final Properties properties, final File directory)
	{
		this.properties=properties; //save the properties
		if(directory!=null) //if a directory was given
		{
			databaseDirectory=directory;  //store the directory
		}
		else  //if a directory was not given
		{
			databaseDirectory=getUserHomeDirectory();  //use the home directory
		}
	}

	/**@return A new data source to serve as a connection factory to the database.
	@exception NamingException Thrown if there is an error retrieving the data source.
	@exception SQLException Thrown if there is an error accessing the database.
	*/
	public DataSource getDataSource() throws NamingException, SQLException
	{
/*G***first try to lookup via JNDI from a properties file some data source
//G***del		Debug.notify("Getting connection"); //G***del
//G***del when works		return DriverManager.getConnection(DATABASE_URL, DATABASE_USERNAME, DATABASE_PASSWORD);	//get a connection and return it
		final InitialContext context=new InitialContext();  //obtain an initial context
		final DataSource dataSource=(DataSource)context.lookup("jdbc/HypersonicCoreDS");  //G***testing
*/
		  //get the database driver
		final String driver=properties.getProperty(DRIVER_PROPERTY, DEFAULT_DRIVER);
		registerDriver(driver); //make sure the database driver is registered
		  //get the database URL and replace {0} with the database directory
		final String url=MessageFormat.format(properties.getProperty(URL_PROPERTY, DEFAULT_URL), new Object[]{getDatabaseDirectory().toString()});
//G***del Debug.notify(url);  //G***del
		return new DefaultDataSource(url, properties);  //create a default data source using the database URL and given properties
	}

}
