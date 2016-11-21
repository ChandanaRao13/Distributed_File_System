package gash.router.database;

import java.util.LinkedList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;

import gash.router.server.manage.exceptions.EmptyConnectionPoolException;
import gash.router.util.Constants;

/**
 * DatabaseConnectionManager for creating a connection pool for handling
 * connections to RethinkDB
 * 
 * @author vaishampayan
 *
 */
public class DatabaseConnectionManager {

	private static DatabaseConnectionManager instance = null;
	public static final RethinkDB rethinkDBInstance = RethinkDB.r;
	protected static Logger logger = LoggerFactory.getLogger(DatabaseConnectionManager.class);

	private static Queue<Connection> connectionsPool;

	/**
	 * Creates a newInstance if current instance is null
	 * 
	 * @return
	 */
	public static DatabaseConnectionManager getInstance() {
		if (instance == null) {
			instance = new DatabaseConnectionManager();
			instance.initConnectionManager();
		}
		return instance;
	}

	/**
	 * private function for creation of new connections during startup
	 * 
	 * @return
	 */
	private Connection createConnection() {
		Connection conn = null;
		try {
			conn = rethinkDBInstance.connection().hostname(Constants.RETHINK_HOST).port(Constants.RETHINK_PORT)
					.connect();
		} catch (Exception e) {
			logger.error("ERROR: Unable to create a connection with the database");
		}

		return conn;
	}

	/**
	 * Creates a pool of connections of size present in Constants class
	 */
	private void initConnectionManager() {
		int connectionPoolSize = Constants.RETHINK_DB_CONNECTION_POOL_SIZE;
		connectionsPool = new LinkedList<Connection>();
		for (int index = 0; index < connectionPoolSize; index++) {
			Connection connection = createConnection();
			connectionsPool.add(connection);
		}
		logger.info("DatabaseConnectionManager: Successfully setup " + connectionPoolSize
				+ " rethinkDB database Connections");
	}

	/**
	 * returns a connection from the pool
	 * 
	 * @return
	 * @throws EmptyConnectionPoolException
	 */
	public static Connection getConnection() throws EmptyConnectionPoolException {
		if (connectionsPool == null || connectionsPool.size() == 0)
			throw new EmptyConnectionPoolException("Connection Pool Empty exception !!!");

		Connection connection = connectionsPool.poll();
		return connection;
	}

	/**
	 * release the connection to the connection pool
	 * 
	 * @param connection
	 */
	public static void releaseConnection(Connection connection) {
		connectionsPool.add(connection);
	}
}
