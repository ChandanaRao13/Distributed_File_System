package gash.router.database;

import java.io.IOException;
import java.io.StringReader;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.manage.exceptions.FileNotFoundException;
import gash.router.util.Constants;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.Limit;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

public class DatabaseHandler {
	public static final RethinkDB rethinkDBInstance = RethinkDB.r;
	public static final JSONParser jsonParse = new JSONParser();
	protected static Logger logger = LoggerFactory
			.getLogger(DatabaseHandler.class);

	/**
	 * creates a new connection and returns the connection
	 * 
	 * @return
	 */
	public static Connection getConnection() {
		Connection conn = null;
		try {
			conn = rethinkDBInstance.connection()
					.hostname(Constants.RETHINK_HOST)
					.port(Constants.RETHINK_PORT).connect();
			System.out.println("Connection Established");
		} catch (Exception e) {
			logger.error("ERROR: Unable to create a connection with the database");
			System.out.println("Could not establish database connection");
		}
		return conn;
	}

	/**
	 * adds the give line into the db
	 * 
	 * @param filename
	 * @param line
	 * @param chunkId
	 * @return 
	 */
	@Deprecated
	public static boolean addFile(String filename, String line, int chunkId) {
		Connection conn = getConnection();
		try {
			rethinkDBInstance
					.db(Constants.DATABASE)
					.table(Constants.TABLE)
					.insert(rethinkDBInstance
							.hashMap(Constants.FILE_NAME, filename)
							.with(Constants.FILE_CONTENT, line)
							.with(Constants.CHUNK_ID, chunkId)).run(conn);
		} catch (Exception e) {
			logger.error("ERROR: Unable to store file in the database");
			System.out.println("File is not added");
			return false;
		}
		return true;
	}

	/**
	 * generic method to store the file as chunks in the form of byte[]
	 * 
	 * @param filename
	 * @param input
	 * @param chunkId
	 */
	public static void addFile(String filename, int chunkCount, byte[] input,
			int chunkId) {
		Connection connection = getConnection();
		try {
			rethinkDBInstance
					.db(Constants.DATABASE)
					.table(Constants.TABLE)
					.insert(rethinkDBInstance
							.hashMap(Constants.FILE_NAME, filename)
							.with(Constants.CHUNK_COUNT, chunkCount)
							.with(Constants.FILE_CONTENT, input)
							.with(Constants.CHUNK_ID, chunkId)).run(connection);
		} catch (Exception e) {
			logger.debug("ERROR: Unable to store file in the database");
			System.out.println("File in not added");
		}
	}

	/**
	 * 
	 * @param filename
	 * @return
	 * @throws FileNotFoundException
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public static int getFilesChunkCount(String filename)
			throws FileNotFoundException, IOException, ParseException {
		Connection conn = getConnection();
		Cursor<String> data = rethinkDBInstance.db(Constants.DATABASE)
				.table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap("filename", filename))
				.limit(1).run(conn);
		if (data == null)
			throw new FileNotFoundException(filename);
		else {
			for (Object change : data) {
				System.out.println(change);
				JSONObject fileContent = (JSONObject) jsonParse.parse(new StringReader((String) change));
				return Integer.parseInt((String) fileContent.get(Constants.CHUNK_COUNT));
			}
			return 0;
		}
	}

	/**
	 * 
	 * reads the file with name @param filename from database
	 * and returns all the chunks
	 * 
	 * @param filename
	 * @return
	 * @throws IOException
	 * @throws ParseException
	 * @throws FileNotFoundException
	 */
	@SuppressWarnings("unchecked")
	public static JSONArray getFileContents(String filename) throws IOException, ParseException, FileNotFoundException {
		Connection conn = getConnection();
		Cursor<String> data = rethinkDBInstance.db(Constants.DATABASE)
				.table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap("filename", filename))
				.run(conn);
		if (data == null)
			throw new FileNotFoundException(filename);
		else {
			JSONArray fileContents = new JSONArray();
			for (Object change : data) {
				System.out.println(change);
				JSONObject fileContent = (JSONObject) jsonParse.parse(new StringReader((String) change));
				fileContents.add(fileContent);
			}
			return fileContents;
		}
	}

	/**
	 * reads the file with specific fileContent from database 
	 * @param filename
	 * @param chunkId
	 * @return
	 * @throws IOException
	 * @throws ParseException
	 * @throws FileNotFoundException
	 */
	@SuppressWarnings("unchecked")
	public static JSONArray getFileContentWithChunkId(String filename, int chunkId) throws IOException, ParseException, FileNotFoundException {
		Connection conn = getConnection();
		Cursor<String> data = rethinkDBInstance.db(Constants.DATABASE)
				.table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap("filename", filename).with(Constants.CHUNK_ID, chunkId))
				.run(conn);
				//.with(Constants.CHUNK_ID, chunkId)
		if (data == null)
			throw new FileNotFoundException(filename);
		else {
			JSONArray fileContents = new JSONArray();
			for (Object change : data) {
				System.out.println(change);
				JSONObject fileContent = (JSONObject) jsonParse.parse(new StringReader((String) change));
				fileContents.add(fileContent);
			}
			return fileContents;
		}
	}
}