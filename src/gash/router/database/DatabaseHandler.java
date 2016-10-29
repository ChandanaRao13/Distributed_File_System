package gash.router.database;

import gash.router.database.datatypes.FluffyFile;
import gash.router.server.manage.exceptions.FileChunkNotFoundException;
import gash.router.server.manage.exceptions.FileNotFoundException;
import gash.router.util.Constants;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.rethinkdb.RethinkDB;
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
	 */
	@Deprecated
	public static boolean addFile(String filename, String line, int chunkId, int totalChunks) {
		Connection conn = getConnection();
		try {
			rethinkDBInstance
					.db(Constants.DATABASE)
					.table(Constants.TABLE)
					.insert(rethinkDBInstance
							.hashMap(Constants.FILE_NAME, filename)
							.with(Constants.FILE_CONTENT, line)
							.with(Constants.CHUNK_COUNT, totalChunks)
							.with(Constants.CHUNK_ID, chunkId)).run(conn);
			return true;
		} catch (Exception e) {
			logger.error("ERROR: Unable to store file in the database");
			System.out.println("File is not added");
			return false;
		}
	}

	/**
	 * adds the give line into the db
	 * 
	 * @param filename
	 * @param line
	 * @param chunkId
	 */
	@Deprecated
	public static boolean addFile(String filename, ByteString line, int chunkId, int totalChunks) {
		Connection conn = getConnection();
		try {
			rethinkDBInstance
					.db(Constants.DATABASE)
					.table(Constants.TABLE)
					.insert(rethinkDBInstance
							.hashMap(Constants.FILE_NAME, filename)
							.with(Constants.CHUNK_COUNT, totalChunks)
							.with(Constants.FILE_CONTENT, line)
							.with(Constants.CHUNK_ID, chunkId)).run(conn);
			System.out.println("File saved to DB: " + filename);
			return true;
		} catch (Exception e) {
			logger.error("ERROR: Unable to store file in the database");
			System.out.println("File is not added");
			return false;
		}
	}

	/**
	 * generic method to store the file as chunks in the form of byte[]
	 * 
	 * @param filename
	 * @param input
	 * @param chunkId
	 */
	public static boolean addFile(String filename, int chunkCount, byte[] input,
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
			return true;
		} catch (Exception e) {
			logger.debug("ERROR: Unable to store file in the database");
			System.out.println("File in not added");
			return false;
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
				.limit(1).getField(Constants.CHUNK_COUNT).run(conn);
		if (data == null)
			throw new FileNotFoundException(filename);
		else {
			
			ArrayList<String> result = data.bufferedItems();
			if(result.size() == 1) {
				return Integer.parseInt(String.valueOf(result.get(0)));
			} else {
				return 0;
			}
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
	public static List<FluffyFile> getFileContents(String filename) throws IOException, ParseException, FileNotFoundException {
		Connection connection = getConnection();
		Cursor<String> dataFromDB = rethinkDBInstance.db(Constants.DATABASE)
				.table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap("filename", filename))
				.run(connection);
		if (dataFromDB == null)
			throw new FileNotFoundException(filename);
		else {
			List<FluffyFile> fileContents = new ArrayList<FluffyFile>();
			for (Object record : dataFromDB) {
				JSONObject fileContentJSON = (JSONObject) jsonParse.parse(new StringReader((String) record));
				FluffyFile fluffyFile = new FluffyFile();
				fluffyFile.setChunkId(Integer.parseInt((String) fileContentJSON.get(Constants.CHUNK_ID)));
				fluffyFile.setFile((byte[]) fileContentJSON.get(Constants.FILE_CONTENT));
				fluffyFile.setFilename((String) fileContentJSON.get(Constants.FILE_NAME));
				fluffyFile.setTotalChunks(Integer.parseInt((String) fileContentJSON.get(Constants.CHUNK_COUNT)));
				fileContents.add(fluffyFile);
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
	public static List<FluffyFile> getFileContentWithChunkId(String filename, int chunkId) throws IOException, ParseException, FileNotFoundException {
		Connection connection = getConnection();

		Cursor<String> dataFromDB = rethinkDBInstance.db(Constants.DATABASE)
				.table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap("filename", filename).with(Constants.CHUNK_ID, chunkId))
				.run(connection);

		if (dataFromDB == null)
			throw new FileNotFoundException(filename);
		else {
			List<FluffyFile> fileContents = new ArrayList<FluffyFile>();
			for (Object record : dataFromDB) {
				JSONObject fileContentJSON = (JSONObject) jsonParse.parse(new StringReader((String) record));
				FluffyFile fluffyFile = new FluffyFile();
				fluffyFile.setChunkId(Integer.parseInt((String) fileContentJSON.get(Constants.CHUNK_ID)));
				fluffyFile.setFile((byte[]) fileContentJSON.get(Constants.FILE_CONTENT));
				fluffyFile.setFilename((String) fileContentJSON.get(Constants.FILE_NAME));
				fluffyFile.setTotalChunks(Integer.parseInt((String) fileContentJSON.get(Constants.CHUNK_COUNT)));
				fileContents.add(fluffyFile);
			}
			return fileContents;
		}
	}
	
	
	/**
	 * reads fileContent from given file and chunk id 
	 * @param filename
	 * @param chunkId
	 * @return
	 * @throws IOException
	 * @throws ParseException
	 * @throws FileNotFoundException
	 * @throws FileChunkNotFoundException 
	 */
	public static String getFileChunkContentWithChunkId(String filename, int chunkId) throws IOException, ParseException, FileNotFoundException, FileChunkNotFoundException {
		Connection connection = getConnection();

		Cursor<String> dataFromDB = rethinkDBInstance.db(Constants.DATABASE)
				.table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap(Constants.FILE_NAME, filename).with(Constants.CHUNK_ID, chunkId))
				.getField(Constants.FILE_CONTENT)
				.run(connection);

		if (dataFromDB == null)
			throw new FileChunkNotFoundException(filename, chunkId);
		else {
			ArrayList<String> result = dataFromDB.bufferedItems();
			if(result.size() == 1) {
				System.out.println("Data: " + result.get(0));
				return result.get(0);
			} else {
				return "";
			}
			
		}
	}
}