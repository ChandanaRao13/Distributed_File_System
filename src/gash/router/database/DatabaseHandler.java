package gash.router.database;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

import gash.router.database.datatypes.FluffyFile;
import gash.router.server.manage.exceptions.EmptyConnectionPoolException;
import gash.router.server.manage.exceptions.FileChunkNotFoundException;
import gash.router.server.manage.exceptions.FileNotFoundException;
import gash.router.util.Constants;

public class DatabaseHandler {

	private static DatabaseConnectionManager databaseConnectionManager = DatabaseConnectionManager.getInstance();

	public static final RethinkDB rethinkDBInstance = RethinkDB.r;
	public static final JSONParser jsonParse = new JSONParser();
	protected static Logger logger = LoggerFactory.getLogger(DatabaseHandler.class);

	/**
	 * adds the give line into the db
	 * 
	 * @param filename
	 * @param line
	 * @param chunkId
	 * @throws EmptyConnectionPoolException 
	 * @throws Exception
	 */
	@Deprecated
	public static boolean addFile(String filename, String line, int chunkId, int totalChunks) throws EmptyConnectionPoolException {
		Connection conn = databaseConnectionManager.getConnection();
		try {
			rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
					.insert(rethinkDBInstance.hashMap(Constants.FILE_NAME, filename).with(Constants.FILE_CONTENT, line)
							.with(Constants.CHUNK_COUNT, totalChunks).with(Constants.CHUNK_ID, chunkId))
					.run(conn);
			return true;
		} catch (Exception e) {
			logger.error("ERROR: Unable to store file in the database");
			System.out.println("File is not added");
			return false;
		} finally {
			databaseConnectionManager.releaseConnection(conn);
		}
	}

	/**
	 * adds the give line into the db
	 * 
	 * @param filename
	 * @param line
	 * @param chunkId
	 * @throws EmptyConnectionPoolException 
	 * @throws Exception
	 */
	@Deprecated
	public static boolean addFile(String filename, ByteString line, int chunkId, int totalChunks) throws EmptyConnectionPoolException {
		Connection conn = databaseConnectionManager.getConnection();
		try {
			String contentString = new String(line.toByteArray());
			rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
					.insert(rethinkDBInstance.hashMap(Constants.FILE_NAME, filename)
							.with(Constants.CHUNK_COUNT, totalChunks).with(Constants.FILE_CONTENT, contentString)
							.with(Constants.CHUNK_ID, chunkId))
					.run(conn);
			System.out.println("File saved to DB: " + filename);
			return true;
		} catch (Exception e) {
			logger.error("ERROR: Unable to store file in the database");
			System.out.println("File is not added");
			return false;
		} finally {
			databaseConnectionManager.releaseConnection(conn);
		}
	}

	/**
	 * generic method to store the file as chunks in the form of byte[]
	 * 
	 * @param filename
	 * @param input
	 * @param chunkId
	 * @throws EmptyConnectionPoolException 
	 * @throws Exception
	 */
	public static boolean addFile(String filename, int chunkCount, byte[] input, int chunkId) throws EmptyConnectionPoolException {
		Connection connection = databaseConnectionManager.getConnection();
		try {
			// String contentString = new String(input);
			rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE).insert(rethinkDBInstance
					.hashMap(Constants.FILE_NAME, filename).with(Constants.CHUNK_COUNT, chunkCount)
					.with(Constants.FILE_CONTENT, rethinkDBInstance.binary(input)).with(Constants.CHUNK_ID, chunkId))
					.run(connection);
			return true;
		} catch (Exception e) {
			logger.debug("ERROR: Unable to store file in the database");
			System.out.println("File in not added");
			e.printStackTrace();
			return false;
		} finally {
			databaseConnectionManager.releaseConnection(connection);
		}
	}

	/**
	 * 
	 * @param filename
	 * @return
	 * @throws FileNotFoundException 
	 * @throws EmptyConnectionPoolException 
	 * @throws Exception
	 */
	public static int getFilesChunkCount(String filename) throws FileNotFoundException, EmptyConnectionPoolException {
		Connection conn = databaseConnectionManager.getConnection();
		Cursor<String> data = rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap("filename", filename)).limit(1).getField(Constants.CHUNK_COUNT)
				.run(conn);
		if (data == null)
			throw new FileNotFoundException(filename);
		else {
			databaseConnectionManager.releaseConnection(conn);
			ArrayList<String> result = data.bufferedItems();
			if (result.size() == 1) {
				return Integer.parseInt(String.valueOf(result.get(0)));
			} else {
				return 0;
			}
		}
	}

	/**
	 * 
	 * reads the file with name @param filename from database and returns all
	 * the chunks
	 * 
	 * @param filename
	 * @return
	 * @throws FileNotFoundException
	 * @throws ParseException
	 * @throws IOException
	 * @throws EmptyConnectionPoolException
	 * @throws Exception
	 */
	public static List<FluffyFile> getFileContents(String filename)
			throws FileNotFoundException, IOException, ParseException, EmptyConnectionPoolException {
		Connection connection = databaseConnectionManager.getConnection();
		Cursor<String> dataFromDB = rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap("filename", filename)).run(connection);
		databaseConnectionManager.releaseConnection(connection);
		if (dataFromDB == null)
			throw new FileNotFoundException(filename);
		else {
			List<FluffyFile> fileContents = new ArrayList<FluffyFile>();
			for (Object record : dataFromDB) {
				FluffyFile fluffyFile = new FluffyFile();
				HashMap<String, Object> fileContentMap = (HashMap<String, Object>) record;
				fluffyFile.setChunkId(new Integer(fileContentMap.get(Constants.CHUNK_ID).toString()));
				fluffyFile.setFile((byte[]) fileContentMap.get(Constants.FILE_CONTENT));
				fluffyFile.setFilename(fileContentMap.get(Constants.FILE_NAME).toString());
				fluffyFile.setTotalChunks(new Integer(fileContentMap.get(Constants.CHUNK_COUNT).toString()));
				fileContents.add(fluffyFile);
			}
			return fileContents;
		}
	}

	/**
	 * reads the file with specific fileContent from database
	 * 
	 * @param filename
	 * @param chunkId
	 * @return
	 * @throws FileNotFoundException
	 * @throws ParseException
	 * @throws IOException
	 * @throws EmptyConnectionPoolException
	 * @throws Exception
	 */
	public static List<FluffyFile> getFileContentWithChunkId(String filename, int chunkId)
			throws FileNotFoundException, IOException, ParseException, EmptyConnectionPoolException {
		Connection connection = databaseConnectionManager.getConnection();

		Cursor<String> dataFromDB = rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap("filename", filename).with(Constants.CHUNK_ID, chunkId))
				.run(connection);

		databaseConnectionManager.releaseConnection(connection);
		if (dataFromDB == null)
			throw new FileNotFoundException(filename);
		else {
			List<FluffyFile> fileContents = new ArrayList<FluffyFile>();
			for (Object record : dataFromDB) {
				FluffyFile fluffyFile = new FluffyFile();
				HashMap<String, Object> fileContentMap = (HashMap<String, Object>) record;
				fluffyFile.setChunkId(new Integer(fileContentMap.get(Constants.CHUNK_ID).toString()));
				fluffyFile.setFile((byte[]) fileContentMap.get(Constants.FILE_CONTENT));
				fluffyFile.setFilename(fileContentMap.get(Constants.FILE_NAME).toString());
				fluffyFile.setTotalChunks(new Integer(fileContentMap.get(Constants.CHUNK_COUNT).toString()));
				fileContents.add(fluffyFile);
			}
			return fileContents;
		}
	}

	/**
	 * reads fileContent from given file and chunk id
	 * 
	 * @param filename
	 * @param chunkId
	 * @return
	 * @throws IOException
	 * @throws ParseException
	 * @throws FileNotFoundException
	 * @throws FileChunkNotFoundException
	 * @throws EmptyConnectionPoolException
	 */
	public static ByteString getFileChunkContentWithChunkId(String filename, int chunkId) throws IOException,
			ParseException, FileNotFoundException, FileChunkNotFoundException, EmptyConnectionPoolException {
		Connection connection = databaseConnectionManager.getConnection();

		Cursor<String> dataFromDB = rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap(Constants.FILE_NAME, filename).with(Constants.CHUNK_ID, chunkId))
				// .getField(Constants.FILE_CONTENT)
				.run(connection);

		databaseConnectionManager.releaseConnection(connection);
		if (dataFromDB == null)
			throw new FileChunkNotFoundException(filename, chunkId);
		else {
			for (Object record : dataFromDB) {
				System.out.println("record");
				System.out.println(record);
				HashMap<String, Object> fileContentMap = (HashMap<String, Object>) record;
				System.out.println(fileContentMap.get(Constants.FILE_CONTENT).getClass().getName());
				return ByteString.copyFrom((byte[]) fileContentMap.get(Constants.FILE_CONTENT));
			}
			return ByteString.copyFrom("".getBytes());
		}
	}
	
	
	
	public static boolean isFileAvailable(String filename) throws EmptyConnectionPoolException{
		Connection connection = databaseConnectionManager.getConnection();

		Cursor<String> dataFromDB = rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
				.filter(rethinkDBInstance.hashMap(Constants.FILE_NAME, filename)).run(connection);
		databaseConnectionManager.releaseConnection(connection);
		if (dataFromDB == null)
			System.out.println("Database error");
		else{
			int count  = 0;
			for (Object record : dataFromDB) {
				count ++;
			}
			
			if(count != 0){
				return true;
			}
		}
		return false;	
	}
	
	/**
	 * generic method to delete the file from the database
	 * 
	 * @param filename
	 * @param input
	 * @param chunkId
	 * @throws EmptyConnectionPoolException 
	 * @throws Exception
	 */
	public static boolean deleteFile(String filename) throws EmptyConnectionPoolException {
		Connection connection = databaseConnectionManager.getConnection();
		try {
			HashMap<String, Object> dataFromDB = rethinkDBInstance.db(Constants.DATABASE).table(Constants.TABLE)
					.filter(rethinkDBInstance.hashMap(Constants.FILE_NAME, filename)).delete().run(connection);
			databaseConnectionManager.releaseConnection(connection);
			return true;
		} catch (Exception e) {
			logger.debug("ERROR: Unable to delete file in the database");
			System.out.println("File in not deleted");
			e.printStackTrace();
			return false;
		} finally {
			databaseConnectionManager.releaseConnection(connection);
		}
	}
}