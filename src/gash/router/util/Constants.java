
package gash.router.util;

public class Constants {
	
	//DB constants
	public static final String RETHINK_HOST = "localhost";
	public static final int RETHINK_PORT = 28015;
	
	public static final String DATABASE = "Fluffy";
	public static final String TABLE = "Files";
	public static final String FILE_NAME = "filename";
	public static final String CHUNK_COUNT = "totalChunks";
	public static final String FILE_CONTENT = "content";
	public static final String NO_OF_CHUNKS = "noOfChuncks";
	public static final String CHUNK_ID = "chunckId";
	
	public static final Integer MaxLoadCount = 1000;

	public static final Integer RETHINK_DB_CONNECTION_POOL_SIZE = 100;
}