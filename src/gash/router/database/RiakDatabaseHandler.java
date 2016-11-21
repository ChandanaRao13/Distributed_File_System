package gash.router.database;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;

import gash.router.database.datatypes.FluffyFile;
import gash.router.database.datatypes.FluffyFileList;
import gash.router.util.Constants;

public class RiakDatabaseHandler {
	protected static Logger logger = LoggerFactory.getLogger("RiakDatabase");
	public static RiakDatabaseHandler riakDatabase = null;
	private RiakCluster riakCluster = null;
	private RiakNode riakNode = null;
	private RiakClient riakClient = null;

	private RiakDatabaseHandler() {
		try {
			riakNode = new RiakNode.Builder().withRemoteAddress(Constants.RIAK_HOST).build();
			riakCluster = new RiakCluster.Builder(riakNode).build();
			riakCluster.start();
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug("Error Unable to start Riak");
		}
	}

	public static RiakDatabaseHandler getRiakInstance() {
		if (riakDatabase == null) {
			return new RiakDatabaseHandler();
		}
		return riakDatabase;
	}

	/**
	 * store the file with chunkcount < 4
	 * 
	 * @param filename
	 * @param chunkCount
	 * @param input
	 * @param chunkId
	 */
	public synchronized void storeFile(String filename, int chunkCount, byte[] input, int chunkId) {
		try {
			riakClient = new RiakClient(riakCluster);
			Namespace databaseRiak = new Namespace(Constants.DATABASE_RIAK);
			boolean exist = false;
			Location fileLocation = new Location(databaseRiak, filename);
			FetchValue getFile = new FetchValue.Builder(fileLocation).build();
			FetchValue.Response response = riakClient.execute(getFile);
			FluffyFileList outputFile = response.getValue(FluffyFileList.class);
			if (outputFile != null) {
				int i = 0;
				for (; i < outputFile.getFiles().size(); i++) {
					if (outputFile.getFiles().get(i).getChunkId() == chunkId) {
						exist = true;
						break;
					}
				}
				if (exist) {
					outputFile.getFiles().get(i).setFilename(filename);
					outputFile.getFiles().get(i).setTotalChunks(chunkCount);
					outputFile.getFiles().get(i).setFile(input);
					outputFile.getFiles().get(i).setChunkId(chunkId);

				} else {
					FluffyFile fluffyFile = new FluffyFile();
					fluffyFile.setFilename(filename);
					fluffyFile.setTotalChunks(chunkCount);
					fluffyFile.setFile(input);
					fluffyFile.setChunkId(chunkId);
					outputFile.getFiles().add(fluffyFile);
				}
			} else {
				FluffyFile fluffyFile = new FluffyFile();
				fluffyFile.setFilename(filename);
				fluffyFile.setTotalChunks(chunkCount);
				fluffyFile.setFile(input);
				fluffyFile.setChunkId(chunkId);
				outputFile = new FluffyFileList();
				outputFile.getFiles().add(fluffyFile);
			}
			StoreValue storeFile = new StoreValue.Builder(outputFile).withLocation(fileLocation).build();
			riakClient.execute(storeFile);
			riakCluster.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug("Error: Unable to store the file in the riak database: " + e.getMessage());
			System.out.println("Error: Unable to store the file in the riak database: " + e.getMessage());
		}
	}

	/**
	 * retrieve the file if present
	 * @param filename
	 * @return
	 */
	public synchronized List<FluffyFile> getFile(String filename) {
		try {
			riakClient = new RiakClient(riakCluster);
			Namespace databaseRiak = new Namespace(Constants.DATABASE_RIAK);
			Location fileLocation = new Location(databaseRiak, filename);
			FetchValue getFile = new FetchValue.Builder(fileLocation).build();
			FetchValue.Response response = riakClient.execute(getFile);
			FluffyFileList outputFile = response.getValue(FluffyFileList.class);
			riakCluster.shutdown();
			if (outputFile != null)
				return outputFile.getFiles();
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug("Error :Unable to retrieve the file stored in the riak database: " + e.getMessage());
			System.out.println("Error :Unable to retrieve the file stored in the riak database: " + e.getMessage());
		}
		return null;
	}

	/**
	 * get the chunk count of the file
	 * @param filename
	 * @return
	 */
	public synchronized int getChunkCount(String filename) {
		try {
			riakClient = new RiakClient(riakCluster);
			Namespace databaseRiak = new Namespace(Constants.DATABASE_RIAK);
			Location fileLocation = new Location(databaseRiak, filename);
			FetchValue getFile = new FetchValue.Builder(fileLocation).build();
			FetchValue.Response response = riakClient.execute(getFile);
			FluffyFileList outputFile = response.getValue(FluffyFileList.class);
			riakCluster.shutdown();
			if (outputFile != null)
				return outputFile.getFiles().size();
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug("Error: Unable to retrieve the file stored in the riak database: " + e.getMessage());
			System.out.println("Error: Unable to retrieve the file stored in the riak database: " + e.getMessage());
		}
		return 0;
	}

	/**
	 * retrieve the file if present
	 * @param filename
	 * @param chunkId
	 * @return
	 */
	public synchronized FluffyFile getFileWithChunkID(String filename, int chunkId) {
		try {
			riakClient = new RiakClient(riakCluster);
			Namespace databaseRiak = new Namespace(Constants.DATABASE_RIAK);
			Location fileLocation = new Location(databaseRiak, filename);
			FetchValue getFile = new FetchValue.Builder(fileLocation).build();
			FetchValue.Response response = riakClient.execute(getFile);
			FluffyFileList outputFile = response.getValue(FluffyFileList.class);
			riakCluster.shutdown();
			if (outputFile != null) {
				for (int i = 0; i < outputFile.getFiles().size(); i++) {
					if (outputFile.getFiles().get(i).getChunkId() == chunkId) {
						return outputFile.getFiles().get(i);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug("Error while fetching file");
			System.out.println("Unable to retrieve the file stored in the riak database");
		}
		return null;
	}

	/**
	 * retrieve the file if present
	 * @param filename
	 * @param chunkId
	 * @return
	 */
	public synchronized FluffyFile getChunk(String filename, int chunkId) {
		try {
			riakClient = new RiakClient(riakCluster);
			Namespace databaseRiak = new Namespace(Constants.DATABASE_RIAK);
			Location fileLocation = new Location(databaseRiak, filename);
			FetchValue getFile = new FetchValue.Builder(fileLocation).build();
			FetchValue.Response response = riakClient.execute(getFile);
			FluffyFileList outputFile = response.getValue(FluffyFileList.class);
			riakCluster.shutdown();
			if (outputFile != null) {
				for (int i = 0; i < outputFile.getFiles().size(); i++) {
					if (outputFile.getFiles().get(i).getChunkId() == chunkId)
						return outputFile.getFiles().get(i);
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug("Error while fetching file");
			System.out.println("Unable to retrieve the file stored in the riak database");
		}
		return null;
	}

	/**
	 * retrieve all files
	 * @return
	 */
	public synchronized List<FluffyFile> getAllFiles() {
		try {
			riakClient = new RiakClient(riakCluster);
			Namespace databaseRiak = new Namespace(Constants.DATABASE_RIAK);
			List<FluffyFile> allFiles = new ArrayList<>();
			URL getFileUrl = new URL("http://localhost:8098/buckets/FluffyRiakDatabase/keys?keys=true");
			BufferedReader in = new BufferedReader(new InputStreamReader(getFileUrl.openStream()));
			StringBuilder sb = new StringBuilder();
			int cp;
			while ((cp = in.read()) != -1) {
				sb.append((char) cp);
			}
			String output = sb.toString();
			JSONObject outputJsonFormat = new JSONObject(output);
			JSONArray outputJsonArray = outputJsonFormat.getJSONArray("keys");
			Location fileLocation = null;
			FetchValue getFile = null;

			for (int i = 0; i < outputJsonArray.length(); i++) {
				fileLocation = new Location(databaseRiak, outputJsonArray.get(i).toString());
				getFile = new FetchValue.Builder(fileLocation).build();
				FetchValue.Response response = riakClient.execute(getFile);
				FluffyFileList outputFile = response.getValue(FluffyFileList.class);
				allFiles.addAll(outputFile.getFiles());
			}
			riakCluster.shutdown();
			if (allFiles.size() > 0)
				return allFiles;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * delete the file from database
	 * @param filename
	 */
	public synchronized void deleteFile(String filename) {
		try {
			riakClient = new RiakClient(riakCluster);
			Namespace databaseRiak = new Namespace(Constants.DATABASE_RIAK);
			Location fileLocation = new Location(databaseRiak, filename);
			DeleteValue dv = new DeleteValue.Builder(fileLocation).build();
			riakClient.execute(dv);
			riakCluster.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug("Error while deleting file");
			System.out.println("Unable to delete the file stored in the riak database");
		}
	}

	/**
	 * 
	 * @param filename
	 * @param chunkCount
	 * @param input
	 * @param chunkId
	 */
	@Deprecated
	public void storeFileOneChunk(String filename, int chunkCount, byte[] input, int chunkId) {
		try {
			riakClient = new RiakClient(riakCluster);
			Namespace databaseRiak = new Namespace("Sample");
			Location fileLocation = new Location(databaseRiak, filename);
			FluffyFile fluffyFile = new FluffyFile();
			fluffyFile.setFilename(filename);
			fluffyFile.setTotalChunks(chunkCount);
			fluffyFile.setFile(input);
			fluffyFile.setChunkId(chunkId);
			StoreValue storeFile = new StoreValue.Builder(fluffyFile).withLocation(fileLocation).build();
			riakClient.execute(storeFile);
			riakCluster.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug("Error while storing file");
			System.out.println("Unable to store the file in the riak database");
		}
	}

	/**
	 * 
	 * @param filename
	 */
	@Deprecated
	public void getFileOneChunk(String filename) {
		try {
			riakClient = new RiakClient(riakCluster);
			Namespace databaseRiak = new Namespace("Sample");
			Location fileLocation = new Location(databaseRiak, filename);
			FetchValue getFile = new FetchValue.Builder(fileLocation).build();
			FetchValue.Response response = riakClient.execute(getFile);
			response.getValue(FluffyFile.class);
			riakCluster.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
			logger.debug("Error while fetching file");
			System.out.println("Unable to retrieve the file stored in the riak database");
		}
	}
}
