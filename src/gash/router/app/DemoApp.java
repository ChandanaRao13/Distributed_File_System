/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.app;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.google.protobuf.ByteString;

import gash.router.client.CommConnection;
import gash.router.client.CommListener;
import gash.router.client.MessageClient;
import gash.router.database.DatabaseHandler;
import routing.Pipe.CommandMessage;

public class DemoApp implements CommListener {
	private MessageClient mc;

	public DemoApp(MessageClient mc) {
		init(mc);
	}

	private void init(MessageClient mc) {
		this.mc = mc;
		this.mc.addListener(this);
	}

	private void ping(int N) {
		// test round-trip overhead (note overhead for initial connection)
		final int maxN = 10;
		long[] dt = new long[N];
		long st = System.currentTimeMillis(), ft = 0;
		for (int n = 0; n < N; n++) {
			mc.ping();
			ft = System.currentTimeMillis();
			dt[n] = ft - st;
			st = ft;
		}

		System.out.println("Round-trip ping times (msec)");
		for (int n = 0; n < N; n++)
			System.out.print(dt[n] + " ");
		System.out.println("");
	}

	@Override
	public String getListenerID() {
		return "demo";
	}

	@Override
	public void onMessage(CommandMessage msg) {
		System.out.println("---> " + msg);	
	}

	/**
	 * sample application (client) use of our messaging service
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		String host = "127.0.0.1";
		int port = 4568;

		try {
			MessageClient mc = new MessageClient(host, port);
			DemoApp da = new DemoApp(mc);

			// do stuff w/ the connection
			// da.ping(2);
			da.chunkFile(args[0]);
			//da.sendFileAsChunks(new File(args[0]));
			System.out.println("\n** exiting in 10 seconds. **");
			System.out.flush();
			Thread.sleep(10 * 1000);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			CommConnection.getInstance().release();
		}
	}

	@Deprecated
	private void chunkFile(String file) throws IOException {
		String line = null;
		try {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(
					file));

			int chunkId = 0;
			while ((line = bufferedReader.readLine()) != null) {
				// databaseHandler.addFile(file, line, chunkId);
				mc.sendFileChunks(file, line, chunkId);
				chunkId++;
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private void sendFileAsChunks(File file) {
		ArrayList<ByteString> chunkedFile = new ArrayList<ByteString>();

		int sizeOfChunk = 10 * 10;
		int numOfChunks = 0;
		byte[] buffer = new byte[sizeOfChunk];

		try {
			BufferedInputStream bis = new BufferedInputStream(
					new FileInputStream(file));
			String name = file.getName();

			int tmp = 0;
			while ((tmp = bis.read(buffer)) > 0) {
				try {
					ByteString bs = ByteString.copyFrom(buffer, 0, tmp);
					chunkedFile.add(bs);
					numOfChunks++;
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}

			for (int index = 0; index < chunkedFile.size(); index++) {
				System.out.println(chunkedFile.get(index));
				mc.sendFile(chunkedFile.get(index), name, numOfChunks,
						index + 1); 
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		System.out.println(chunkedFile.size());
	}
}
