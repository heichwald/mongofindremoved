package parser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

class Utils {
	static byte[] fromBytestobytes(List<Byte> entry) {
		byte[] res = new byte[entry.size()];
		int i = 0;
		for (Byte e : entry) {
			res[i++] = (byte) e;
		}
		return res;
	}

	static String toHexString(byte[] array) {
		return DatatypeConverter.printHexBinary(array);
	}

	static byte[] toByteArray(String s) {
		return DatatypeConverter.parseHexBinary(s.replaceAll(" ", ""));
	}

	static void printbyteArray(byte[] a) {
		for (Object o : a) {
			System.out.print(o);
			System.out.print(",");
		}
		System.out.println();
	}
}

public class ParseBinary {

	static MongoClient client;
	static DB db;
	static DBCollection images;
	static DBCollection files;
	static Logger infoLogger;

	private static boolean buildBsonANdInsertToMongoRecovery(List<Byte> bytes) {
		BSONObject bsonRes = new BasicBSONDecoder().readObject(Utils.fromBytestobytes(bytes));
		// The binary file has all your collections for one db, so you may use a field name
		// of your doc to identify the collection the doc belongs to
		// Depends on your doc schema
		boolean success = false;
		if (bsonRes.containsField("dirname")) {
			try {
				files.insert(new BasicDBObject(bsonRes.toMap()).append("recovery", true));
				success = true;
			} catch (Exception eMongo) {
				//Usually error when duplicates
				//E11000 is for duplicates error,when insert remove if statement if you want to log this error as well
				if(!eMongo.getMessage().contains("E11000")) infoLogger.warning(eMongo.getMessage());
			}
		} else {
			try {
				images.insert(new BasicDBObject(bsonRes.toMap()).append("recovery", true));
				success = true;
			} catch (Exception eMongo) {
				if(!eMongo.getMessage().contains("E11000")) infoLogger.warning(eMongo.getMessage());
			}
		}
		return success;
	}

	private static void buildSizeOfMongoDoc(List<Byte> doc) {
		int finalLength = doc.size();
		// First 4 bytes are for the size of the doc
		// If size in bytes is 200, the four bytes will be 200 0 0 0
		// If size is > 256, ex: 400 the four bytes will be 144 1 0 0 (144+256
		// *1)
		int a = finalLength / 256;
		int b = finalLength - a * 256;
		doc.set(1, (byte) 0);
		if (a != 0) {
			doc.set(1, (byte) a);
		}
		doc.set(0, (byte) b);
	}

	private static void handleOneDBBinaryFile(String file) throws IOException {
		int docsCount = 0;
		int mongoSuccess = 0;
		try {
			Path inPath = Paths.get(file);
			byte[] data = Files.readAllBytes(inPath);
			// We store the last 4 bytes we have read in a found mongodoc
			LinkedList<Byte> last4bytes = new LinkedList<>();

			/*
			 * Start is _id (07 5F 69 64)
			 */
			List<Byte> start = new ArrayList<>();
			start.add((byte) 7);
			start.add((byte) 95);
			start.add((byte) 105);
			start.add((byte) 100);

			/*
			 * Next removed/deleted doc starts with EE EE EE EE
			 */
			List<Byte> end = new ArrayList<>();
			end.add((byte) -18);
			end.add((byte) -18);
			end.add((byte) -18);
			end.add((byte) -18);

			List<Byte> newMongoDocBytes = null;
			for (int i = 0; i < data.length; i++) {

				if (last4bytes.size() >= 4) {
					// We keep only 4 bytes
					last4bytes.remove();
				}
				last4bytes.add(data[i]);

				if (newMongoDocBytes != null) {
					// we are currently building a found doc (start has been found before as newMongoDocBytes != null)
					newMongoDocBytes.add(data[i]);
				}

				if (last4bytes.equals(start)) {
					boolean ok = true;
					for (int j = 0; j < 4; j++) {
						// -18 == 0xEE
						// previous bytes have to be EE EE EE EE
						if (data[i + j - 7] != (byte) -18) {
							ok = false;
						}
					}
					if (ok) {
						// We have found a new mongo doc
						newMongoDocBytes = new ArrayList<>(last4bytes);
					}

				} else if (newMongoDocBytes != null && last4bytes.equals(end)) {
					// We have found the next occurence of EE EE EE EE
					// We go back just before EE EE EE EE (possible end of the current
					// doc)
					for (int j = 0; j < 4; j++) {
						newMongoDocBytes.remove(newMongoDocBytes.size() - 1);
					}
					int prepnb = 4;
					// Prefix of 4 bytes to store the size of the doc
					for (int j = 0; j < prepnb; j++) {
						newMongoDocBytes.add(0, (byte) 0);
					}
					buildSizeOfMongoDoc(newMongoDocBytes);
					try {
						if (buildBsonANdInsertToMongoRecovery(newMongoDocBytes))
							mongoSuccess++;
						docsCount++;
					} catch (java.lang.IllegalArgumentException e) {
						// Usually stopping just before EE EE EE EE is not the
						// actual size of the doc but bigger.
						// Because we have specified a larger size in the
						// prefix, the bson parser will give us an exception and
						// the message is helpful
						// bad data. lengths don't match read:361 != len:396
						// We parse the error message to get the actual size of
						// the doc, remove the bytes in excess at the end of the
						// doc and retry to read it.
 						try {
							int toRemove = newMongoDocBytes.size() - Integer.parseInt((e.getMessage().substring(36, e.getMessage().indexOf("!")).trim()));
							i = i - toRemove;
							for (int j = 0; j < toRemove; j++) {
								newMongoDocBytes.remove(newMongoDocBytes.size() - 1);
							}
							buildSizeOfMongoDoc(newMongoDocBytes);
							if (buildBsonANdInsertToMongoRecovery(newMongoDocBytes))
								mongoSuccess++;
							docsCount++;
						} catch (Exception e1) {
							//If we found a doc that should not happen
							infoLogger.severe(e1.getMessage());
						}
					} finally {
						// The mongo doc is done, we will instantiate a new
						// newMongoDocBytes when we find a new start occurence
						newMongoDocBytes = null;
					}
				}
			}
		} finally {
			infoLogger.info("Mongo success found (can be less than total count because of duplicates _id) for file: " + file + " is " + mongoSuccess);
			infoLogger.info("Non unique docs found for file: " + file + " is " + docsCount);
		}
	}

	public static void main(String[] args) throws IOException {
		infoLogger = Logger.getLogger(ParseBinary.class.toString());
		infoLogger.setLevel(Level.INFO);
		//Max 10 Mb per log file
		infoLogger.addHandler(new FileHandler("results.log", 10*1024*1024, 100));

		client = new MongoClient("localhost");
		db = client.getDB("yourdb");
		images = db.getCollection("yourcoll");
		files = db.getCollection("files");
		// Create indexes in recovery collection, adapt it
		images.ensureIndex(new BasicDBObject("path", 1), "path_index", true);
		files.ensureIndex(new BasicDBObject("dirname", 1).append("basename", 1), "files_index", true);
		// Mongo creates several files for one db, in this case would be
		// assets.0, assets.1 ...
		String[] files = new String[] { "/Users/heichwald/db/assets.0", "/Users/heichwald/db/assets.1", "/Users/heichwald/db/assets.2" };
		for (String file : files) {
			handleOneDBBinaryFile(file);
		}
	}
}
