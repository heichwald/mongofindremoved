import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class ParseBinary {

	static byte[] fromBytetobyte(List<Byte> entry) {
		byte[] res = new byte[entry.size()];
		int i = 0;
		for (Byte e : entry) {
			res[i++] = (byte) e;
		}
		return res;
	}

	public static String toHexString(byte[] array) {
		return DatatypeConverter.printHexBinary(array);
	}

	public static byte[] toByteArray(String s) {
		return DatatypeConverter.parseHexBinary(s.replaceAll(" ", ""));
	}

	static void printArray(byte[] a) {
		for (Object o : a) {
			System.out.print(o);
			System.out.print(",");
		}
		System.out.println();
	}

	static boolean equalsBytes(List<Byte> l1, byte[] l2) {
		if (l1.size() != l2.length)
			return false;
		int i = 0;
		for (Byte l : l1) {
			if (!l.equals(l2[i++]))
				return false;
		}
		return true;
	}

	public static void main(String[] args) throws IOException {
		MongoClient client = new MongoClient("");
		DB db = client.getDB("recovery_assets");
		DBCollection images = db.getCollection("images");
		images.ensureIndex(new BasicDBObject("path", 1), "path_index", true);
		DBCollection files = db.getCollection("files");
		files.ensureIndex(new BasicDBObject("dirname", 1).append("basename", 1), "files_index", true);
		PrintWriter writer = null;
		try {
			Path inPath = Paths.get("/Users/heichwald/db/assets.2");
			//Path outPath = Paths.get("outtest.0");
			//writer = new PrintWriter("out");
			/*
			 * Look for the end of an image document Storage id is used for that
			 */
			LinkedList<Byte> storageIdDel = new LinkedList<>();
			storageIdDel.add(Byte.valueOf((byte) 105));
			storageIdDel.add(Byte.valueOf((byte) 100));
			storageIdDel.add(Byte.valueOf((byte) 0));
			storageIdDel.add(Byte.valueOf((byte) 41));
			byte[] data = Files.readAllBytes(inPath);
			LinkedList<Byte> last4bytes = new LinkedList<>();

			//ight
			List<Byte> ight = new ArrayList<>();
			ight.add((byte) 105);
			ight.add((byte) 103);
			ight.add((byte) 104);
			ight.add((byte) 116);			
			
			/*
			 * End is 00 00
			 */
			List<Byte> end =  new ArrayList<>();
			end.add((byte) -18);
			end.add((byte) -18);
			end.add((byte) -18);
			end.add((byte) -18);
			/*
			 * Start is _id (07 5F 69 64)
			 */
			List<Byte> start = new ArrayList<>();
			start.add((byte) 7);
			start.add((byte) 95);
			start.add((byte) 105);
			start.add((byte) 100);

			List<Byte> newImageBytes = null;
			int endIndex = -1;
			int count = 0;
			boolean copyrightFound  = false;
			for (int i = 0; i < data.length; i++) {
				if (last4bytes.size() >= 4) {
					last4bytes.remove();
				}
				last4bytes.add(data[i]);
				if (newImageBytes != null) {
					// we are building an image doc
					newImageBytes.add(data[i]);
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
						// We have found a new image
						newImageBytes = new ArrayList<>(last4bytes);
					}
				}
				else if(last4bytes.equals(ight)){
					copyrightFound = true;
				/*} else if (newImageBytes != null && last4bytes.subList(2,4).equals(end)) {
					//The end will be after the storage id value
					endIndex = i + 44;*/
				} else if (newImageBytes != null && last4bytes.equals(end)) {
					for(int j=0; j < 4 ; j++){
						newImageBytes.remove(newImageBytes.size() - 1);
					}
					int prepnb = 4;
					//int appnb = 2;
					//Prefix of 4 bytes to store the size of the doc
					for (int j = 0; j < prepnb; j++) {
						newImageBytes.add(0, (byte) 0);
					}
					//Suffix of two bytes of 0 to close correctly the doc
					/*for (int j = 0; j < appnb; j++) {
						newImageBytes.add((byte) 0);
					}*/
					int finalLength = newImageBytes.size();
					//System.out.println(finalLength);
					//First 4 bytes are for the size of the doc
					//If size in bytes is 200, the four bytes will be 200 0 0 0
					//If size is > 256, ex: 400 the four bytes will be 144 1 0 0 (144+256 *1)
					int a = finalLength / 256;
					int b = finalLength - a * 256;

					if (a != 0) {
						newImageBytes.set(1, (byte) a);
					}
					newImageBytes.set(0, (byte) b);
					//Files.write(outPath, fromBytetobyte(newImageBytes));
					try {
						BSONObject bsonRes = new BasicBSONDecoder().readObject(fromBytetobyte(newImageBytes));
						if(bsonRes.containsField("dirname")){
							try{
								files.insert(new BasicDBObject(bsonRes.toMap()).append("recovery", true));
							}
							catch(Exception eMongo){
								//eMongo.printStackTrace();
							}
						}else{
							try{
								images.insert(new BasicDBObject(bsonRes.toMap()).append("recovery", true));
							}
							catch(Exception eMongo){
								//eMongo.printStackTrace();
							}	
						}
						count++;
						//System.out.println(bsonRes);
						writer.println(bsonRes);
						newImageBytes = null;
						//return;
					} catch (java.lang.IllegalArgumentException e) {
						try{
							//System.out.println("****************");
							//System.out.println(finalLength+" ---  "+e.getMessage());
							int toRemove = finalLength - Integer.parseInt((e.getMessage().substring(36, e.getMessage().indexOf("!")).trim()));
						    i = i - toRemove;
							for(int j=0; j < toRemove ; j++){
								newImageBytes.remove(newImageBytes.size() - 1);
							}
							finalLength = newImageBytes.size();
							//System.out.println(finalLength);
							//First 4 bytes are for the size of the doc
							//If size in bytes is 200, the four bytes will be 200 0 0 0
							//If size is > 256, ex: 400 the four bytes will be 144 1 0 0 (144+256 *1)
							a = finalLength / 256;
							b = finalLength - a * 256;
							newImageBytes.set(1, (byte) 0);
							if (a != 0) {
								newImageBytes.set(1, (byte) a);
							}
							newImageBytes.set(0, (byte) b);
							//System.out.println(newImageBytes.size());
							//Files.write(outPath, fromBytetobyte(newImageBytes));
							BSONObject bsonRes = new BasicBSONDecoder().readObject(fromBytetobyte(newImageBytes));
							if(bsonRes.containsField("dirname")){
								try{
									files.insert(new BasicDBObject(bsonRes.toMap()).append("recovery", true));
								}
								catch(Exception eMongo){
									//eMongo.printStackTrace();
								}
							}else{
								try{
									images.insert(new BasicDBObject(bsonRes.toMap()).append("recovery", true));
								}
								catch(Exception eMongo){
									//eMongo.printStackTrace();
								}								
							}
								
							count++;
							//System.out.println(bsonRes);
							//writer.println(bsonRes);
						}catch(Exception e1){
							//System.out.println(e.getMessage());
							e1.printStackTrace();
							return;
						}
						} finally {
						newImageBytes = null;
					}
				}
			}
			System.out.println("Count: " + count);
		} finally {
		/*	if (writer != null)
				writer.close();
		}*/
		}
	}
}

