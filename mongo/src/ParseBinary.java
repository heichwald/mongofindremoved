import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;

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

	static boolean matchWord(List<Byte> in, byte[] token) {
		return in.size() >= token.length
				&& equalsBytes(in.subList(in.size() - token.length, in.size()),
						token);
	}

	static int int32FromBytes(byte a1, byte a2, byte a3, byte a4) {
		byte[] intAs4Bytes = new byte[] { a1, a2, a3, a4 };
		return ByteBuffer.wrap(intAs4Bytes).getInt();
	}

	public static void main(String[] args) throws IOException {
		PrintWriter writer = null;
		try {
			Path inPath = Paths.get("/home/herve/Downloads/assets.0");
			writer = new PrintWriter("out");
			LinkedList<Byte> storageIdDel = new LinkedList<>();
			storageIdDel.add(Byte.valueOf((byte) 105));
			storageIdDel.add(Byte.valueOf((byte) 100));
			storageIdDel.add(Byte.valueOf((byte) 0));
			storageIdDel.add(Byte.valueOf((byte) 41));
			byte[] data = Files.readAllBytes(inPath);
			LinkedList<Byte> last4bytes = new LinkedList<>();
			List<Byte> end = new ArrayList<>(4);
			end.add((byte) 0);
			end.add((byte) 0);
			end.add((byte) 0);
			end.add((byte) 0);
			List<Byte> start = new ArrayList<>();
			start.add((byte) 7);
			start.add((byte) 95);
			start.add((byte) 105);
			start.add((byte) 100);
			List<BSONObject> res = new ArrayList<>();
			List<Byte> newImageBytes = null;
			int endIndex = -1;
			int count = 0;
			for (int i = 0; i < data.length; i++) {
				if (last4bytes.size() >= 4) {
					last4bytes.remove();
				}
				last4bytes.add(data[i]);
				if (newImageBytes != null) {
					newImageBytes.add(data[i]);
				}
				if (last4bytes.equals(start)) {
					// System.out.println("yes");
					for (int j = 0; j < 4; j++) {
						// -18 == 0xEE
						if (data[i + j - 7] == (byte) -18) {
							newImageBytes = new ArrayList<>(last4bytes);
						}
					}
				} else if (newImageBytes != null
						&& last4bytes.equals(storageIdDel)) {
					endIndex = i + 44;
				} else if (i == endIndex) {
					int prepnb = 4;
					int appnb = 2;

					for (int j = 0; j < prepnb; j++) {
						newImageBytes.add(0, (byte) 0);
					}
					for (int j = 0; j < appnb; j++) {
						newImageBytes.add((byte) 0);
					}
					int finalLength = newImageBytes.size();
					int a = finalLength / 256;
					int b = finalLength - a * 256;

					if (a != 0) {
						newImageBytes.set(1, (byte) a);
					}
					newImageBytes.set(0, (byte) b);
					try {
						BSONObject bsonRes = new BasicBSONDecoder()
								.readObject(fromBytetobyte(newImageBytes));
						count++;
						System.out.println(bsonRes);
						writer.println(bsonRes);
						newImageBytes = null;
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						newImageBytes = null;
						endIndex = -1;
					}
				}
			}
			System.out.println("Count: " + count);
		} finally {
			if (writer != null)
				writer.close();
		}
	}
}
