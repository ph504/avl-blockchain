package approach4.MPTPointerBased;


import approach4.CompositeKey;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 *  Serialization interface to define how to convert a given T type into ByteString.
 *  There are some built in common implementations for convenience.
 *  
 *  TODO: ByteString introduces an unnecessary level of byte[] copying.
 *  
 * @author tr1b6162
 *
 * @param <T>
 */
public interface Serializer<T> {
	
	byte[] serialize(T obj);
	T deserialize(byte[] bytes);
	
	public static final Serializer<String> STRING_UTF8 = new Serializer<String>() {
		@Override
		public byte[] serialize(String obj) {
			return obj.getBytes(StandardCharsets.UTF_8);
		}
		
		@Override
		public String deserialize(byte[] bytes) {
			return new String(bytes, StandardCharsets.UTF_8);
		}
	};

	public static final Serializer<CompositeKey<Integer, Date>> COMPOSITE_KEY = new Serializer<CompositeKey<Integer, Date>>() {
		@Override
		public byte[] serialize(CompositeKey<Integer, Date> obj) {
			ByteBuffer bb = ByteBuffer.allocate(24+4);
			bb.putLong(0, obj.k2.getTime());
			bb.putInt(24, obj.k1);
			return bb.array();
		}

		@Override
		public CompositeKey<Integer, Date> deserialize(byte[] bytes) {

			if (bytes == null || bytes.length == 0) throw new AssertionError("Does not allow null values");

			ByteBuffer bb = ByteBuffer.wrap(bytes);

			byte[] dateB = new byte[24];
			byte[] intB = new byte[4];
			bb.get(dateB, 0, 24);
			bb.get(intB, 24, 4);

			Date date = new Date(ByteBuffer.wrap(dateB).getLong());
			Integer int_ = ByteBuffer.wrap(intB).getInt();

			CompositeKey<Integer, Date> ck = new CompositeKey<>(int_, date);
			return ck;


		}
	};
	
	/**
	 * TODO, test me
	 */
	public static final Serializer<Boolean> BOOLEAN = new Serializer<Boolean>() {
		private final byte[] FALSE = new byte[] {(byte) 0x00};
		private final byte[] TRUE = new byte[] {(byte) 0xFF};
		
		@Override
		public byte[] serialize(Boolean obj) {
			return obj ? TRUE : FALSE;
		}
		@Override
		public Boolean deserialize(byte[] bytes) {
			return bytes.length == TRUE.length && bytes[0] == TRUE[0];
		}
	};
	
	/**
	 * TODO, test me
	 */
	public static final Serializer<Long> INT64 = new Serializer<Long>() {
		@Override
		public byte[] serialize(Long l) {
			return serializeLong(l);
		}
		@Override
		public Long deserialize(byte[] bytes) {
			return deserializeLong(bytes);
		}	
	};

	static long deserializeLong(byte[] bytes) {
		if (bytes == null || bytes.length == 0) throw new AssertionError("Does not allow null values");
		long result = 0;
		for (int i = 0; i < 8; i++) {
			result <<= 8;
			result |= (bytes[i] & 0xFF);
		}
		return result;
	}

	static byte[] serializeLong(Long l) {
		if (l == null) throw new AssertionError("Does not allow null values");
		byte[] result = new byte[Long.BYTES];
		for (int i = 7; i >= 0; i--) {
			result[i] = (byte)(l & 0xFF);
			l >>= 8;
		}
		return result;
	}

	/**
	 * TODO, test me
	 */
	public static final Serializer<Integer> INT32 = new Serializer<Integer>() {
		@Override
		public byte[] serialize(Integer l) {
			return serializeInteger(l);
		}
		@Override
		public Integer deserialize(byte[] bytes) {
			return deserializeInteger(bytes);
		}
	};

	static int deserializeInteger(byte[] bytes) {
		if (bytes == null || bytes.length == 0) throw new AssertionError("Does not allow null values");
		int result = 0;
		for (int i = 0; i < 4; i++) {
			result <<= 8;
			result |= (bytes[i] & 0xFF);
		}
		return result;
	}

	static byte[] serializeInteger(Integer l) {
		if (l == null) throw new AssertionError("Does not allow null values");
		byte[] result = new byte[Integer.BYTES];
		for (int i = 3; i >= 0; i--) {
			result[i] = (byte)(l & 0xFF);
			l >>= 8;
		}
		return result;
	}

}
