package approach4.MPTPointerBased;


import java.util.Iterator;

/**
 * Immutable class representing an arbitrary length of nibbles.
 * 
 * Since underlying byte[] is immutable substring() methods return a view of same array 
 * so they are fast.
 * 
 * @author tr1b6162
 *
 */
public class NibbleString implements Iterable<Byte>{

	public static final byte EVEN_START 	= 0b0000_0000;
	public static final byte ODD_START 		= 0b0001_0000;
	public static final byte TERMINAL 		= 0b0010_0000;
	private static final char[] base16 = new char[]{'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
	
	// each nibble is one byte despite the obvious waste of space
	// each nibble is right aligned
	private byte[] nibbles; 
	private int offset;
	private int length;
	
	private NibbleString() {};
	
	private NibbleString(byte[] nibbles, int begin, int length) {
		this.nibbles = nibbles;
		this.offset = begin;
		this.length = length;
	}
	
	/**
	 * Returns a char as nibbles hex representation
	 * 
	 * 0b0000_0110 ==> '6'
	 * 0b0000_1111 ==> 'f'
	 * 
	 * etc..
	 * 
	 * @param pos
	 * @return
	 */
	public char nibbleAsChar(int pos) {
		return base16[nibbleAsByte(pos)];
	}
	
	/**
	 * Returns nibble as byte.
	 * Returned nibble is right aligned and can be used as an index
	 * 
	 * @param pos
	 * @return
	 */
	public byte nibbleAsByte(int pos) {
		if (pos < 0 || pos >= length) throw new IllegalArgumentException("Out of bounds");
		return nibbles[pos + offset];
	}
	
	public int size() {
		return length;
	}
	
	/**
	 * Returns a view of the underlying NibbleString bounded by the given indices,
	 * without copying its underlying byte[].
	 * 
	 * @param startIndex
	 * @return
	 */
	public NibbleString substring(int startIndex) {
		return substring(startIndex, size());
	}
	
	/**
	 * Returns a view of the underlying NibbleString bounded by the given indices,
	 * without copying its underlying byte[].
	 * 
	 * @param startIndex
	 * @param endIndex
	 * @return
	 */
	public NibbleString substring(int startIndex, int endIndex) {
		final int newLength = endIndex - startIndex;
		if ((startIndex | endIndex | newLength | (length - newLength)) < 0) 
			throw new IllegalArgumentException("Out of bounds");
		
		return new NibbleString(nibbles, offset + startIndex, newLength);
	}
	
	/**
	 * Converts given ByteBuffer to NibbleString by copying the underlying byte[] 
	 * 
	 * Resulting NibbleString is always even length and is not packed.
	 * 
	 * @param bytes
	 * @return
	 */
	public static NibbleString from(byte[] bytes) {
		NibbleString instance = new NibbleString();
		int len = bytes.length;
		instance.nibbles = new byte[len << 1];
		
		int w = 0;
		for (int r=0; r<len; r++) {
			instance.nibbles[w++] = (byte) ((bytes[r] & 0xf0) >> 4);
			instance.nibbles[w++] = (byte) ((bytes[r] & 0x0f));
		}
		
		instance.offset = 0;
		instance.length = instance.nibbles.length;
		
		return instance;
		
	}

	/**
	 * Unpacks a packed ByteBuffer into a NibbleString. 
	 * 
	 * First nibble of a packed ByeString always contains leading flags representing 
	 *  1) nibble is odd/even length. (Since a byte can store 2 nibbles)
	 * 	2) nibble is a key for a terminal node 
	 * 
	 * @param bytes
	 * @return
	 */
	public static NibbleString unpack(byte[] bytes) {
		if (bytes == null || bytes.length == 0) throw new IllegalArgumentException("Can not be empty");
		
		int len = bytes.length;
		NibbleString instance = new NibbleString();
		
		int w = 0, r = 1;
		if ((bytes[0] & ODD_START) == ODD_START) {
			instance.nibbles = new byte[(len << 1) - 1];
			instance.nibbles[w++] = (byte) (bytes[0] & 0x0f);
		} else {
			instance.nibbles = new byte[(len << 1) - 2];
		}
		
		while (r<len) {
			instance.nibbles[w++] = (byte) ((bytes[r] & 0xf0) >> 4);
			instance.nibbles[w++] = (byte) (bytes[r++] & 0x0f);
		}
		
		instance.offset = 0;
		instance.length = instance.nibbles.length;
				
		return instance;
	}
	
	/**
	 * Packs a NibbleString into a packed ByteBuffer.
	 * 
	 * First nibble of a packed ByeString always contains leading flags representing 
	 *  1) nibble is odd/even length. (Since a byte can store 2 nibbles)
	 * 	2) nibble is a key for a terminal node
	 * 
	 * @param n
	 * @param isTerminal
	 * @return
	 */
	public static byte[] pack(NibbleString n, boolean isTerminal) {
		int len = n.size();
		
		byte[] result = new byte[(len >> 1) + 1];
		boolean odd = (len & 0x01) == 1;
		
		byte flag = odd ? ODD_START : EVEN_START;
		flag = (byte) (isTerminal ? flag | TERMINAL : flag);
		
		result[0] = odd ? (byte) (flag | n.nibbleAsByte(0)) : flag;
		
		int read = odd ? 1 : 0;
		int write = 1;
		while (read < len) {
			result[write++] = (byte) ((n.nibbleAsByte(read++) << 4) | n.nibbleAsByte(read++));
		}
		
		return result;
	}

	@Override
	public int hashCode() {
        if (nibbles == null)
            return 0;

        int result = 1;
        for (int i=0; i<length; i++) {
        	result = 31 * result + nibbleAsByte(i);
        }

        return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NibbleString other = (NibbleString) obj;
		if (length != other.length)
			return false;
		for (int i=0; i<length; i++) {
			if (nibbleAsByte(i) != other.nibbleAsByte(i))
				return false;
		}
		return true;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (int i=0; i<length; i++) {
			sb.append(nibbleAsChar(i));
		}
		return sb.toString();
	}

	@Override
	public Iterator<Byte> iterator() {
		return new Iterator<Byte>() {
			int i=0;
			@Override public boolean hasNext() { return length > i;}
			@Override public Byte next() { return nibbleAsByte(i++);}
		};
	}
}
