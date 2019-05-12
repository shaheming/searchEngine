package edu.uci.ics.cs221.index.inverted;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
/**
 * Implement this compressor with Delta Encoding and Variable-Length Encoding. See Project 3
 * description for details.
 */
public class DeltaVarLenCompressor implements Compressor {

  @Override
  public byte[] encode(List<Integer> integers) {
    if (integers.isEmpty()) {
      return new byte[0];
    }

    ByteArrayOutputStream encoded = new ByteArrayOutputStream();
    Stack<Byte> bytestack = new Stack<>();
    int pre_num = 0;
    for (Integer num : integers) {
      int tmp = num;
      num = num - pre_num;
      pre_num = tmp;
      if (num < 0) {
        throw new RuntimeException("array is not sorted");
      }
      // 0x7F -> 127 01111111
      bytestack.push((byte) (num & 0x7F));
      num >>= 7;
      // 0x80 -> 128 10000000
      while (num > 0) {
        bytestack.push((byte) (num & 0x7F | 0x80));
        num >>= 7;
      }
      while (!bytestack.isEmpty()) {
        //        System.out.println(Integer.toBinaryString((int) out.pop() & 0xFF));
        encoded.write(bytestack.pop());
      }
    }
    return encoded.toByteArray();
  }

  @Override
  public List<Integer> decode(byte[] bytes, int start, int length) {
    if (length == 0 || start >= bytes.length) return new ArrayList<>();
    ArrayList<Integer> decoded = new ArrayList<>(length / (4));
    int num = 0;
    int pre_num = 0;
    for (int i = start; i < start + length; i++) {
      num = 0;
      while ((bytes[i] & 0x80) == 0x80) {
        num += bytes[i] & 0x7F;
        num <<= 7;
        i++;
      }
      num += bytes[i] & 0x7F;
      num += pre_num;
      decoded.add(num);
      pre_num = num;
    }
    return decoded;
  }
}
