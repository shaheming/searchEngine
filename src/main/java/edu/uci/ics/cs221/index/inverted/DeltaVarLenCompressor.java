package edu.uci.ics.cs221.index.inverted;

import java.util.ArrayList;
import java.util.*;


/**
 * Implement this compressor with Delta Encoding and Variable-Length Encoding.
 * See Project 3 description for details.
 */
public class DeltaVarLenCompressor implements Compressor {
    static public double log(double value, double base) {
        return Math.log(value) / Math.log(base);
    }

    @Override
    public byte[] encode(List<Integer> integers) {
        int size=integers.size();
        ArrayList list = new ArrayList();
        for(int i=0;i<size;i++){
            int new_value;
            if(i==0) new_value=integers.get(0);
            else new_value=integers.get(i)-integers.get(i-1);
            int bytes_length = ((32-Integer.numberOfLeadingZeros(new_value))+ 6)/7;
            bytes_length = bytes_length > 0 ? bytes_length : 1;
            byte[] temp = new byte[bytes_length];
            for(int j = bytes_length-1; j >=0; j--) {
                temp[j] = (byte) ((new_value & 0b1111111) | 0b10000000); //0b->binary
                new_value >>= 7;
            }
            temp[0] = (byte) (temp[0]&0b01111111); //reset the first byte

            for(int j = 0; j < bytes_length; j++) {
                list.add(temp[j]);
            }
        }
        byte[] result = new byte[list.size()];
        for(int i=0;i<list.size();i++){
            result[i]=(byte)list.get(i);
        }
        return result;

    }

    @Override
    public List<Integer> decode(byte[] bytes, int start, int length) {
       byte[] buffer=new byte[length];
       for(int i=0;i<length;i++){
           buffer[i]=bytes[start+i];
       }



        throw new UnsupportedOperationException();
    }
}
