package edu.uci.ics.cs221.index.inverted;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.*;


/**
 * Implement this compressor with Delta Encoding and Variable-Length Encoding.
 * See Project 3 description for details.
 */
public class DeltaVarlenCompressor_Chenghao implements Compressor {
    static public double log(double value, double base) {
        return Math.log(value) / Math.log(base);
    }

    @Override
    public byte[] encode(List<Integer> integers) {
        if (integers.isEmpty()) {
            return new byte[0];
        }
        int size=integers.size();
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        for(int i=0;i<size;i++){
            int new_value;
            if(i==0) new_value=integers.get(0);
            else new_value=integers.get(i)-integers.get(i-1);
            int bytes_length = ((32-Integer.numberOfLeadingZeros(new_value))+ 6)/7;
            bytes_length = bytes_length > 0 ? bytes_length : 1;
            byte[] temp = new byte[bytes_length];
            for(int j = bytes_length-1; j>=0; j--) {
                temp[j] = (byte) ((new_value & 0b01111111) | 0b10000000); //0b->binary
                new_value >>= 7;
            }
            temp[bytes_length-1] = (byte) (temp[bytes_length-1]&0b01111111); //reset the first byte
            for(int j = 0; j < bytes_length; j++) {
                result.write(temp[j]);
            }
        }
        return result.toByteArray();

    }

    @Override
    public List<Integer> decode(byte[] bytes, int start, int length) {
        if(length==0||length>bytes.length||(start+length)>bytes.length) return null;
        List<Integer> result=new ArrayList<>();
        int temp=0;
        int pre_temp=0;
        for(int i=0;i<length;i++){
            temp=temp+(byte)(bytes[start+i]& 0b01111111);
            if((bytes[start+i]&0b10000000)==0b00000000){
                temp=pre_temp+temp;
                result.add(temp);
                pre_temp=temp;
                temp=0;
            }
            else temp <<= 7;

        }

        return result;
    }
}