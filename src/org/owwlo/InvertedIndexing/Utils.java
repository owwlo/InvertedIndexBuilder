
package org.owwlo.InvertedIndexing;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

class Utils {

    /**
     * Deserialize an Object from a file.
     * 
     * @param file File to deserialize.
     * @param c Class for result Object.
     * @return An Object deserialized from given file.
     */
    public static Object readObjectFromFile(File file, Class c) {
        Object obj = null;
        Kryo kryo = new Kryo();
        try {
            FileInputStream streamIn = new FileInputStream(file);
            BufferedInputStream bin = new BufferedInputStream(streamIn);
            Input inputStream = new Input(bin);
            obj = kryo.readObject(inputStream, c);
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return obj;
    }

    /**
     * Write object to file. This method will remove original.
     * 
     * @param file File where this object will be written to.
     * @param obj The object that will be serialize
     */
    public static void writeObjectToFile(File file, Object obj) {
        Kryo kryo = new Kryo();
        if (file.exists()) {
            file.delete();
        }
        try {
            FileOutputStream fout = new FileOutputStream(file);
            BufferedOutputStream bout = new BufferedOutputStream(fout);
            Output outputStream = new Output(bout);
            kryo.writeObject(outputStream, obj);
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
