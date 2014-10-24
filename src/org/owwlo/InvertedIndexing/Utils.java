
package org.owwlo.InvertedIndexing;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

class Utils {

    public static Object readObjectFromFile(File file) {
        Object obj = null;
        if (file.exists()) {
            file.delete();
        }
        try {
            FileInputStream streamIn = new FileInputStream(file);
            BufferedInputStream bin = new BufferedInputStream(streamIn);
            ObjectInputStream objectinputstream = new ObjectInputStream(bin);
            obj = objectinputstream.readObject();
            objectinputstream.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
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
        if (file.exists()) {
            file.delete();
        }
        try {
            FileOutputStream fout = new FileOutputStream(file);
            BufferedOutputStream bout = new BufferedOutputStream(fout);
            ObjectOutputStream oos = new ObjectOutputStream(bout);
            oos.writeObject(obj);
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

    }

}
