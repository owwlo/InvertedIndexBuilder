
package org.owwlo.InvertedIndexing;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class InvertedIndexBuilder {
    private static final String IDX_FILE_PREFIX = "idx";
    private static final String SE_IDX_FILE_PREFIX = "seIdx";
    private static final String IDX_OBJ_FILE_PREFIX = "idxObj";
    private static final String POST_LIST_PREFIX = "pstl";

    /**
     * Keep a one-to-one correspondence between token and offset of Secondary
     * Index.
     */
    private Map<String, Integer> _tokenMap = new LinkedHashMap<String, Integer>();

    /**
     * Reader for Secondary Index.
     */
    private RandomAccessFile _seIdxIn;

    /**
     * Readers for each Posting List file.
     */
    private List<RandomAccessFile> _ivtiMapInList = new ArrayList<RandomAccessFile>();

    /**
     * Tool for serialization/deserialization.
     */
    private Kryo kryo = new Kryo();

    /**
     * Count how many distributed Inverted Index Map has been created.
     */
    private int _mapCount = 0;

    /**
     * Base dir for the whole index.
     */
    private File _baseDir;

    /**
     * Collections of different instances of IvtMap.
     */
    private List<IvtMapBase> _ivtCollection = new LinkedList<InvertedIndexBuilder.IvtMapBase>();

    /**
     * Writers for Secondary Index.
     */
    private Map<Integer, Output> _secondIdxOutputObjectStreamMap = new HashMap<Integer, Output>();

    /**
     * Create a new builder instance.
     * 
     * @param dir Path where you want to create invertMap
     * @return New InvertedIndexBuilder instance.
     */
    public static InvertedIndexBuilder getBuilder(File dir) {
        if (dir.exists() && !dir.isDirectory()) {
            throw new Error(dir.getAbsolutePath() + " is not a directory.");
        }
        boolean loadExisting = false;
        if (new File(dir, IDX_FILE_PREFIX).exists()) {
            loadExisting = true;
        }
        return new InvertedIndexBuilder(dir, loadExisting);
    }

    private InvertedIndexBuilder(File dir, boolean loadExisting) {
        this._baseDir = dir;
        if (loadExisting) {
            loadIvtMaps(dir);
        }
    }

    private List<File> getAllFiles(final File folder) {
        List<File> fileList = new LinkedList<File>();

        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                fileList.addAll(getAllFiles(fileEntry));
            } else {
                fileList.add(fileEntry);
            }
        }
        return fileList;
    }

    private void loadIvtMaps(File dir) {
        List<File> fileList = getAllFiles(dir);
        for (File file : fileList) {
            String filename = file.getName();
            if (filename.startsWith(POST_LIST_PREFIX)) {
                _mapCount++;
            }
        }
        _tokenMap = (Map<String, Integer>) Utils.readObjectFromFile(new File(_baseDir,
                IDX_FILE_PREFIX), LinkedHashMap.class);

        try {
            _seIdxIn = new RandomAccessFile(new File(dir, SE_IDX_FILE_PREFIX), "r");
            for (int i = 0; i < _mapCount; i++) {
                _ivtiMapInList.add(new RandomAccessFile(new File(dir, POST_LIST_PREFIX + i), "r"));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public IvtMapInteger getUnifiedDistributedIvtiIntegerMap() {
        return new IvtMapInteger(_baseDir, -1, false, _tokenMap, this);
    }

    public IvtMapInteger createDistributedIvtiIntegerMap() {
        IvtMapInteger map = new IvtMapInteger(_baseDir, _mapCount, true, _tokenMap, this);
        if (map != null) {
            _ivtCollection.add(map);
            try {
                File secondIdxFile = new File(_baseDir, IDX_OBJ_FILE_PREFIX + _mapCount);
                FileOutputStream fout = new FileOutputStream(secondIdxFile);
                BufferedOutputStream bout = new BufferedOutputStream(fout);
                _secondIdxOutputObjectStreamMap.put(_mapCount, new Output(bout));
                _mapCount++;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return map;
    }

    /**
     * Commit all changes within Inverted Index Maps built through this builder
     * to storage.
     */
    public void commit() {
        for (IvtMapBase ivt : _ivtCollection) {
            ivt.commit();
        }
        buildSecondIndex();
    }

    private void buildSecondIndex() {
        try {
            List<Input> objInList = new LinkedList<Input>();
            List<FileInputStream> streamInList = new LinkedList<FileInputStream>();
            List<File> objFileList = new LinkedList<File>();
            for (int i = 0; i < _mapCount; i++) {
                File secondIdxFile = new File(_baseDir, IDX_OBJ_FILE_PREFIX + i);
                FileInputStream streamIn = new FileInputStream(secondIdxFile);
                BufferedInputStream bin = new BufferedInputStream(streamIn);
                Input oin = new Input(bin);
                streamInList.add(streamIn);
                objInList.add(oin);
                objFileList.add(secondIdxFile);
            }

            DataOutputStream sIdxOut = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(new File(_baseDir, SE_IDX_FILE_PREFIX))));

            Map<String, Integer> tokenMap = new LinkedHashMap<String, Integer>();
            for (int i = 0; i < _mapCount; i++) {
                Input ois = objInList.get(i);
                while (ois.available() > 0) {
                    SecondIndexObject sio = (SecondIndexObject) kryo.readObject(ois,
                            SecondIndexObject.class);
                    String token = sio.getToken();
                    if (!tokenMap.containsKey(token)) {
                        tokenMap.put(token, tokenMap.size());
                    }
                }
            }

            Utils.writeObjectToFile(new File(_baseDir, IDX_FILE_PREFIX), tokenMap);

            for (FileInputStream fis : streamInList) {
                fis.close();
            }
            objInList.clear();
            streamInList.clear();
            for (File file : objFileList) {
                FileInputStream streamIn = new FileInputStream(file);
                BufferedInputStream bin = new BufferedInputStream(streamIn);
                Input oin = new Input(bin);
                streamInList.add(streamIn);
                objInList.add(oin);
            }

            for (int i = 0; i < _mapCount; i++) {
                Input ois = objInList.get(i);
                for (String key : tokenMap.keySet()) {
                    tokenMap.put(key, -1);
                }
                while (ois.available() > 0) {
                    SecondIndexObject sio = (SecondIndexObject) kryo.readObject(ois,
                            SecondIndexObject.class);
                    String token = sio.getToken();
                    int offset = sio.getOffset();
                    tokenMap.put(token, offset);
                }
                for (String key : tokenMap.keySet()) {
                    sIdxOut.writeInt(tokenMap.get(key));
                }
            }

            for (File f : objFileList) {
                f.delete();
            }
            sIdxOut.flush();
            sIdxOut.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Close this builder.
     */
    public void close() {
        commit();
        for (IvtMapBase ivt : _ivtCollection) {
            ivt.close();
        }
    }

    /**
     * Interface for IvtMapBase
     * 
     * @author owwlo
     */
    private static abstract class IvtMapBase {
        public abstract void commit();

        public abstract void close();
    }

    public static class IvtMapInteger extends IvtMapBase implements Map<String, List<Integer>> {
        private DataOutputStream postListOut;
        private File pstlFile;
        private boolean isNew;
        private int mapId;
        private Map<String, Integer> seIdxMap;
        private InvertedIndexBuilder builder;

        private IvtMapInteger(File dir, int mapId, boolean isNew, Map<String, Integer> idxMap,
                InvertedIndexBuilder builder) {
            this.isNew = isNew;
            this.mapId = mapId;
            this.seIdxMap = idxMap;
            this.builder = builder;
            pstlFile = new File(dir, POST_LIST_PREFIX + mapId);
            if (isNew) {
                createNewMap(pstlFile);
            }
        }

        private void createNewMap(File file) {
            try {
                postListOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(
                        file, false)));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void clear() {
        }

        @Override
        public boolean containsKey(Object key) {
            if (seIdxMap.containsKey(key)) {
                return true;
            }
            return false;
        }

        @Override
        public boolean containsValue(Object value) {
            return false;
        }

        @Override
        public Set<java.util.Map.Entry<String, List<Integer>>> entrySet() {
            return null;
        }

        @Override
        public List<Integer> get(Object key) {
            return builder.getPostingList(key.toString(), mapId);
        }

        @Override
        public boolean isEmpty() {
            return seIdxMap.isEmpty();
        }

        @Override
        public Set<String> keySet() {
            return seIdxMap.keySet();
        }

        @Override
        synchronized public List<Integer> put(String key, List<Integer> value) {
            int offset = postListOut.size();
            int size = value.size();
            builder.writeIndex(key, offset, mapId);
            try {
                postListOut.writeInt(size);
                for (int integer : value) {
                    postListOut.writeInt(integer);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return value;
        }

        @Override
        public void putAll(Map<? extends String, ? extends List<Integer>> m) {
            for (String key : m.keySet()) {
                this.put(key, m.get(key));
            }
        }

        @Override
        public List<Integer> remove(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            return this.keySet().size();
        }

        @Override
        public Collection<List<Integer>> values() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            try {
                if (isNew) {
                    postListOut.flush();
                    postListOut.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            builder._ivtCollection.remove(this);
            builder.closeForIvtMap(mapId);
        }

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            this.close();
        }

        @Override
        public void commit() {
        }
    }

    synchronized private void writeIndex(String token, int offset, int mapId) {
        SecondIndexObject sio = new SecondIndexObject(token, offset);
        kryo.writeObject(_secondIdxOutputObjectStreamMap.get(mapId), sio);
    }

    synchronized private void closeForIvtMap(int mapId) {
        Output out = _secondIdxOutputObjectStreamMap.get(mapId);
        out.close();
        _secondIdxOutputObjectStreamMap.remove(mapId);
    }

    synchronized private List<Integer> getOffsets(String token) {
        List<Integer> offsets = new ArrayList<Integer>();
        if (!_tokenMap.containsKey(token)) {
            return offsets;
        }
        int seIdxOffset = _tokenMap.get(token);
        try {
            for (int i = 0; i < _mapCount; i++) {
                _seIdxIn.seek(_tokenMap.size() * Integer.SIZE / 8 * i + seIdxOffset * Integer.SIZE
                        / 8);
                int offset = _seIdxIn.readInt();
                offsets.add(offset);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return offsets;
    }

    synchronized private List<Integer> getPostingList(String token, int mapId) {
        List<Integer> offsets = getOffsets(token);
        List<Integer> result = new ArrayList<Integer>();
        try {
            for (int i = 0; i < offsets.size(); i++) {
                int offset = offsets.get(i);
                if (offset == -1) {
                    continue;
                }
                RandomAccessFile raf = _ivtiMapInList.get(i);
                raf.seek(offset);
                int length = raf.readInt();
                for (int j = 0; j < length; j++) {
                    result.add(raf.readInt());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    private static class SecondIndexObject implements Serializable {
        private String token;
        private int offset;

        public SecondIndexObject(String token, int offset) {
            super();
            this.token = token;
            this.offset = offset;
        }

        public String getToken() {
            return token;
        }

        public int getOffset() {
            return offset;
        }
    }

    public int getTotalIvtiMapCount() {
        return _mapCount;
    }
}
