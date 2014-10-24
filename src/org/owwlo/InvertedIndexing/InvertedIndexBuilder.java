
package org.owwlo.InvertedIndexing;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InvertedIndexBuilder {
    private static final String IDX_FILE_PREFIX = "idx";
    private static final String POST_LIST_PREFIX = "pstl";

    /**
     * Count how many distributed Inverted Index Map has been created.
     */
    private int _mapCount = 0;

    /**
     * Structure holds relation from tokens offset.
     */
    private Map<String, Integer[]> _idxMap = new HashMap<String, Integer[]>();

    /**
     * Base dir for the whole index.
     */
    private File _baseDir;

    /**
     * Collections of different instances of IvtMap.
     */
    private List<IvtMapBase> ivtCollection = new LinkedList<InvertedIndexBuilder.IvtMapBase>();

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
        return new InvertedIndexBuilder(dir);
    }

    private InvertedIndexBuilder(File dir) {
        this._baseDir = dir;
    }

    public IvtMapInteger createDistributedIvtiIntegerMap() {
        IvtMapInteger map = new IvtMapInteger(_baseDir, _mapCount, true, _idxMap, this);
        if (map != null) {
            this.ivtCollection.add(map);
            _mapCount++;
        }
        return map;
    }

    /**
     * Commit all changes within Inverted Index Maps built through this builder
     * to storage.
     */
    public void commit() {
        for (IvtMapBase ivt : ivtCollection) {
            ivt.commit();
        }
        File idxFile = new File(_baseDir, IDX_FILE_PREFIX);
        Utils.writeObjectToFile(idxFile, _idxMap);
    }

    /**
     * Close this builder.
     */
    public void close() {
        commit();
        for (IvtMapBase ivt : ivtCollection) {
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
        private RandomAccessFile postListIn;
        private File pstlFile;
        private boolean isNew;
        private int mapId;
        private Map<String, Integer[]> idxMap;
        private InvertedIndexBuilder builder;

        private IvtMapInteger(File dir, int mapId, boolean isNew, Map<String, Integer[]> idxMap,
                InvertedIndexBuilder builder) {
            this.isNew = isNew;
            this.mapId = mapId;
            this.idxMap = idxMap;
            this.builder = builder;
            pstlFile = new File(dir, POST_LIST_PREFIX + mapId);
            if (isNew) {
                createNewMap(pstlFile);
            } else {
                loadMap(pstlFile);
            }
        }

        private void loadMap(File file) {
            try {
                postListIn = new RandomAccessFile(file, "r");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        private void createNewMap(File file) {
            try {
                postListOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(
                        file, false)));
                postListIn = new RandomAccessFile(file, "r");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void clear() {
        }

        @Override
        public boolean containsKey(Object key) {
            if (idxMap.containsKey(key) && idxMap.get(key).length > mapId
                    && idxMap.get(key)[mapId] != -1) {
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
            List<Integer> result = new ArrayList<Integer>();
            if ((!idxMap.containsKey(key)) || idxMap.get(key).length <= mapId
                    || idxMap.get(key)[mapId] == -1) {
                return result;
            }
            int offset = idxMap.get(key)[mapId];
            try {
                postListIn.seek(offset);
                int size = postListIn.readInt();
                for (int i = 0; i < size; i++) {
                    result.add(postListIn.readInt());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public boolean isEmpty() {
            for (String key : idxMap.keySet()) {
                Integer[] offsetList = idxMap.get(key);
                if (offsetList.length > mapId
                        && offsetList[mapId] != -1) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Set<String> keySet() {
            Set<String> set = new HashSet<String>();
            for (String key : idxMap.keySet()) {
                Integer[] offsetList = idxMap.get(key);
                if (offsetList.length > mapId
                        && offsetList[mapId] != -1) {
                    set.add(key);
                }
            }
            return set;
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
                } else {
                    postListIn.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
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

    private static <T> T[] extendArraySize(T[] array, int newSize) {
        T[] temp = array.clone();
        array = (T[]) new Object[newSize];
        System.arraycopy(temp, 0, array, 0, temp.length);
        return array;
    }

    synchronized private void writeIndex(String token, int offset, int mapId) {
        if (!_idxMap.containsKey(token)) {
            _idxMap.put(token, new Integer[_mapCount]);
        }
        Integer[] offsetList = _idxMap.get(token);
        if (offsetList.length != _mapCount) {
            offsetList = extendArraySize(offsetList, _mapCount);
            _idxMap.put(token, offsetList);
        }
        _idxMap.get(token)[mapId] = offset;
    }
}
