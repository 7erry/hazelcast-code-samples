import com.hazelcast.core.QueueStore;

import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.io.*;

import org.apache.commons.io.FilenameUtils;

public class TheQueueStore implements QueueStore<Item> {

    private void writeFile(Long key, Item value) throws IOException{
        FileOutputStream fout = null;
        try {
            fout = new FileOutputStream("./"+key+".ser");
            ObjectOutputStream oos = new ObjectOutputStream(fout);
            oos.writeObject(value);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private Item readFile(Long key) {
        FileInputStream fin = null;
        try {
            fin = new FileInputStream("./"+key+".ser");
            ObjectInputStream ois = new ObjectInputStream(fin);
            return (Item)ois.readObject();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            return null;
        }
    }

    private void deleteFile(Long key) {
        File file = new File("./"+key+".ser");
        file.delete();
    }

    @Override
    public void delete(Long key) {
        System.out.println("delete:	"+key.toString());
        deleteFile(key);
    }

    @Override
    public void store(Long key, Item value) {
        System.out.println("store:\t"+key.toString());
        try {
            writeFile(key,value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void storeAll(Map<Long, Item> map) {
        System.out.println("store all:	"+map.size());
	for (Map.Entry<Long, Item> entry : map.entrySet()) {	
            try {
                writeFile(entry.getKey(), entry.getValue());
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
    }

    @Override
    public void deleteAll(Collection<Long> keys) {
        System.out.println("deleteAll:	"+keys.size());
	for (Long key : keys) {
	    deleteFile(key);
	}
    }

    @Override
    public Item load(Long key) {
        System.out.println("load:\t"+key.toString());
        return readFile(key);
    }

    @Override
    public Map<Long, Item> loadAll(Collection<Long> keys) {
        System.out.println("loadAll");
        Map<Long,Item> map = new HashMap();
	for (Long key : keys) {
            map.put(key,readFile(key));
        };
        return map;
    }

    @Override
    public Set<Long> loadAllKeys() {
        System.out.println("loadAllKeys");
        Set<Long> keys = new HashSet();
        File[] files = new File("./").listFiles();
        for (File file : files) {
            // ensure we only read the {key}.ser files
            if (file.isFile() && file.getName().contains("ser")) {
                String key = FilenameUtils.removeExtension(file.getName());
                System.out.println("\t Loading key:\t"+key);
                keys.add(new Long(key));
            }
        }
        return keys;
    }
}
