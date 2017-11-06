import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by H77 on 2017/10/25.
 */
public class LocalFSConn {
    static {
        mkdir(Config.LOG);
    }
    public static void  mkdir(String str){
        File file = new File(str);
        if(!file.exists()) file.mkdir();
    }

    public static void deleteFile(String name){
        File f = new File(name);
        if(f.exists()) f.delete();
    }

    public static List<String> listAll(String name){
        File f = new File(name);
        List<String> lists = new ArrayList<>();
        if(!f.exists()) return lists;
        File[] files = f.listFiles();
        for(File file :files) lists.add(file.getPath());
        return lists;
    }

    public static byte[] readFile(String name){
        File file = new File(name);
        if(!file.exists()) return new byte[0];
        Long length = file.length();
        byte[] content = new byte[length.intValue()];
        FileInputStream in = null;
        try {
            in = new FileInputStream(file);
            in.read(content);
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return content;
    }
}
