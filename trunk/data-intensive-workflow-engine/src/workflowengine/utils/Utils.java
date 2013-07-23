/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 *
 * @author Orachun
 */
public class Utils
{

    public static final double KB = 1 / 1024;
    public static final double MB = 1;
    public static final double GB = 1024;
    public static final double TB = 1024 * 1024;
    private static final Properties PROP = new Properties();
    private static final String CONFIG_FILE = "default.properties";
    private static boolean isPropInited = false;
    private static final int BUFFER_LEN = 1024*1024; //length of buffer in bytes

    public static void main(String[] args)
    {
        getfileListInDir("/home/we/execution-site-proxy/we-file-storage/wf_1/rawdir/");
    }

    public static Properties getPROP()
    {
        initProp();
        return PROP;
    }
    
    public static void disableDB()
    {
        PROP.setProperty("db_disabled", "true");
    }
    public static void enableDB()
    {
        PROP.setProperty("db_disabled", "false");
    }
    public static boolean isDBEnabled()
    {
        return !PROP.getProperty("db_disabled").equals("true");
    }
    
    public static void initProp()
    {
        if (!isPropInited)
        {
            try
            {
                InputStreamReader is = new FileReader(CONFIG_FILE);
                PROP.load(is);
                is.close();
                PROP.setProperty("home_dir", System.getProperty("user.home"));
                enableDB();
                isPropInited = true;
            }
            catch (IOException ex)
            {
                System.err.println("Cannot read the configuration file " + CONFIG_FILE + ".");
                System.exit(2);
            }
        }
    }

    public static String getProp(String name)
    {
        initProp();
        return PROP.getProperty(name);
    }

    public static int getIntProp(String name)
    {
        String s = getProp(name);
        return s == null ? null : Integer.parseInt(s);
    }

    public static double getDoubleProp(String name)
    {
        String s = getProp(name);
        return s == null ? null : Double.parseDouble(s);
    }

    public static long time()
    {
        return (long) Math.round(System.currentTimeMillis() / 1000.0);
    }

    public static String uuid()
    {
        return UUID.randomUUID().toString();
    }

    public static String execAndWait(String[] cmds, boolean getOutput)
    {
        try
        {
            Process p = Runtime.getRuntime().exec(cmds);
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder sb = null;
            if (getOutput)
            {
                sb = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null)
                {
                    sb.append(line).append('\n');
                }
            }
            p.waitFor();
            return getOutput ? sb.toString() : "";
        }
        catch (IOException | InterruptedException ex)
        {
            return null;
        }
    }

    public static boolean exec(String[] cmds)
    {
        try
        {
            Runtime.getRuntime().exec(cmds);
            return true;
        }
        catch (IOException ex)
        {
            return false;
        }
    }

    public static boolean isFileExist(String path)
    {
        return new File(path).exists();
    }

    public static boolean isDir(String path)
    {
        return new File(path).isDirectory();
    }

    public static void setExecutable(String path)
    {
        new File(path).setExecutable(true);
    }

    public static void setExecutableInDirSince(String dirPath, final long since)
    {
        File[] files = new File(dirPath).listFiles(new FileFilter()
        {
            @Override
            public boolean accept(File pathname)
            {
                if(since == -1)
                {
                    return true;
                }
                return pathname.lastModified() > since;
            }
        });
        for (File f : files)
        {
            f.setExecutable(true);
        }
    }

    public static void createDir(String path)
    {
        new File(path).mkdirs();
    }

    public static void printMap(OutputStream out, Map m)
    {
        PrintWriter pw = new PrintWriter(out);
        pw.println("Printing map ...");
        for (Object k : m.keySet())
        {
            pw.println(k + " ===== " + m.get(k));
            pw.println();
        }
        pw.println("----------------");
        pw.flush();
    }

    public static void pipe(InputStream in, OutputStream out) throws IOException
    {
        byte[] buffer = new byte[BUFFER_LEN];
        int len = 1;
        while (len > -1)
        {
            len = in.read(buffer);
            if(len > -1)
            {
                out.write(buffer, 0, len);
            }
        }
    }
    
    public static void streamToFile(InputStream in, String filepath, long offset, long length) throws IOException
    {
        FileOutputStream fos = new FileOutputStream(filepath);
        FileChannel fc = fos.getChannel();
        fc.position(offset);
        ByteBuffer bb = ByteBuffer.allocate(BUFFER_LEN);
        byte[] buffer = new byte[BUFFER_LEN];
        long count = 0;
        int readSize;
        while(count < length)
        {
            readSize = in.read(buffer);
            bb.put(buffer, 0, readSize);
            fc.write(bb);
        }
        fc.close();
    }
    
    public static String[] getfileListInDir(String dirPath)
    {
        if(!Utils.isFileExist(dirPath))
        {
            return new String[0];
        }
        File file = new File(dirPath).getAbsoluteFile();
        LinkedList<String> fileList = new LinkedList<>();
        LinkedList<File> q = new LinkedList<>();
        q.push(file);
        while(!q.isEmpty())
        {
            File f = q.pop();
            if(f.isDirectory())
            {
                for(File childFile : f.listFiles())
                {
                    q.push(childFile);
                }
            }
            else
            {
                fileList.add(f.getAbsolutePath());
            }
        }
        return fileList.toArray(new String[]{});
    }
    
    public static String getParentPath(String filePath)
    {
        return new File(filePath).getParent();
    }
    
    
    public static String[] getAllfilesInDir(String dirPath)
    {
        File file = new File(dirPath);
        LinkedList<String> fileList = new LinkedList<>();
        LinkedList<File> q = new LinkedList<>();
        q.push(file);
        while(!q.isEmpty())
        {
            File f = q.pop();
            if(f.isDirectory())
            {
                for(File childFile : f.listFiles())
                {
                    q.push(childFile);
                }
            }
            else
            {
                fileList.add(f.getAbsolutePath());
            }
        }
        return fileList.toArray(new String[]{});
    }
    
    
}
