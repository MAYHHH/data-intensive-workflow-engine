/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 *
 * @author Orachun
 */
public class Utils
{
    public static final double KB = 1/1024;
    public static final double MB = 1;
    public static final double GB = 1024;
    public static final double TB = 1024*1024;
    private static final Properties PROP = new Properties();
    private static final String CONFIG_FILE = "default.properties";
    private static boolean isPropInited = false;
    
    public static void main(String[] args)
    {
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
        return (long)Math.round(System.currentTimeMillis()/1000.0);
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
            if(getOutput)
            {
                sb = new StringBuilder();
                String line;
                while((line = br.readLine()) != null)
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
        File[] files = new File(dirPath).listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname)
            {
                return pathname.lastModified() > since;
            }
        });
        for(File f : files)
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
        for(Object k : m.keySet())
        {
            pw.println(k+" ===== "+m.get(k));
            pw.println();
        }
        pw.println("----------------");
        pw.flush();
    }
    
}
