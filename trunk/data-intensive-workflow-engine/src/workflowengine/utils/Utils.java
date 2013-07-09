/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

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
}
