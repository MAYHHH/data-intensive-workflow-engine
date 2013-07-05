/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

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
}
