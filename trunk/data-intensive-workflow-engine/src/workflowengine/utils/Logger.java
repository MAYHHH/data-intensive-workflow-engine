/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Date;

/**
 *
 * @author udomo
 */
public class Logger
{
    private PrintWriter pw;
    private OutputStream os;
    public Logger(String filename)
    {
        try
        {
            os = new FileOutputStream(filename, true);
            pw = new PrintWriter(os);
        }
        catch(FileNotFoundException ex)
        {
            System.err.println("Can't initialize logger. Use STDERR instead.");
            pw = new PrintWriter(System.err);
        }
    }
    
    public void log(String msg)
    {
        pw.print(new Date().toString());
        pw.print(": ");
        pw.println(msg);
        pw.flush();
    }
    
    public void log(String msg, Exception ex)
    {
        log(msg);
        ex.printStackTrace(pw);
        pw.flush();
    }    
}
