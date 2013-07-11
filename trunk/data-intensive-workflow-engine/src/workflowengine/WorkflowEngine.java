/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 *
 * @author Orachun
 */
public class WorkflowEngine
{
    //Global Vars
    public static final Properties PROP = new Properties();
    
    private static final String CONFIG_FILE = "default.properties";
  
    static 
    {
        init();
    }
    
    public static void init()
    {
        try
        {
            InputStreamReader is = new FileReader(CONFIG_FILE);
            PROP.load(is);
            is.close();
        }
        catch (IOException ex)
        {
            System.err.println("Cannot read the configuration file " + CONFIG_FILE + ".");
            System.exit(2);
        }
    }
    
    
    public static void main(String[] args)
    {
        if(args.length == 0)
        {
            System.err.println("Please specify server type (TaskExecutor, TaskManager, ExecutionSiteProxy).");
            System.exit(1);
        }
        WorkflowEngine we = new WorkflowEngine();
        we.init();
        switch (args[0])
        {
            case "TaskExecutor":
                System.err.println("Starting task executor ...");
                TaskExecutor taskExecutor = TaskExecutor.start();
                System.err.println("Task executor started.");
                break;
            case "TaskManager":
                System.err.println("Starting task manager ...");
                TaskManager taskManager = TaskManager.start();
                System.err.println("Task manager started.");
                break;
            case "ExecutionSiteProxy":
                System.err.println("Starting execution site proxy ...");
                ExecutionSiteProxy esp = ExecutionSiteProxy.start();
                System.err.println("Execution site proxy started.");
                break;
        }
    }
}
