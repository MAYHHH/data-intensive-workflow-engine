/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import workflowengine.utils.Utils;

/**
 *
 * @author Orachun
 */
public class WorkflowEngine
{

    public static void main(String[] args) throws ClassNotFoundException
    {
        if (args.length == 0)
        {
            System.err.println("Please specify server type (TaskExecutor, TaskManager, ExecutionSiteProxy).");
            System.exit(1);
        }
        switch (args[0])
        {
            case "TaskExecutor":
                System.err.println("Starting task executor ...");
                TaskExecutor taskExecutor = TaskExecutor.startService();
                if (taskExecutor != null)
                {
                    System.err.println("Task executor started.");
                }
                else
                {
                    System.err.println("Cannot start task executor.");
                }
                break;
            case "TaskManager":
                System.err.println("Starting task manager ...");
                TaskManager taskManager = TaskManager.startService();
                if (taskManager != null)
                {
                    System.err.println("Task manager started.");
                }
                else
                {
                    System.err.println("Cannot start task manager.");
                }
                break;
            case "ExecutionSiteProxy":
                System.err.println("Starting execution site proxy ...");
                ExecutionSiteProxy esp = ExecutionSiteProxy.startService();
                if (esp != null)
                {
                    System.err.println("Execution site proxy started.");
                }
                else
                {
                    System.err.println("Cannot start task executor.");
                }
                break;
        }
    }
}
