/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Properties;
import workflowengine.communication.Communicable;
import workflowengine.communication.Message;
import workflowengine.utils.Logger;
import workflowengine.utils.Utils;


/**
 *
 * @author Orachun
 */
public class TaskExecutor
{
    public static final short STATUS_IDLE = 1;
    public static final short STATUS_BUSY = 2;
    private static Logger logger = new Logger("task-executor.log");
    
    private static TaskExecutor te = null;
    
    private Properties p = new Properties();
    private short status = STATUS_IDLE;
//    private String nodeName;
    private String currentTask;
    private int currentTaskDbid = -1;
    private String managerHost;
    private int managerPort;
    private double cpu = -1;
    private HashMap<String, Long> startTime = new HashMap<>();
    private HashMap<String, Long> endTime = new HashMap<>();
    private Communicable comm = new Communicable(){
        @Override
        public void handleMessage(Message msg)
        {
            switch(msg.getType())
            {
                case Message.TYPE_DISPATCH_TASK:
                    exec(msg);
                    break;
                case Message.TYPE_GET_NODE_STATUS:
                    try
                    {
                        updateNodeStatus();
                    }
                    catch (IOException ex) {}
                    break;
                case Message.TYPE_MIGRATE_TASK:
                    break;
            }
        }
    };
    
    private TaskExecutor()
    {
        managerHost = WorkflowEngine.PROP.getProperty("task_manager_host");
        managerPort = Integer.parseInt(WorkflowEngine.PROP.getProperty("task_manager_port"));
        comm.startServer(Integer.parseInt(WorkflowEngine.PROP.getProperty("task_executor_port")));
        startHeartBeat();
    }
    
    public static TaskExecutor start()
    {
        if(te == null)
        {
            te = new TaskExecutor();
        }
        logger.log("Task executor is started.");
        return te;
    }
    
    private void startHeartBeat()
    {
        Thread heartBeat = new Thread(new Runnable() {

            @Override
            public void run()
            {
                while (true)
                {
                    try
                    {
                        updateNodeStatus();
                        Thread.sleep(5000);
                    }
                    catch (InterruptedException ex) {}
                    catch (IOException ex) {}
                }
            }
        });
        heartBeat.start();
    }
   
    public void updateNodeStatus() throws IOException
    {
        Message response = new Message(Message.TYPE_UPDATE_NODE_STATUS);
//        response.setParam("name", nodeName);
        response.setParam("status", status);
        File defaultStorage = new File(WorkflowEngine.PROP.getProperty("working_dir"));
        response.setParam("free_space", defaultStorage.getFreeSpace()); //in bytes
        response.setParam("current_tid", currentTaskDbid);
        response.setParam("free_memory", getFreeMemory());
        response.setParam("cpu", getCPU());
        comm.sendMessage(managerHost, managerPort, response);
    }
    
    public double getFreeMemory()
    {
        try
        {
            Process p = Runtime.getRuntime().exec(new String[]
            {
                "/bin/bash", "-c", "grep \"MemFree:\" /proc/meminfo | awk '{print $2}'"
            });
            return Integer.parseInt(new BufferedReader(new InputStreamReader(p.getInputStream())).readLine());
        }
        catch (IOException ex)
        {
            return -1;
        }
    }
    public double getCPU()
    {
        if(cpu == -1)
        {
            try
            {
                Process p = Runtime.getRuntime().exec(new String[]
                {
                    "/bin/bash", "-c", "echo 0 $(lscpu | grep \"CPU MHz:\" | awk '{print $3}' |  sed 's#^#+#' ) | bc"
                });
                cpu = Double.parseDouble(new BufferedReader(new InputStreamReader(p.getInputStream())).readLine());
            }
            catch (IOException ex)
            {
                cpu = -1;
            }
        }
        return cpu;
    }
    
    synchronized public void exec(Message msg)
    {
        if(status == STATUS_BUSY)
        {
            return;
        }
        long start = 0;
        try
        {
            currentTask = msg.getParam("task_name");
            logger.log("Starting execution of task "+currentTask+".");
            status = STATUS_BUSY;
            String cmdPrefix = WorkflowEngine.PROP.getProperty("command_prefix");
            String cmdSuffix = WorkflowEngine.PROP.getProperty("command_suffix");
            String[] cmd = (cmdPrefix+msg.getParam("cmd")+cmdSuffix).split(";");
            currentTaskDbid = msg.getIntParam("tid");
            start = Utils.time();
            startTime.put(currentTask, start);
            
//            System.err.println("Start executing "+currentTask+" ...");
            ProcessBuilder pb = new ProcessBuilder(cmd).directory(new File(
                    WorkflowEngine.PROP.getProperty("working_dir") +
                    msg.getParam("wfid")
                    ));
            Process proc = pb.start();
            
//            Process proc = Runtime.getRuntime().exec(cmd);
            
            Message response = new Message(Message.TYPE_UPDATE_TASK_STATUS);
            response.setParam("task_name", msg.getParam("task_name"));
            response.setParam("tid", msg.getParam("tid"));
            response.setParam("wfid", msg.getParam("wfid"));
//            response.setParam("wfid", msg.getParam("wfid"));
            response.setParam("start", start);
            response.setParam("end", -1);
            response.setParam("exit_value", -1);
            comm.sendMessage(managerHost, managerPort, response);
            
            int exitVal = proc.waitFor();
            long end = Utils.time();
            endTime.put(currentTask, end);
            currentTask = "";
            currentTaskDbid = -1;
            status = STATUS_IDLE;
            response = new Message(Message.TYPE_UPDATE_TASK_STATUS);
            response.setParam("task_name", msg.getParam("task_name"));
            response.setParam("tid", msg.getParam("tid"));
            response.setParam("wfid", msg.getParam("wfid"));
//            response.setParam("wfid", msg.getParam("wfid"));
            response.setParam("start", start);
            response.setParam("end", end);
            response.setParam("exit_value", exitVal);
            logger.log("Execution of task "+currentTask+" is finished.");
            
//            System.err.println("Finish executing "+msg.getParam("task_name")+".");
            updateNodeStatus();
            comm.sendMessage(managerHost, managerPort, response);
            
        }
        catch (Exception ex) 
        {
            long end = Utils.time();
            logger.log("Exception while "+currentTask+" is executing: "+ex.getMessage());
            Message response = new Message(Message.TYPE_UPDATE_TASK_STATUS);
            response.setParam("task_name", msg.getParam("task_name"));
            response.setParam("tid", msg.getParam("tid"));
            response.setParam("start", start);
            response.setParam("end", end);
            response.setParam("exit_value", 100);
            try
            {
                comm.sendMessage(managerHost, managerPort, response);
            }
            catch (IOException ex1)
            {
                logger.log("Cannot send message to "+managerHost+".");
            }
        }
    }
    synchronized public void migrate(String targetHost)
    {
        
    }
    synchronized public void receiveMigratedTask()
    {
        
    }
}
