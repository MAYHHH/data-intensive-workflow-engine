/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import com.zehon.exception.FileTransferException;
import com.zehon.sftp.SFTPClient;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import workflowengine.communication.Communicable;
import workflowengine.communication.HostAddress;
import workflowengine.communication.Message;
import workflowengine.utils.Logger;
import workflowengine.utils.SFTPUtils;
import workflowengine.utils.Utils;
import workflowengine.workflow.Task;
import workflowengine.workflow.WorkflowFile;


/**
 *
 * @author Orachun
 */
public class TaskExecutor
{
    public static final short STATUS_IDLE = 1;
    public static final short STATUS_BUSY = 2;
    private static final int HEARTBEAT_INTERVAL = 10000; //10 seconds
    private static Logger logger = new Logger("task-executor.log");
    
    private static TaskExecutor te = null;
    
    private Properties p = new Properties();
    private short status = STATUS_IDLE;
    private String currentTaskName;
    private String currentProcName;
    private String currentWorkingDir;
    private int currentTaskDbid = -1;
    private long currentTaskStart;
    private long currentTaskEnd;
    private Message currentRequestMsg;
    private boolean isSuspensed = false;
    private String uuid;
    
    private HostAddress espAddr;
    private int port;
    private double cpu = -1;
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
                    updateNodeStatus();
                    break;
                case Message.TYPE_SUSPEND_TASK:
                    suspendCurrentTask();
                    break;
            }
        }
    };
    
    private TaskExecutor() throws IOException
    {
        espAddr = new HostAddress(Utils.getPROP(), "exec_site_proxy_host", "exec_site_proxy_port");
        port = (Utils.getIntProp("task_executor_port"));
        uuid = Utils.uuid();
        comm.setLocalPort(port);
        comm.setTemplateMsgParam("uuid", uuid);
        comm.startServer();
        startHeartBeat();
    }
    
    public static TaskExecutor startService()
    {
        try
        {
            if (te == null)
            {
                te = new TaskExecutor();
            }
            logger.log("Task executor is started.");
            return te;
        }
        catch (IOException ex)
        {
            logger.log("Cannot start task executor: " + ex.getLocalizedMessage());
            return null;
        }
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
                        Thread.sleep(HEARTBEAT_INTERVAL);
                    }
                    catch (InterruptedException ex) {}
                }
            }
        });
        heartBeat.start();
    }
   
    public void updateNodeStatus()
    {
        Message response = new Message(Message.TYPE_UPDATE_NODE_STATUS);
        response.setParam("status", status);
        File defaultStorage = new File(Utils.getProp("working_dir"));
        response.setParam("free_space", defaultStorage.getFreeSpace()); //in bytes
        response.setParam("current_tid", currentTaskDbid);
        response.setParam("free_memory", getFreeMemory());
        response.setParam("cpu", getCPU());
        response.setParam("port", this.port);
        try
        {
            comm.sendMessage(espAddr, response);
        }
        catch (IOException ex){}
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
    
    
    private void setIdle()
    {
        currentTaskName = "";
        currentTaskDbid = -1;
        currentProcName = "";
        currentWorkingDir = "";
        status = STATUS_IDLE;
        currentTaskStart = -1;
        currentTaskEnd = -1;
        currentRequestMsg = null;
    }
    
    
    private String[] prepareCmd(Message msg)
    {
        String cmdPrefix = Utils.getProp("command_prefix");
        StringBuilder cmd = new StringBuilder();
        cmd.append(cmdPrefix).append(currentWorkingDir).append(msg.getParam("cmd"));
        if(msg.getParam("migrate") != null)
        {
            cmd.append(";-_condor_restart;").append(currentTaskName).append(".ckpt");
        }
        return cmd.toString().split(";");
    }
    
    /**
     * Download input files using information of execution message
     * @param msg 
     */
    private void downloadInputFiles(Message msg) throws FileTransferException
    {
        WorkflowFile[] files = (WorkflowFile[])msg.getObjectParam("input_files");
        String remoteDir = msg.getParam("file_dir");
        String espHost = Utils.getProp("exec_site_proxy_host");
        SFTPClient client = SFTPUtils.getSFTP(espHost);
        for(WorkflowFile f : files)
        {
            String localFilePath = currentWorkingDir+f.getName();
            if(f.getType() == WorkflowFile.TYPE_DIRECTIORY)
            {
                client.getFolder(remoteDir+f.getName(), localFilePath, null);
            }
            else
            {
                client.getFile(f.getName(), remoteDir, currentWorkingDir);
            }
        }
    }
    private void uploadOutputFiles(Message msg) throws FileTransferException
    {
        WorkflowFile[] files = (WorkflowFile[])msg.getObjectParam("output_files");
        String remoteDir = msg.getParam("file_dir");
        String espHost = Utils.getProp("exec_site_proxy_host");
        SFTPClient client = SFTPUtils.getSFTP(espHost);
        for(WorkflowFile f : files)
        {
            String localFilePath = currentWorkingDir+f.getName();
            if(new File(localFilePath).isDirectory())
            {
                client.sendFolder(localFilePath, remoteDir, null);
            }
            else
            {
                client.sendFile(localFilePath, remoteDir);
            }
        }
    }
    
    private void downloadExecutable(Message msg) throws FileTransferException
    {
        String namespace = msg.getParam("namespace");
        if(!Utils.isFileExist(currentWorkingDir+namespace+"/"))
        {
            long time = System.currentTimeMillis();
            SFTPUtils.getSFTP(Utils.getProp("code_repository_host"))
                .getFolder(Utils.getProp("code_repository_dir")+namespace+"/", currentWorkingDir, null);
            Utils.setExecutableInDirSince(currentWorkingDir, time);
        }
        
        
//        String execName = msg.getParam("cmd").split(";")[0];
//        SFTPUtils.getSFTP(Utils.getProp("code_repository_host"))
//                .getFile(execName, Utils.getProp("code_repository_dir"), currentWorkingDir);
//        new File(currentWorkingDir+execName).setExecutable(true);
    }
    
    private Process startProcess(String[] cmds) throws IOException
    {
        ProcessBuilder pb = new ProcessBuilder(cmds).directory(new File(
                currentWorkingDir));
        pb.redirectError(new File(currentWorkingDir + currentTaskName + ".stderr"));
        pb.redirectOutput(new File(currentWorkingDir + currentTaskName + ".stdout"));

        currentTaskStart = Utils.time();
        logger.log("Starting execution of task "+currentTaskName+".");
        return pb.start();
    }
    
    private void prepareDirectory(Message msg)
    {
        WorkflowFile[] inputs = (WorkflowFile[])msg.getObjectParam("input_files");
        WorkflowFile[] outputs = (WorkflowFile[])msg.getObjectParam("output_files");
        for(WorkflowFile f : inputs)
        {
            if(f.getType() == WorkflowFile.TYPE_DIRECTIORY)
            {
                new File(currentWorkingDir+f.getName()).mkdirs();
            }
        }
        for(WorkflowFile f : outputs)
        {
            if(f.getType() == WorkflowFile.TYPE_DIRECTIORY)
            {
                new File(currentWorkingDir+f.getName()).mkdirs();
            }
        }
    }
    
    private String[] prepareExecutionAndGetCommands(Message msg) throws FileTransferException
    {
        currentWorkingDir = Utils.getProp("working_dir")+msg.getParam("wfid")+"/";
        currentRequestMsg = msg;
        currentTaskName = msg.getParam("task_name");
        String[] cmds = prepareCmd(msg);
        currentProcName = cmds[0];
        currentTaskDbid = msg.getIntParam("tid");
        prepareDirectory(msg);
        downloadInputFiles(msg);
        downloadExecutable(msg);
        return cmds;
    }
    
    synchronized public void exec(Message msg)
    {
        if(status == STATUS_BUSY)
        {
            return;
        }
        try
        {
            isSuspensed = false;
            status = STATUS_BUSY;
            String[] cmds = prepareExecutionAndGetCommands(msg);
            Process proc = startProcess(cmds);
            
            //Send task status to manager that the task is started
            Message response = new Message(Message.TYPE_UPDATE_TASK_STATUS);
            response.setParam("task_name", currentTaskName);
            response.setParam("tid", currentTaskDbid);
            response.setParam("wfid", msg.getParam("wfid"));
            response.setParam("status", Task.STATUS_EXECUTING);
            response.setParam("start", currentTaskStart);
            response.setParam("end", -1);
            response.setParam("exit_value", -1);
            comm.sendMessage(espAddr, response);
            
            //Wait for the process to complete
            int exitVal = proc.waitFor();
            
            currentTaskEnd = Utils.time();
            
            if(isSuspensed)
            {
                setIdle();
                return;
            }
            
            //Send task status to manager that the task is finished
            response = new Message(Message.TYPE_UPDATE_TASK_STATUS);
            response.setParam("task_name", currentTaskName);
            response.setParam("tid", currentTaskDbid);
            response.setParam("wfid", msg.getParam("wfid"));
            if(exitVal == 0)
            {
                response.setParam("status", Task.STATUS_COMPLETED);
            }
            else
            {
                response.setParam("status", Task.STATUS_FAIL);
            }
            response.setParam("start", currentTaskStart);
            response.setParam("end", currentTaskEnd);
            response.setParam("exit_value", exitVal);
            logger.log("Execution of task "+currentTaskName+" is finished.");
            
            uploadOutputFiles(msg);
            
            setIdle();
            updateNodeStatus();
            comm.sendMessage(espAddr, response);
            
        }
        catch (IOException | InterruptedException | FileTransferException ex) 
        {
            currentTaskEnd = Utils.time();
            String errorMsg = "Exception while "+currentTaskName+" is executing: "+ex.getMessage();
            logger.log(errorMsg, ex);
            Message response = new Message(Message.TYPE_UPDATE_TASK_STATUS);
            response.setParam("task_name", currentTaskName);
            response.setParam("tid", currentTaskDbid);
            response.setParam("wfid", msg.getParam("wfid"));
            response.setParam("status", Task.STATUS_FAIL);
            response.setParam("start", currentTaskStart);
            response.setParam("end", currentTaskEnd);
            response.setParam("exit_value", -1);
            response.setParam("error_msg", errorMsg);
            try
            {
                comm.sendMessage(espAddr, response);
            }
            catch (IOException ex1)
            {
                logger.log("Cannot send message to "+espAddr+".", ex1);
            }
        }
    }
    
    synchronized public void suspendCurrentTask()
    {
        if(currentTaskName.length() == 0)
        {
            return;
        }
        try
        {
            
            Runtime.getRuntime().exec(new String[]{
                "/bin/bash", "-c", "killall -s SIGTSTP "+currentProcName
            }).waitFor();
            isSuspensed = true;
            Runtime.getRuntime().exec(new String[]{
                "/bin/bash", "-c", "mv "+currentWorkingDir+currentProcName+".ckpt "+
                    currentWorkingDir+currentTaskName+".ckpt"
            }).waitFor();
            
            Message response = new Message(Message.TYPE_UPDATE_TASK_STATUS);
            response.setParam("task_name", currentTaskName);
            response.setParam("tid", currentTaskDbid);
            response.setParam("wfid", currentRequestMsg.getParam("wfid"));
            response.setParam("status", Task.STATUS_SUSPENDED);
            response.setParam("start", currentTaskStart);
            response.setParam("end", currentTaskEnd);
            response.setParam("exit_value", -1);
            comm.sendMessage(espAddr, response);
            comm.sendMessage(espAddr, new Message(Message.TYPE_SUSPEND_TASK_COMPLETE));
        }
        catch (IOException | InterruptedException ex)
        {
            logger.log("Cannot suspend process: "+ ex.getMessage());
        }
    }
    
}
