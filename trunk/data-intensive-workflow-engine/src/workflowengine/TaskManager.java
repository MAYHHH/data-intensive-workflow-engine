/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import workflowengine.communication.FileTransferException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import workflowengine.communication.Communicable;
import workflowengine.communication.HostAddress;
import workflowengine.communication.Message;
import workflowengine.utils.DBRecord;
import workflowengine.resource.ExecSite;
import workflowengine.resource.Worker;
import workflowengine.schedule.RandomScheduler;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.Scheduler;
import workflowengine.schedule.SchedulerSettings;
import workflowengine.utils.DBException;
import workflowengine.utils.Logger;
import workflowengine.utils.Utils;
import workflowengine.workflow.Workflow;
import workflowengine.workflow.Task;
import workflowengine.workflow.WorkflowFile;

/**
 * Task manager of the execution site to manage running of tasks in each worker.
 *
 * @author Orachun
 */
public class TaskManager
{

    public static final Logger logger = new Logger("task-manager.log");
    private static final Scheduler sch = new RandomScheduler();
    private static TaskManager tm = null;
    private HostAddress addr;
    private HostAddress nearestEsp;
    private Properties p = new Properties();
    private int suspendCount = -1;
    private int workerCount = -1;
    private int dispatchCheckInterval = 15000;
    private Communicable comm = new Communicable("Task Manager")
    {
        @Override
        public void handleMessage(Message msg)
        {
//            logger.log("Received msg from "+msg.getParam(Message.PARAM_FROM));
//            logger.log(msg.toString());
            switch (msg.getType())
            {
                case Message.TYPE_UPDATE_TASK_STATUS:
                    updateTaskStatus(msg);
                    break;
                case Message.TYPE_UPDATE_NODE_STATUS:
                    if(msg.getBooleanParam(Message.PARAM_NEED_RESPONSE))
                    {
                        updateNodeStatus(msg);
                    }
                    else
                    {
                        updateNodeStatuses(msg);
                    }
                    break;
                case Message.TYPE_SUBMIT_WORKFLOW:
                    submitWorkflow(msg);
                    break;
                case Message.TYPE_SUSPEND_TASK_COMPLETE:
                    suspendComplete();
                    break;
                case Message.TYPE_REGISTER_FILE:
                    registerFileInProxy(msg);
                    break;
            }
        }
    };

    private TaskManager() throws IOException
    {
        addr = new HostAddress(Utils.getPROP(), "task_manager_host", "task_manager_port");
        nearestEsp = new HostAddress(Utils.getPROP(), "nearest_esp_host", "nearest_esp_port");
        comm.setTemplateMsgParam(Message.PARAM_FROM_SOURCE, Message.SOURCE_TASK_MANAGER);
        comm.setLocalPort(addr.getPort());
        comm.startServer();
//        startDispatchThread();
    }

    public static TaskManager startService()
    {
        try
        {
            if (tm == null)
            {
                tm = new TaskManager();
            }
            logger.log("Task manager is started.");
            return tm;
        }
        catch (IOException ex)
        {
            logger.log("Cannot start task manager: " + ex.getLocalizedMessage());
            return null;
        }
    }
    
    private void startDispatchThread()
    {
        new Thread(new Runnable() {

            @Override
            public void run()
            {
                while (true)
                {
                    dispatchTask();
                    try
                    {
                        Thread.sleep(dispatchCheckInterval);
                    }
                    catch (InterruptedException ex)
                    {
                        logger.log("Cannot sleep for dispatching task.", ex);
                    }
                }
            }
        }).start();
    }

    public void updateNodeStatus(Message msg)
    {
        HostAddress espAddr = msg.getAddressParam(Message.PARAM_ESP_ADDRESS);
        Worker.updateWorkerStatus(
                espAddr,
                msg.getAddressParam(Message.PARAM_WORKER_ADDRESS),
                msg.getIntParam("current_tid"),
                msg.getDoubleParam("free_memory"),
                msg.getDoubleParam("free_space"),
                msg.getDoubleParam("cpu"),
                msg.getParam(Message.PARAM_WORKER_UUID));
        if(msg.getBooleanParam(Message.PARAM_NEED_RESPONSE))
        {
            try
            {
                logger.log("Sending response message to worker "+msg.getAddressParam(Message.PARAM_WORKER_ADDRESS));
                Message response = new Message(Message.TYPE_RESPONSE_TO_WORKER);
                response.setParamFromMsg(msg, Message.PARAM_WORKER_UUID);
                comm.sendResponseMsg(espAddr, msg, response);
                logger.log("Done.");
            }
            catch (IOException ex)
            {
                logger.log("Cannot send response message.", ex);
            }
        }
    }
    public void updateNodeStatuses(Message m)
    {
        Message[] msgs = (Message[])m.getObjectParam(Message.PARAM_WORKER_MSGS);
        for(Message msg : msgs)
        {
            updateNodeStatus(msg);
        }
        //TODO:Support down worker by reschedule, etc.
    }

    public void updateTaskStatus(Message msg)
    {
        Task.updateTaskStatus(
                msg.getIntParam("tid"),
                msg.getIntParam("start"),
                msg.getIntParam("end"),
                msg.getIntParam("exit_value"),
                msg.getCharParam("status"));
        char status = msg.getCharParam("status");
        if (status == Task.STATUS_COMPLETED || status == Task.STATUS_FAIL)
        {
            if (isRescheduleNeeded())
            {
                reschedule(msg.getIntParam("wfid"));
            }
            dispatchTask();
        }
    }

    public boolean isRescheduleNeeded()
    {
        //TODO: implement this
        return false;
    }

    public void reschedule(int wfid)
    {
        //TODO: implement this
        Workflow wf = Workflow.open(Utils.getProp("working_dir") + wfid + "/" + wfid + ".wfobj");
        List<DBRecord> res = DBRecord.select(
                "SELECT s.tid, w.uuid "
                + "FROM workflow_task t JOIN schedule s ON t.tid = s.tid "
                + "JOIN worker w on w.wkid = s.wkid "
                + "WHERE status = 'C' AND t.wfid='" + wfid + "'");
        HashMap<Task, Worker> fixed = new HashMap<>(res.size());
        for (DBRecord r : res)
        {
            Task t = Task.getWorkflowTaskFromDB(r.getInt("tid"));
            Worker w = Worker.getWorkerFromDB(r.get("uuid"));
            fixed.put(t, w);
        }
        Schedule schedule = sch.getSchedule(new SchedulerSettings(wf, getExecSite(), fixed));

        updateScheduleForWorkflow(wf, schedule);

        broadcastToWorkers(new Message(Message.TYPE_SUSPEND_TASK));
        //Will call dispatchTask automatically after all workers suspent 

    }

    public void submitWorkflow(Message msg)
    {
        logger.log("Workflow " + msg.getParam("dax_file") + " is submitted.");
        Workflow w = Workflow.fromDAX(msg.getParam("dax_file"));
        String inputFileDir = msg.getParam("input_dir");
        String remoteInputFileDir = Utils.getProp("exec_site_file_storage_dir") + w.getWorkingDirSuffix();
        try
        {
//            SFTPUtils.getSFTP(nearestEsp.getHost()).sendFolder(inputFileDir, remoteInputFileDir, null);
            comm.sendFilesInDir(nearestEsp.getHost(), nearestEsp.getPort(), remoteInputFileDir, inputFileDir);
        }
        catch (FileTransferException ex)
        {
            logger.log("Cannot upload input files for " + w.getName() + ": " + ex.getMessage());
            return;
        }
        List<WorkflowFile> files = w.getInputFiles();
        for (WorkflowFile f : files)
        {
            insertFileToEsp(f.getDbid(), nearestEsp);
        }
        execWorkflow(w);
    }

    private void insertFileToEsp(int fid, HostAddress espAddr)
    {
        int esid = new DBRecord("exec_site",
                "hostname", espAddr.getHost(),
                "port", espAddr.getPort()).insertIfNotExist();
        new DBRecord("exec_site_file",
                "esid", esid,
                "fid", fid).insert();
    }
    private void insertFileToEsp(int fid, String espHost)
    {
        int esid = new DBRecord("exec_site",
                "hostname", espHost
                ).insertIfNotExist();
        new DBRecord("exec_site_file",
                "esid", esid,
                "fid", fid).insert();
    }
    

    public void execWorkflow(Workflow wf) throws DBException
    {
        logger.log("Scheduling the workflow " + wf.getName() + ".");
        Schedule schedule;
        
        //Synchronized to shedule one workflow at a time
        synchronized (this)
        {
            schedule = sch.getSchedule(new SchedulerSettings(wf, getExecSite()));
        }
        
        setScheduleForWorkflow(wf, schedule);
        logger.log("Workflow " + wf.getName() + " is scheduled.");
        dispatchTask();
    }

    public void updateScheduleForWorkflow(Workflow wf, Schedule schedule) throws DBException
    {
        for (Task t : wf.getTaskIterator())
        {
            Worker w = schedule.getWorkerForTask(t);
            DBRecord.update("UPDATE schedule SET wkid='" + w.getDbid() + "' WHERE tid='" + t.getDbid() + "'");
        }
    }

    public void setScheduleForWorkflow(Workflow wf, Schedule schedule) throws DBException
    {
        for (Task t : wf.getTaskIterator())
        {
            Worker w = schedule.getWorkerForTask(t);
            new DBRecord("schedule", "wkid", w.getDbid(), "tid", t.getDbid()).insert();
        }
    }

    /**
     * Dispatch any ready tasks
     */
    synchronized public void dispatchTask()
    {
        List<DBRecord> results = DBRecord.select("_task_to_dispatch", new DBRecord());
        for (DBRecord res : results)
        {
            final DBRecord r = res;
            final Task t = Task.getWorkflowTaskFromDB(r.getInt("tid"));
            final Message msg = new Message(Message.TYPE_DISPATCH_TASK);
            String dir = Utils.getProp("exec_site_file_storage_dir") + t.getWorkingDirSuffix();
            msg.setParam("cmd", t.getCmd());
            msg.setParam("task_name", t.getName());
            msg.setParam("task_namespace", t.getNamespace());
            msg.setParam("tid", t.getDbid());
            msg.setParam("wfid", t.getWfdbid());
            msg.setParam(Message.PARAM_WORKER_UUID, r.get("uuid"));
            msg.setParam("input_files", t.getInputFiles());
            msg.setParam("file_dir", dir);
            msg.setParam("output_files", t.getOutputFiles());
            if (t.getStatus() == 'S')
            {
                msg.setParam("migrate", "migrate");
            }

            /**
             * Start new thread for each task dispatching to transfer input files
             * parallelly.
             */
            new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        logger.log("Uploading input files for task " + t.getName() + " to " + r.get("uuid") + "...");
                        boolean isUploadComplete = uploadInputFilesForTaskToEsp(t, r.get("esp_hostname"));
                        if(isUploadComplete)
                        {
                            logger.log("Done.");
                            logger.log("Dispatching task " + t.getName() + " to " + r.get("esp_hostname")+":"+r.get("port") + "...");
                            comm.sendMessage(r.get("esp_hostname"), r.getInt("esp_port"), msg);
                            logger.log("Done.");
                            Task.updateTaskStatus(t.getDbid(), -1, -1, -1, Task.STATUS_DISPATCHED);
                        }
                        else
                        {
                            logger.log("Fail uploading file.");
                        }
                    }
                    catch (IOException | FileTransferException ex)
                    {
                        logger.log("Cannot send execution request of " + t.getName() + " to " + r.get("hostname") + ": " + ex.getMessage());
                    }
                }
            }).start();
        }
    }

    /**
     * Upload required input files for the given task from any proxy to the
     * given espHost name. This method will block until all transfers are
     * stopped.
     *
     * @param t task
     * @param espHost execution site proxy hostname
     * @throws FileTransferException
     */
    private boolean uploadInputFilesForTaskToEsp(Task t, String espHost) throws FileTransferException
    {
        //Select input files needed by the task t
        List<DBRecord> files = DBRecord.select(
                "SELECT f.fid, f.name "
                + "FROM workflow_task_file tf join `file` f on tf.fid = f.fid "
                + "WHERE `type`='I' AND tid='" + t.getDbid() + "'");
//        SFTPClient sftp = SFTPUtils.getSFTP(espHost);
        String dir = Utils.getProp("exec_site_file_storage_dir") + t.getWorkingDirSuffix();
        LinkedList<Message> sentMsgs = new LinkedList<>();
        for (DBRecord f : files)
        {
            String fname = f.get("name");
            //Whether the file is in the espHost
            boolean fileExist = DBRecord.select("SELECT fid "
                    + "FROM exec_site_file esf "
                    + "JOIN exec_site es ON esf.esid = es.esid "
                    + "WHERE es.hostname='"+espHost+"' "
                    + "AND fid = '"+f.get("fid")+"'").size() > 0;
            if (!fileExist)
            {
                try
                {
                    //Select the execution site that contain the file f
                    DBRecord r = DBRecord.select("SELECT hostname, port "
                            + "FROM exec_site_file esf JOIN exec_site es ON esf.esid = es.esid "
                            + "WHERE esf.fid = '" + f.get("fid") + "'").get(0);
                    
                    //Send message to that execution site to transfer required files
                    //to espHpst
                    Message msg = new Message(Message.TYPE_FILE_UPLOAD_REQUEST);
                    msg.setParam("filename", fname);
                    msg.setParam("dir", dir);
                    msg.setParam("upload_to", espHost);
                    msg.setParam("fid", f.get("fid"));
                    logger.log("Request file "+fname+" from "+r.get("hostname")+ " to "+espHost);
                    comm.sendForResponseAsync(r.get("hostname"), r.getInt("port"), addr.getPort(), msg);
                    logger.log("Done");
                    sentMsgs.add(msg);
                }
                catch (IndexOutOfBoundsException ex)
                {
                    throw new FileTransferException("No execution site containing the file "+fname+".");
                }
                catch (IOException ex)
                {
                    throw new FileTransferException("Cannot request input file " + fname + ": " + ex.getMessage());
                }
            }
        }

        boolean complete = true;
        for (Message msg : sentMsgs)
        {
            Message res = comm.getResponseMessage(msg);
            if(!res.getBooleanParam("upload_complete"))
            {
                complete = false;
            }
            else
            {
                insertFileToEsp(msg.getIntParam("fid"), msg.getParam("upload_to"));
            }
        }
        return complete;
    }

    public ExecSite getExecSite() throws DBException
    {
        List<DBRecord> workers = DBRecord.select("worker", "SELECT uuid FROM worker");
        ExecSite es = new ExecSite();
        for (DBRecord w : workers)
        {
            es.addWorker(Worker.getWorkerFromDB(w.get("uuid")));
        }
        return es;
    }

    public void broadcastToWorkers(Message msg)
    {
        List<DBRecord> workers = DBRecord.select("worker", "select hostname, port from worker");
        for (DBRecord w : workers)
        {
            try
            {
                comm.sendMessage(w.get("hostname"), w.getInt("port"), msg);
            }
            catch (IOException ex)
            {
                logger.log("Cannot broadcast message to " + w.get("hostname") + ": " + ex.getMessage());
            }
        }
    }

    public void suspendComplete()
    {
        if (suspendCount == -1)
        {
            suspendCount = 0;
            workerCount = DBRecord.select("worker", "select count(hostname) as count from worker").get(0).getInt("count");
        }
        suspendCount++;
        if (suspendCount == workerCount)
        {
            dispatchTask();
        }
    }
    
    public void registerFileInProxy(Message msg)
    {
        WorkflowFile[] wfiles = (WorkflowFile[])msg.getObjectParam("files");
        HostAddress esp = msg.getAddressParam(Message.PARAM_ESP_ADDRESS);
        for(WorkflowFile f : wfiles)
        {
            insertFileToEsp(f.getDbid(), esp);
        }
    }
}
