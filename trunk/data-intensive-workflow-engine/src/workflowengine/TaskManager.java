/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.io.File;
import workflowengine.communication.FileTransferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import workflowengine.communication.Communicator;
import workflowengine.communication.HostAddress;
import workflowengine.communication.message.Message;
import workflowengine.utils.DBRecord;
import workflowengine.resource.ExecSite;
import workflowengine.resource.Worker;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.Scheduler;
import workflowengine.schedule.SchedulerSettings;
import workflowengine.schedule.fc.CostOptimizationFC;
import workflowengine.schedule.fc.FC;
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
public class TaskManager extends Service
{

    public static final Logger logger = Utils.getLogger();
    public static final int TASK_DELAY_THRESHOLD = 15;
    public static final int RESCHEDULING_INTERVAL = 1800;
//    private static Scheduler sch = null;
    private static TaskManager tm = null;
    private HostAddress nearestEsp;
    private Properties p = new Properties();
    private int suspendCount = -1;
    private int workerCount = -1;
    private int dispatchCheckInterval = 15000;
    private final Object DISPATCH_LOCK_OBJ = new Object();
    private int count = 0;
    private boolean rescheduling = false;
    private long lastReschedulingTime = 0;

    private TaskManager() throws IOException, ClassNotFoundException, 
            IllegalAccessException, InstantiationException
    {
        prepareComm();
        getScheduler();
        addr = new HostAddress(Utils.getPROP(), "task_manager_host", "task_manager_port");
        nearestEsp = new HostAddress(Utils.getPROP(), "nearest_esp_host", "nearest_esp_port");
        comm.setTemplateMsgParam(Message.PARAM_FROM_SOURCE, Message.SOURCE_TASK_MANAGER);
        comm.setListeningPort(addr.getPort());
        comm.startServer();
//        startDispatchThread();
    }
    
    private Scheduler getScheduler() 
    {
        try
        {
            Class c = ClassLoader.getSystemClassLoader().loadClass(Utils.getProp("scheduler").trim());
            Scheduler s = (Scheduler) c.newInstance();
            return s;
        }
        catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex)
        {
            java.util.logging.Logger.getLogger(TaskManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    protected void prepareComm()
    {
        comm = new Communicator("Task Manager")
        {
            @Override
            public void handleMessage(Message msg)
            {
//            logger.log("Received msg from "+msg.getParam(Message.PARAM_FROM));
//            logger.log(msg.toString());
                switch (msg.getType())
                {
                    case Message.TYPE_SUBMIT_WORKFLOW:
                        submitWorkflow(msg);
                        break;
                    case Message.TYPE_DISPATCH_TASK_REQUEST:
                        dispatchTaskRequest(msg);
                        break;
                    case Message.TYPE_SHUTDOWN:
                        System.exit(0);
                        break;
                    default:
                        throw new RuntimeException("The message type is not "
                                + "handled.");
                }
            }
        };
    }

    private void dispatchTaskRequest(Message msg)
    {
        if (isRescheduleNeeded(msg))
        {
            System.out.println("Rescheduling...");
            logger.log("Rescheduling ...");
            reschedule(msg.getInt("wfid"));
            logger.log("Done.");
            System.out.println("Done.");
        }
        dispatchTask();
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
        catch (IOException | ClassNotFoundException | IllegalAccessException | InstantiationException ex)
        {
            logger.log("Cannot start task manager: ", ex);
            return null;
        }
    }

    private void startDispatchThread()
    {
        new Thread(new Runnable()
        {
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

    /**
     * Check whether the current workflow should be rescheduled.
     * @param msg message contains info of the task that is just finished
     * @return true if the start time or finish time is later than estimated
     * for TASK_DELAY_THRESHOLD seconds
     */
    public boolean isRescheduleNeeded(Message msg)
    {
//        count++;
//        return count == 2;
        
//        //TODO: implement this
        if(Utils.time() - lastReschedulingTime < RESCHEDULING_INTERVAL)
        {
            return false;
        }
        
        int start = msg.getInt("start");
        int end = msg.getInt("end");
        int tid = msg.getInt("tid");
        DBRecord s = DBRecord.select("schedule", new DBRecord("schedule").set("tid", tid)).get(0);
        
        int est_start = s.getInt("estimated_start");
        int est_finish = s.getInt("estimated_finish");
        lastReschedulingTime = Utils.time();
        return (start - est_start > TASK_DELAY_THRESHOLD) || (end - est_finish > TASK_DELAY_THRESHOLD);
////        return false;
    }
    
    public void reschedule(int wfid)
    {
        //TODO: implement this
        rescheduling = true;
        Workflow wf = Workflow.open(Utils.getProp("working_dir") + wfid + "/" + wfid + ".wfobj");

        //Build fixed mapping of completed tasks
        List<DBRecord> res = DBRecord.select(
                "SELECT s.tid, w.uuid "
                + "FROM workflow_task t JOIN schedule s ON t.tid = s.tid "
                + "JOIN worker w on w.wkid = s.wkid "
                + "WHERE status = 'C' AND t.wfid='" + wfid + "'");
        HashMap<Task, Worker> fixedMapping = new HashMap<>(res.size());
        for (DBRecord r : res)
        {
            Task t = Task.getWorkflowTaskFromDB(r.getInt("tid"));
            Worker w = Worker.getWorkerFromDB(r.get("uuid"));
            fixedMapping.put(t, w);
        }

        //Calculate new schedule
        SchedulerSettings ss = new SchedulerSettings(wf, getExecSite(), fixedMapping);
        Schedule currentSch = (Schedule)Utils.readFromFile("schedule_wf"+wfid+".sch");
        ss.setParam("current_schedule", currentSch);
        Schedule newSchedule = getScheduler().getSchedule(ss);
        newSchedule.evaluate();
        
        int currentFinishTime = Workflow.getEstimatedFinishTime(wfid);
        int newFinishTime = (int)Math.round(newSchedule.getMakespan() + estimateMigrationTime());
        if (currentFinishTime > newFinishTime)
        {

            Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
            synchronized (DISPATCH_LOCK_OBJ)
            {
                //Update new schedule
                updateScheduleForWorkflow(wf, newSchedule, Utils.time());

                //Checkpoint and suspend all running tasks
                List<Message> msgs = broadcastToWorkers(new Message(Message.TYPE_SUSPEND_TASK), true);
                for (Message m : msgs)
                {
                    comm.getResponseMessage(m);
                }
                rescheduling = false;
                dispatchTask();
            }
        }
        else
        {
            rescheduling = false;
        }
    }
    
    public int estimateMigrationTime()
    {
        return 0;
    }

    public void submitWorkflow(Message msg)
    {
        String daxFile = msg.get("dax_file");
        logger.log("Workflow " + daxFile + " is submitted.");
        Properties p = (Properties)msg.getObject("properties");
        Utils.setProp(p);
        Workflow w;
        String inputFileDir = msg.get("input_dir");
        if(daxFile.endsWith("dummy"))
        {
            w = Workflow.fromDummyDAX(daxFile, false);
            logger.log("Creating dummy input files...");
            w.createDummyInputFiles(inputFileDir);
            logger.log("Done.");
        }
        else
        {
            w = Workflow.fromDAX(daxFile);
        }

        logger.log("Uploading input files...");
        String remoteInputFileDir = Utils.getProp("exec_site_file_storage_dir") + w.getWorkingDirSuffix();
        try
        {
            comm.sendFilesInDir(nearestEsp.getHost(), nearestEsp.getPort(), remoteInputFileDir, inputFileDir);
        }
        catch (FileTransferException ex)
        {
            logger.log("Cannot upload input files for " + w.getName() + ": " + ex.getMessage());
            return;
        }
        
        List<WorkflowFile> files = w.getInputFiles();
        for (File f : new File(inputFileDir).listFiles())
        {
            WorkflowFile wff = WorkflowFile.getFile(f.getName(), f.length()*Utils.BYTE, WorkflowFile.TYPE_FILE);
            registerFile(new WorkflowFile[]
            {
                wff
            }, nearestEsp, true);
            files.remove(wff);
        }

        
//        List<WorkflowFile> files = w.getInputFiles();
//        registerFile(files.toArray(new WorkflowFile[files.size()]), nearestEsp, true);
        
        
        logger.log("Done.");
        if(files.isEmpty())
        {
            execWorkflow(w);
        }
        else
        {
            logger.log("Cannot execute the submitted workflow: some input file is missing.");
        }
    }

    public void execWorkflow(Workflow wf) throws DBException
    {
        logger.log("Scheduling the workflow " + wf.getName() + ".");
        Schedule schedule;

        //Synchronized to shedule one workflow at a time
        long scheduledTime;
        synchronized (this)
        {
            Workflow.setStartedTime(wf.getDbid(), Utils.time());
            FC fc = new CostOptimizationFC(
                    Utils.getDoubleProp(CostOptimizationFC.PROP_DEADLINE), 
                    Utils.getDoubleProp(CostOptimizationFC.PROP_CONSTANT_PENALTY), 
                    Utils.getDoubleProp(CostOptimizationFC.PROP_WEIGHTED_PENALTY)
                    );
            schedule = getScheduler().getSchedule(new SchedulerSettings(wf, getExecSite(), fc));
            scheduledTime = Utils.time();
            Workflow.setScheduledTime(wf.getDbid(), scheduledTime);
            int estFinish = (int)Math.round(Utils.time()+schedule.getMakespan());
            Workflow.setEstimatedFinishTime(wf.getDbid(), estFinish);
        }
        schedule.evaluate();
        setScheduleForWorkflow(wf, schedule, scheduledTime);
        
        System.gc();
        
        Utils.writeToFile(schedule, "schedule_wf"+wf.getDbid()+".sch");
        logger.log("Workflow " + wf.getName() + " is scheduled.");
        dispatchTask();
    }

    public void updateScheduleForWorkflow(Workflow wf, Schedule schedule, long startTime) throws DBException
    {
        SchedulerSettings ss = schedule.getSettings();
        for (Task t : wf.getTaskIterator())
        {
            if(!ss.isFixedTask(t))
            {
                Worker w = schedule.getWorkerForTask(t);
                DBRecord.update("UPDATE schedule "
                        + "SET wkid='" + w.getDbid() + "', "
                        + "estimated_start='" + Math.round(startTime + schedule.getEstimatedStart(t)) + "', "
                        + "estimated_finish='" + Math.round(startTime + schedule.getEstimatedFinish(t)) + "' "
                        + "WHERE tid='" + t.getDbid() + "'");
            }
        }
    }

    public void setScheduleForWorkflow(Workflow wf, Schedule schedule, long startTime) throws DBException
    {
        for (Task t : wf.getTaskIterator())
        {
            Worker w = schedule.getWorkerForTask(t);
            new DBRecord("schedule",
                    "wkid", w.getDbid(),
                    "tid", t.getDbid(),
                    "estimated_start", Math.round(startTime+schedule.getEstimatedStart(t)),
                    "estimated_finish", Math.round(startTime+schedule.getEstimatedFinish(t))).insert();
        }
    }

    /**
     * Dispatch ready tasks
     */
    public void dispatchTask()
    {
        if (rescheduling)
        {
            return;
        }
        synchronized (DISPATCH_LOCK_OBJ)
        {
            List<DBRecord> results = DBRecord.select("_task_to_dispatch", new DBRecord());
            List<Thread> dispatchThreads = new LinkedList<>();
            for (DBRecord res : results)
            {
                final DBRecord r = res;
                final Task t = Task.getWorkflowTaskFromDB(r.getInt("tid"));
                final Message msg = new Message(Message.TYPE_DISPATCH_TASK);
                String dir = Utils.getProp("exec_site_file_storage_dir") + t.getWorkingDirSuffix();
                msg.set("cmd", t.getCmd());
                msg.set("task_name", t.getName());
                msg.set("task_namespace", t.getNamespace());
                msg.set("tid", t.getDbid());
                msg.set("wfid", t.getWfdbid());
                msg.set(Message.PARAM_WORKER_UUID, r.get("uuid"));
                msg.set("input_files", t.getInputFiles());
                msg.set("file_dir", dir);
                msg.set("output_files", t.getOutputFiles());
                if (t.getStatus() == Task.STATUS_SUSPENDED)
                {
                    msg.set("migrate", true);
                }
                else
                {
                    msg.set("migrate", false);
                }

                /**
                 * Start new thread for each task dispatching to transfer input
                 * files parallelly.
                 */
                Thread dispatchThread = new Thread(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            logger.log("Uploading input files and checkpoint files (if needed) "
                                    + "for task " + t.getName() + " to " + r.get("uuid") + "...");

                            //Upload input files
                            boolean isUploadComplete = uploadInputFilesForTaskToEsp(t, r.get("esp_hostname"));

                            //Upload checkpoint files if this task is migrated
//                            if (t.getStatus() == Task.STATUS_SUSPENDED)
//                            {
//                                isUploadComplete = isUploadComplete && uploadCheckpointFiles(t.getDbid(), r.get("esp_hostname"));
//                            }
                            
                            //Start dispatch task
                            if (isUploadComplete)
                            {
                                logger.log("Done.");
                                logger.log("Dispatching task " + t.getName() + " to " + r.get("esp_hostname") + ":" + r.get("port") + "...");
                                Message response = comm.sendForResponseSync(r.get("esp_hostname"), r.getInt("esp_port"), addr.getPort(), msg);
                                if (response.getBoolean("complete"))
                                {
                                    logger.log("Done.");
                                    Task.updateTaskStatus(t.getDbid(), -1, -1, -1, Task.STATUS_DISPATCHED);
                                }
                                else
                                {
                                    logger.log("Fail dispatching task " + t.getName() + ". The worker is busy.");
                                }
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
                });
                dispatchThread.start();
                dispatchThreads.add(dispatchThread);
            }
            try
            {
                for(Thread th : dispatchThreads)
                {
                    th.join();
                }
            }
            catch (InterruptedException ex)
            {
                logger.log("", ex);
            }
        }
        System.gc();
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
        String dir = Utils.getProp("exec_site_file_storage_dir") + t.getWorkingDirSuffix();
        LinkedList<Message> sentMsgs = new LinkedList<>();
        for (DBRecord f : files)
        {
            String fname = f.get("name");
            //Whether the file is in the espHost
            boolean fileExist = DBRecord.select("SELECT fid "
                    + "FROM exec_site_file esf "
                    + "JOIN exec_site es ON esf.esid = es.esid "
                    + "WHERE es.hostname='" + espHost + "' "
                    + "AND fid = '" + f.get("fid") + "'").size() > 0;
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
                    msg.set("filename", fname);
                    msg.set("dir", dir);
                    msg.set("upload_to", espHost);
                    msg.set("fid", f.get("fid"));
                    msg.set("file", WorkflowFile.getFileFromDB(f.getInt("fid")));
                    logger.log("Request file " + fname + " from " + r.get("hostname") + " to " + espHost);
                    comm.sendForResponseAsync(r.get("hostname"), r.getInt("port"), addr.getPort(), msg);
                    logger.log("Done");
                    sentMsgs.add(msg);
                }
                catch (IndexOutOfBoundsException ex)
                {
                    throw new FileTransferException("No execution site containing the file " + fname + ".");
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
            if (!res.getBoolean("upload_complete"))
            {
                complete = complete && false;
                logger.log("", ((Exception) res.getObject("exception")));
            }
        }
        return complete;
    }

    public boolean uploadCheckpointFiles(int tid, String espHost)
    {
        try
        {
            DBRecord rec = DBRecord.select(
                    "SELECT es.hostname, es.port, esc.path "
                    + "FROM exec_site_checkpoint esc "
                    + "JOIN exec_site es ON esc.esid = es.esid "
                    + "WHERE esc.tid = '" + tid + "' "
                    + "ORDER BY esc.checkpointed_at DESC LIMIT 1")
                    .get(0);
            Message msg = new Message(Message.TYPE_CHECKPOINT_FILE_UPLOAD_REQUEST)
                    .set("path", rec.get("path"))
                    .set("upload_to", espHost);
            Message response = comm.sendForResponseSync(rec.get("hostname"), rec.getInt("port"), addr.getPort(), msg);
            return response.getBoolean("complete");
        }
        catch (IndexOutOfBoundsException ex)
        {
            logger.log("No host with checkpoint for tid " + tid + " found.");
            return false;
        }
        catch (IOException ex)
        {
            logger.log("Cannot send message.", ex);
            return false;
        }
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

    public ArrayList<Message> broadcastToWorkers(Message msg, boolean waitForResponse)
    {
        List<DBRecord> workers = DBRecord.select("worker", "select uuid, esp_hostname, esp_port from worker");
        ArrayList<Message> msgs = new ArrayList<>();
        for (DBRecord w : workers)
        {
            msg.set(Message.PARAM_WORKER_UUID, w.get("uuid"));
            Message m = msg.copy();
            msgs.add(m);
            try
            {
                if (waitForResponse)
                {
                    comm.sendForResponseAsync(
                            w.get("esp_hostname"), 
                            w.getInt("esp_port"), 
                            this.addr.getPort(), 
                            m
                            );
                }
                else
                {
                    comm.sendMessage(w.get("esp_hostname"), w.getInt("esp_port"), m);
                }
            }
            catch (IOException ex)
            {
                logger.log("Cannot broadcast message to " + w.get("uuid") + ": " + ex.getMessage());
            }
        }
        if (waitForResponse)
        {
            return msgs;
        }
        else
        {
            return null;
        }
    }
    
    
}
