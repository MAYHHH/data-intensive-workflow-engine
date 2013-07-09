/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.io.IOException;
import java.util.HashMap;
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
import workflowengine.utils.DBException;
import workflowengine.utils.Logger;
import workflowengine.workflow.Workflow;
import workflowengine.workflow.Task;

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
    private Properties p = new Properties();
    private int suspendCount = -1;
    private int workerCount = -1;
    private Communicable comm = new Communicable()
    {
        @Override
        public void handleMessage(Message msg)
        {
            switch (msg.getType())
            {
                case Message.TYPE_UPDATE_TASK_STATUS:
                    updateTaskStatus(msg);
                    break;
                case Message.TYPE_UPDATE_NODE_STATUS:
                    updateNodeStatus(msg);
                    break;
                case Message.TYPE_SUBMIT_WORKFLOW:
                    submitWorkflow(msg);
                    break;
                case Message.TYPE_SUSPEND_TASK_COMPLETE:
                    suspendComplete();
                    break;
            }
        }
    };
    
    private TaskManager()
    {
        addr = new HostAddress(WorkflowEngine.PROP, "task_manager_host", "task_manager_port");
        comm.setLocalPort(addr.getPort());
        comm.startServer();
    }
    
    public static TaskManager start()
    {
        if(tm == null)
        {
            tm = new TaskManager();
        }
        logger.log("Task manager is started.");
        return tm;
    }

    synchronized public void updateNodeStatus(Message msg)
    {
        HostAddress workerAddr = (HostAddress)msg.getObjectParam("address");
        HostAddress espAddr = (HostAddress)msg.getObjectParam("esp_address");
        Worker.updateWorkerStatus(
                espAddr,
                workerAddr, 
                msg.getIntParam("current_tid"), 
                msg.getDoubleParam("free_memory"), 
                msg.getDoubleParam("free_space"),
                msg.getDoubleParam("cpu"),
                msg.getParam("uuid")
        );
    }

    synchronized public void updateTaskStatus(Message msg)
    {
        Task.updateTaskStatus(
                msg.getIntParam("tid"), 
                msg.getIntParam("start"), 
                msg.getIntParam("end"), 
                msg.getIntParam("exit_value"), 
                msg.getParam("status").charAt(0)
        );
        if(msg.getIntParam("end")!=-1)
        {
            if(isRescheduleNeeded())
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
        Workflow wf = Workflow.open(WorkflowEngine.PROP.getProperty("working_dir")+wfid+"/"+wfid+".wfobj");
        List<DBRecord> res = DBRecord.select(
                "SELECT s.tid, w.uuid "
                + "FROM workflow_task t JOIN schedule s ON t.tid = s.tid "
                + "JOIN worker w on w.wkid = s.wkid "
                + "WHERE status = 'C' AND t.wfid='"+wfid+"'");
        HashMap<Task, Worker> fixed = new HashMap<>(res.size());
        for(DBRecord r : res)
        {
            Task t = Task.getWorkflowTaskFromDB(r.getInt("tid"));
            Worker w = Worker.getWorkerFromDB(r.get("uuid"));
            fixed.put(t, w);
        }
        Schedule schedule = sch.getSchedule(wf, getExecSite(), fixed);
        
        updateScheduleForWorkflow(wf, schedule);
        
        broadcastToWorkers(new Message(Message.TYPE_SUSPEND_TASK));
        //Will call dispatchTask automatically after all workers suspent 
        
    }
    
    
    
    public void submitWorkflow(Message msg)
    {
        logger.log("Workflow "+msg.getParam("dax_file")+" is submitted.");
        Workflow w = Workflow.fromDAX(msg.getParam("dax_file"));
        execWorkflow(w);
    }

    synchronized public void execWorkflow(Workflow wf) throws DBException
    {
        logger.log("Scheduling the workflow "+wf.getName()+".");
        Schedule schedule = sch.getSchedule(wf, getExecSite());
        setScheduleForWorkflow(wf, schedule);
        logger.log("Workflow "+wf.getName()+" is scheduled.");
        dispatchTask();
    }
    
    synchronized public void updateScheduleForWorkflow(Workflow wf, Schedule schedule) throws DBException
    {
        for(Task t : wf.getTaskIterator())
        {
            Worker w = schedule.getWorkerForTask(t);
            DBRecord.update("UPDATE schedule SET wkid='"+w.getDbid()+"' WHERE tid='"+t.getDbid()+"'");
        }
    }

    synchronized public void setScheduleForWorkflow(Workflow wf, Schedule schedule) throws DBException
    {
        for(Task t : wf.getTaskIterator())
        {
            Worker w = schedule.getWorkerForTask(t);
            new DBRecord("schedule", "wkid", w.getDbid(), "tid", t.getDbid()).insert();
        }
    }

    public void dispatchTask()
    {
        List<DBRecord> results = DBRecord.select("_task_to_dispatch", new DBRecord());
        for(DBRecord r : results)
        {
            Task t = Task.getWorkflowTaskFromDB(r.getInt("tid"));
            Message msg = new Message(Message.TYPE_DISPATCH_TASK);
            msg.setParam("cmd", t.getCmd());
            msg.setParam("task_name", t.getName());
            msg.setParam("tid", t.getDbid());
            msg.setParam("wfid", t.getWfdbid());
            msg.setParam("uuid", r.get("uuid"));
            if(t.getStatus() == 'S')
            {
                msg.setParam("migrate", "migrate");
            }
            try
            {
                logger.log("Dispatching task "+t.getName()+" to "+r.get("uuid")+"...");
                comm.sendMessage(r.get("esp_hostname"), r.getInt("esp_port"), msg);
            }
            catch (IOException ex)
            {
                System.out.println("Cannot send execution request of "+t.getName()+" to "+r.get("hostname"));
            }
            
        }
    }

    public ExecSite getExecSite() throws DBException
    {
        List<DBRecord> workers = DBRecord.select("worker", "select uuid from worker");
        ExecSite es = new ExecSite();
        for(DBRecord w : workers)
        {
            es.addWorker(Worker.getWorkerFromDB(w.get("uuid")));
        }
        return es;
    }
    
    public void broadcastToWorkers(Message msg)
    {
        //TODO: fix this
        List<DBRecord> workers = DBRecord.select("worker", "select hostname from worker");
        for(DBRecord w : workers)
        {
            try
            {
                comm.sendMessage(w.get("hostname"), Integer.parseInt(WorkflowEngine.PROP.getProperty("task_executor_port")), msg);
            }
            catch (IOException ex)
            {
                logger.log("Cannot broadcast message: "+ex.getMessage());
            }
        }
    }
    
    public void suspendComplete()
    {
        if(suspendCount == -1)
        {
            suspendCount = 0;
            workerCount = DBRecord.select("worker", "select count(hostname) as count from worker").get(0).getInt("count");
        }
        suspendCount++;
        if(suspendCount == workerCount)
        {
            dispatchTask();
        }
    }
}
