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
    private Properties p = new Properties();
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
                    System.err.println("Workflow is submitted.");
                    submitWorkflow(msg);
                    break;
            }
        }
    };
    
    private TaskManager()
    {
        comm.startServer(Integer.parseInt(WorkflowEngine.PROP.getProperty("task_manager_port")));
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
        Worker.updateWorkerStatus(
                msg.getParam("FROM"), 
                msg.getIntParam("current_tid"), 
                msg.getDoubleParam("free_memory"), 
                msg.getDoubleParam("free_space"),
                msg.getDoubleParam("cpu")
        );
    }

    synchronized public void updateTaskStatus(Message msg)
    {
        Task.updateTaskStatus(msg.getIntParam("tid"), msg.getIntParam("start"), msg.getIntParam("end"), msg.getIntParam("exit_value"));
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
                "SELECT s.* "
                + "FROM workflow_task t JOIN schedule s ON t.tid = s.tid "
                + "WHERE status = 'C' AND t.wfid='"+wfid+"'");
        HashMap<Task, Worker> fixed = new HashMap<>(res.size());
        for(DBRecord r : res)
        {
            Task t = Task.getWorkflowTaskFromDB(r.getInt("tid"));
            Worker w = Worker.getWorkerFromDB(r.getInt("wkid"));
            fixed.put(t, w);
        }
        Schedule schedule = sch.getSchedule(wf, getExecSite(), fixed);
        
        updateScheduleForWorkflow(wf, schedule);
        //TODO: when & how to migrate
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

    public Worker getWorkerForTask(Task t) throws DBException
    {
        String hostname = DBRecord.select(null, "SELECT wk.name FROM"
                + "schedule s JOIN worker wk ON s.wkid = wk.wkid"
                + "WHERE wk.name='" + t.getName() + "'").get(0).get("name");
        return Worker.getWorker(hostname, 0, 0, 0);
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
            
            try
            {
                logger.log("Dispatching task "+t.getName()+" to "+r.get("hostname")+"...");
                comm.sendMessage(r.get("hostname"), Integer.parseInt(WorkflowEngine.PROP.getProperty("task_executor_port")), msg);
            }
            catch (IOException ex)
            {
                System.out.println("Cannot send execution request of "+t.getName()+" to "+r.get("hostname"));
            }
            
        }
    }

    public ExecSite getExecSite() throws DBException
    {
        List<DBRecord> workers = DBRecord.select("worker", "select hostname from worker");
        ExecSite es = new ExecSite();
        for(DBRecord w : workers)
        {
            es.addWorker(Worker.getWorkerFromDB(w.get("hostname")));
//            es.addWorker(Worker.getWorker(w.get("hostname"), w.getDouble("cpu"), w.getDouble("free_space"), w.getDouble("unit_cost")));
        }
        return es;
    }
}
