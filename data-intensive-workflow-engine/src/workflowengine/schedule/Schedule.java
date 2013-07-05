package workflowengine.schedule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import workflowengine.resource.ExecSite;
import workflowengine.resource.Worker;
import workflowengine.workflow.Workflow;
import workflowengine.workflow.Task;

/**
 *
 * @author Orachun
 */
public class Schedule
{
    private Random r = new Random();
    private final Workflow wf;
    private final ExecSite es;
    private final List<Task> taskList;
    private final HashMap<Task, Worker> fixedMapping; //For finished tasks
    private HashMap<Task, Worker> mapping;
    private double makespan;
    private double cost;
    private boolean edited;

    public Schedule(Workflow wf, ExecSite es)
    {
        this(wf, es, null);
    }
    
    //Fixed mapping for finished tasks
    public Schedule(Workflow wf, ExecSite es, HashMap<Task, Worker> fixedMapping)
    {
        this.wf = wf;
        this.es = es;
        mapping = new HashMap<>(wf.getTotalTasks());
        taskList = wf.getTaskList();
        this.fixedMapping = new HashMap<>();
        if(fixedMapping != null)
        {
            taskList.removeAll(fixedMapping.keySet());
            mapping.putAll(fixedMapping);
            this.fixedMapping.putAll(fixedMapping);
        }
        mapAll();
        edited = true;
    }

    //Copy constructor
    public Schedule(Schedule sch)
    {
        this.wf = sch.wf;
        this.es = sch.es;
        mapping = new HashMap<>(wf.getTotalTasks());
        this.mapping.putAll(sch.mapping);
        this.makespan = sch.makespan;
        this.edited = sch.edited;
        this.fixedMapping = new HashMap<>();
        this.fixedMapping.putAll(sch.fixedMapping);
        taskList = new ArrayList<>(sch.taskList);
    }
    
    //Map all tasks to the first worker
    private void mapAll()
    {
        Worker w = es.getWorker(0);
        for(int i=0;i<taskList.size();i++)
        {
            setWorkerForTask(i, w);
        }
        this.edited = true;
    }
    
    public void random()
    {
        for(int i=0;i<taskList.size();i++)
        {
            setWorkerForTask(i, es.getWorker(r.nextInt(es.getTotalWorkers())));
        }
        this.edited = true;
    }

    
    public Worker getWorkerForTask(int taskID)
    {
        return mapping.get(taskList.get(taskID));
    }
    
    public Worker getWorkerForTask(Task t)
    {
        return mapping.get(t);
    }

    public void setWorkerForTask(int taskID, Worker s)
    {
        setWorkerForTask(taskList.get(taskID), s);
    }
    public void setWorkerForTask(Task t, Worker s)
    {
        if(!fixedMapping.containsKey(t))
        {
            mapping.put(t, s);
            edited = true;
        }
    }

    public double getMakespan()
    {
        if (edited)
        {
            evaluate();
        }
        return makespan;

    }
    
    public double getCost()
    {
        if(edited)
        {
            evaluate();
        }
        return cost;
    }

    private void evaluate()
    {
        calMakespan();
        calCost();
        edited = false;
    }
    
    private void calMakespan()
    {
        LinkedList<Task> finishedTasks = new LinkedList<>();
        LinkedList<Task> pendingTasks = new LinkedList<>();
        Task start = wf.getStartTask();
        
        for(Task t : fixedMapping.keySet())
        {
            t.setProp("finishTime", 0.0);
        }
        
        finishedTasks.addAll(fixedMapping.keySet());
        pendingTasks.push(start);
        
        for(int i=0;i<es.getTotalWorkers();i++)
        {
            es.getWorker(i).setProp("readyTime", 0.0);
        }
        
        while(!pendingTasks.isEmpty())
        {
            Task t = pendingTasks.pop();
            if(finishedTasks.contains(t))
            {
                continue;
            }
            if(!finishedTasks.containsAll(wf.getParentTasks(t)))
            {
                pendingTasks.push(t);
                continue;
            }
            
            Worker s = mapping.get(t);
            double serverReadyTime = s.getDoubleProp("readyTime");
            double parentFinishTime = 0;
            for(Task p : wf.getParentTasks(t))
            {
                parentFinishTime = Math.max(parentFinishTime, p.getDoubleProp("finishTime"));
            }
            double taskStartTime = Math.max(parentFinishTime, serverReadyTime);
            double taskFinishTime = taskStartTime+s.getExecTime(t);
            t.setProp("startTime", taskStartTime);
            t.setProp("finishTime", taskFinishTime);
            s.setProp("readyTime", taskFinishTime);
            pendingTasks.addAll(wf.getChildTasks(t));
            finishedTasks.add(t);
        }
        makespan = wf.getEndTask().getDoubleProp("finishTime");
    }
    
    private void calCost()
    {
        //Calculate cost of schedule
        cost = 0;
        for (Task t : mapping.keySet())
        {
            cost += mapping.get(t).getExecCost(t);
        }
    }
    
    public void print()
    {
        System.out.println(getMakespan()+" "+getCost());
        for(Task t : mapping.keySet())
        {
            System.out.println(t+"->"+mapping.get(t)+ " start: "+ t.getDoubleProp("startTime")+ " end: "+t.getDoubleProp("finishTime"));
        }
    }
}
