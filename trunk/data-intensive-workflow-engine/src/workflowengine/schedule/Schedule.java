package workflowengine.schedule;

import java.util.HashMap;
import java.util.LinkedList;
import workflowengine.resource.Worker;
import workflowengine.workflow.Workflow;
import workflowengine.workflow.Task;

/**
 *
 * @author Orachun
 */
public class Schedule
{
    protected final SchedulerSettings settings;
//    private Random r;
//    private final Workflow wf;
//    private final ExecSite es;
//    private final List<Task> taskList;
//    private final HashMap<Task, Worker> fixedMapping; //For finished tasks
    private HashMap<Task, Worker> mapping;
    private double makespan;
    private double cost;
    private boolean edited;

    public Schedule(SchedulerSettings settings)
    {
        this.settings = settings;
        mapping = new HashMap<>(settings.getTotalTasks());
        mapping.putAll(settings.getFixedMapping());
        mapAllTaskToFirstWorker();
        edited = true;
    }

//    
//    
//    public Schedule(Workflow wf, ExecSite es)
//    {
//        this(wf, es, null);
//    }
//    
//    //Fixed mapping for finished tasks
//    public Schedule(Workflow wf, ExecSite es, HashMap<Task, Worker> fixedMapping)
//    {
//        r = new Random();
//        this.wf = wf;
//        this.es = es;
//        mapping = new HashMap<>(wf.getTotalTasks());
//        taskList = wf.getTaskList();
//        this.fixedMapping = new HashMap<>();
//        if(fixedMapping != null)
//        {
//            taskList.removeAll(fixedMapping.keySet());
//            mapping.putAll(fixedMapping);
//            this.fixedMapping.putAll(fixedMapping);
//        }
//        mapAllTaskToFirstWorker();
//        edited = true;
//    }

    //Copy constructor
    protected Schedule(Schedule sch)
    {
//        this.r = sch.r;
//        this.wf = sch.wf;
//        this.es = sch.es;
        mapping = new HashMap<>(sch.mapping);
        this.makespan = sch.makespan;
        this.cost = sch.cost;
        this.edited = sch.edited;
        this.settings = sch.settings;
//        this.fixedMapping = sch.fixedMapping;
//        taskList = sch.taskList;
    }
    
    public Schedule copy()
    {
        return new Schedule(this);
    }
    
    
    //Map all tasks to the first worker
    private void mapAllTaskToFirstWorker()
    {
        Worker w = settings.getWorker(0);
        for(int i=0;i<settings.getTotalTasks();i++)
        {
            setWorkerForTask(i, w);
        }
        this.edited = true;
    }
    
    public void random()
    {
        for(int i=0;i<settings.getTotalTasks();i++)
        {
            setWorkerForTask(i, settings.getRandomWorker());
        }
        this.edited = true;
    }

    
    public Worker getWorkerForTask(int taskIndex)
    {
        return mapping.get(settings.getTask(taskIndex));
    }
    
    public Worker getWorkerForTask(Task t)
    {
        return mapping.get(t);
    }

    public void setWorkerForTask(int taskIndex, Worker s)
    {
        setWorkerForTask(settings.getTask(taskIndex), s);
    }
    public void setWorkerForTask(Task t, Worker s)
    {
        if(!settings.isFixedMapping(t))
        {
            mapping.put(t, s);
            edited = true;
        }
    }
    public void setWorkerForTask(int taskIndex, int workerIndex)
    {
        setWorkerForTask(settings.getTask(taskIndex), settings.getWorker(workerIndex));
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
        Task start = settings.getStartTask();
        
        for(Task t : settings.getFixedTasks())
        {
            t.setProp("finishTime", 0.0);
        }
        
        finishedTasks.addAll(settings.getFixedTasks());
        pendingTasks.push(start);
        
        for(int i=0;i<settings.getTotalWorkers();i++)
        {
            settings.getWorker(i).setProp("readyTime", 0.0);
        }
        Workflow wf = settings.getWf();
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
