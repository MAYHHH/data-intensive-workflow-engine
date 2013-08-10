package workflowengine.schedule;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import workflowengine.resource.Worker;
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
    private HashMap<Task, Double> estimatedStart;
    private HashMap<Task, Double> estimatedFinish;
    private double makespan;
    private double cost;
    private boolean edited;
    private double fitness;

    public Schedule(SchedulerSettings settings)
    {
        this.settings = settings;
        estimatedStart = new HashMap<>(settings.getTotalTasks()); 
        estimatedFinish = new HashMap<>(settings.getTotalTasks()); 
        mapping = new HashMap<>(settings.getTotalTasks());
        mapping.putAll(settings.getFixedMapping());
        mapAllTaskToFirstWorker();
        edited = true;
    }

    public SchedulerSettings getSettings()
    {
        return settings;
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
        estimatedStart = new HashMap<>(sch.estimatedStart); 
        estimatedFinish = new HashMap<>(sch.estimatedFinish); 
        this.fitness = sch.fitness;
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
        for (int i = 0; i < settings.getTotalTasks(); i++)
        {
            setWorkerForTask(i, w);
        }
        this.edited = true;
    }

    public void random()
    {
        for (int i = 0; i < settings.getTotalTasks(); i++)
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
        if (!settings.isFixedTask(t))
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
        if (edited)
        {
            evaluate();
        }
        return cost;
    }

    public void evaluate()
    {
        if(edited)
        {
            calMakespan();
            calCost();
        }
        edited = false;
        this.fitness = this.settings.getFc().getFitness(this);
    }
    
    public double getFitness()
    {
        if(edited)
        {
            evaluate();
        }
        return fitness;
    }

    private void calMakespan()
    {
        LinkedList<Task> finishedTasks = new LinkedList<>();
        estimatedStart.clear();
        estimatedFinish.clear();

        for (Task t : settings.getFixedTasks())
        {
            estimatedStart.put(t, 0.0);
            estimatedFinish.put(t, 0.0);
//            t.setProp("finishTime", 0.0);
        }

        finishedTasks.addAll(settings.getFixedTasks());
//        pendingTasks.push(start);

        for (int i = 0; i < settings.getTotalWorkers(); i++)
        {
            settings.getWorker(i).setProp("readyTime", 0.0);
        }
//        Workflow wf = settings.getWf();
        makespan = 0;
        LinkedList<Task> pendingTasks = settings.getOrderedTaskQueue();
        while (!pendingTasks.isEmpty())
        {
            Task t = pendingTasks.poll();
            if (finishedTasks.contains(t))
            {
                continue;
            }
            if (!finishedTasks.containsAll(settings.getParentTasks(t)))
            {
                pendingTasks.push(t);
                continue;
            }

            Worker s = this.getWorkerForTask(t);
            double serverReadyTime = s.getDoubleProp("readyTime");
            double parentFinishTime = 0;
            for (Task p : settings.getParentTasks(t))
            {
//                parentFinishTime = Math.max(parentFinishTime, p.getDoubleProp("finishTime"));
                parentFinishTime = Math.max(parentFinishTime, estimatedFinish.get(p));
            }
            double taskStartTime = Math.max(parentFinishTime, serverReadyTime);
            double taskFinishTime = taskStartTime + s.getExecTime(t);
            
            for (Task c : settings.getChildTasks(t))
            {
                Worker cs = this.getWorkerForTask(c);
                if(!s.equals(cs))
                {
                    taskFinishTime += settings.getTransferTime(s, cs, t.getOutputFilesForTask(c));
                }
            }
            
//            t.setProp("startTime", taskStartTime);
//            t.setProp("finishTime", taskFinishTime);
            estimatedStart.put(t, taskStartTime);
            estimatedFinish.put(t, taskFinishTime);
            s.setProp("readyTime", taskFinishTime);
            makespan = Math.max(makespan, taskFinishTime);
//            pendingTasks.addAll(wf.getChildTasks(t));
            finishedTasks.add(t);
        }
//        makespan = wf.getEndTask().getDoubleProp("finishTime");
//        makespan = estimatedFinish.get(settings.getEndTasks());
    }

    private void calCost()
    {
        //Calculate cost of schedule
        cost = 0;
        for (Task t : mapping.keySet())
        {
            cost += mapping.get(t).getExecCost(t);
        }
        
        LinkedList<Task> queue = settings.getOrderedTaskQueue();
        while(!queue.isEmpty())
        {
            Task t = queue.poll();
            Worker wt = this.getWorkerForTask(t);
            for(Task c : settings.getChildTasks(t))
            {
                Worker wc = this.getWorkerForTask(c);
                if(!wt.equals(wc))
                {
                    cost += settings.getTransferCost(wt, wc, t.getOutputFilesForTask(c));
                }
            }
        }
        
//        cost = makespan*settings.getTotalWorkers();
    }

    public void print()
    {
        System.out.println(getMakespan() + " " + getCost());
        for (Task t : mapping.keySet())
        {
//            System.out.println(t+"->"+mapping.get(t)+ " start: "+ t.getDoubleProp("startTime")+ " end: "+t.getDoubleProp("finishTime"));
            System.out.println(t + "->" + mapping.get(t) 
                    + " start: " + estimatedStart.get(t) 
                    + " end: " + estimatedFinish.get(t));
        }
    }
    
    public double getEstimatedStart(Task t)
    {
        return estimatedStart.get(t);
    }
    
    public double getEstimatedFinish(Task t)
    {
        return estimatedFinish.get(t);
    }
    
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<settings.getTotalTasks();i++)
        {
            sb.append(settings.getWorkerIndex(this.getWorkerForTask(i))).append(", ");
        }
        return sb.toString();
    }
}
