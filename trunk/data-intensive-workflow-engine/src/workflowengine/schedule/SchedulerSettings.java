/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

import workflowengine.schedule.fc.FC;
import workflowengine.schedule.fc.MakespanFC;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import workflowengine.resource.ExecSite;
import workflowengine.resource.Worker;
import workflowengine.workflow.Task;
import workflowengine.workflow.Workflow;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author udomo
 */
public class SchedulerSettings
{
    private Random r;
    private final Workflow originalWf;
    private final Workflow wf;
    private final ExecSite es;
    private final List<Task> taskList;
    private final HashMap<Task, Worker> fixedMapping; //For finished tasks
    private final int totalTasks;
    private final int totalWorkers;
    private final Set<Task> fixedTasks;
    private HashMap<String, Object> params = new HashMap<>();
    private FC fc;
    
    public SchedulerSettings(Workflow wf, ExecSite es)
    {
        this(wf, es, (FC)null);
    }
    public SchedulerSettings(Workflow originWf, ExecSite es, FC fc)
    {
        this(originWf, es, (HashMap<Task, Worker>)null);
        if(fc == null)
        {
            this.fc = new MakespanFC();
        }
        else
        {
            this.fc = fc;
        }
    }
    
    public SchedulerSettings(Workflow originWf, ExecSite es, HashMap<Task, Worker> fixedMapping)
    {
        this.originalWf = originWf;
        r = new Random();
        this.es = es;
        totalWorkers = es.getTotalWorkers();
        this.fixedMapping = new HashMap<>();
        if(fixedMapping == null)
        {
            fixedTasks = new HashSet<>();
            this.wf = originWf;
        }
        else
        {
            this.wf = generateWorkflow(originWf, fixedMapping);
            this.fixedMapping.putAll(fixedMapping);
            fixedTasks = fixedMapping.keySet();
        }
        totalTasks = this.wf.getTotalTasks();
        taskList = this.wf.getTaskList();
    }

    public FC getFc()
    {
        return fc;
    }
    
    
    
    private Workflow generateWorkflow(Workflow originalWf, HashMap<Task, Worker> fixedMapping)
    {
        Workflow w = new Workflow(originalWf.getName(), false);
        
        LinkedList<Task> q = new LinkedList<>();
        q.addAll(originalWf.getStartTasks());
        while(!q.isEmpty())
        {
            Task t = q.poll();
            if(!fixedMapping.containsKey(t))
            {
                w.addTask(t);
                for(Task c : originalWf.getChildTasks(t))
                {
                    if(!fixedMapping.containsKey(c))
                    {
                        w.addEdge(t, c);
                    }
                }
            }
            q.addAll(originalWf.getChildTasks(t));
        }
        w.defineStartAndEndTasks();
        w.generateFileSet();
        return w;
    }
    
    
    HashMap<Task, Worker> getFixedMapping()
    {
        return fixedMapping;
    }
    
    public boolean isFixedTask(Task t)
    {
        return fixedMapping.containsKey(t);
    }
    
    boolean isFixedTask(int taskIndex)
    {
        return isFixedTask(getTask(taskIndex));
    }
    
    Set<Task> getFixedTasks()
    {
        return new HashSet<>(fixedTasks);
    }
    
    public int getTotalTasks()
    {
        return totalTasks;
    }
    public Task getTask(int i)
    {
        return taskList.get(i);
    }
    public List<Task> getStartTasks()
    {
        return wf.getStartTasks();
    }

    public List<Task> getEndTasks()
    {
        return wf.getEndTasks();
    }

    public List<Task> getTaskList()
    {
        return taskList;
    }

    public int getTaskIndex(Task t)
    {
        return taskList.indexOf(t);
    }
    
    public Iterable<Task> getTaskIterable()
    {
        return new Iterable<Task>() {

            @Override
            public Iterator<Task> iterator()
            {
                return taskList.iterator();
            }
        };
    }
    
    
    
    public LinkedList<Task> getOrderedTaskQueue()
    {
        return wf.getTaskQueue();
    }

    public Collection<Task> getChildTasks(Task t)
    {
        return wf.getChildTasks(t);
    }

    public Collection<Task> getParentTasks(Task t)
    {
        return wf.getParentTasks(t);
    }
    
//    
//    /**
//     * To get information of task from the workflow, use this method with
//     * getTask(int). Note that this will include the fixed task mappings.
//     * @return 
//     */
//    public Workflow getWf()
//    {
//        return wf;
//    }
    
    // <editor-fold desc="Worker methods">
    public int getWorkerIndex(Worker w)
    {
        return es.getWorkerIndex(w);
    }
    public Iterable<Worker> getWorkerIterable()
    {
        return es.getWorkerIterable();
    }
    public Worker getRandomWorker()
    {
        return es.getWorker(r.nextInt(totalWorkers));
    }
    public Worker getWorker(int i)
    {
        return es.getWorker(i);
    }
    public int getTotalWorkers()
    {
        return totalWorkers;
    }

    // </editor-fold>
    public double getTransferCost(Worker from, Worker to, WorkflowFile file)
    {
        return es.getTransferCost(from, to, file);
    }

    public double getTransferCost(Worker from, Worker to, WorkflowFile[] files)
    {
        return es.getTransferCost(from, to, files);
    }
    
    
    
    
    // <editor-fold defaultstate="collapsed" desc="getTransferTime methods">
    
    public double getTransferTime(Worker from, Worker to, WorkflowFile file)
    {
        return es.getTransferTime(from, to, file);
    }

    public double getTransferTime(Worker from, Worker to, WorkflowFile[] files)
    {
        return es.getTransferTime(from, to, files);
    }
    // </editor-fold>
    // <editor-fold defaultstate="collapsed" desc="Methods for setting and getting parameter">
    
    public String getParam(String s)
    {
        Object o = params.get(s);
        return o == null ? null : o.toString();
    }
    public char getCharParam(String s)
    {
        Object o = params.get(s);
        return o == null ? null : (char)o;
    }
    public Object getObjectParam(String s)
    {
        return params.get(s);
    }
    public double getDoubleParam(String s)
    {
        Object o = params.get(s);
        return o == null ? null : Double.parseDouble(o.toString());
    }
    public int getIntParam(String s)
    {
        Object o = params.get(s);
        return o == null ? null : Integer.parseInt(o.toString());
    }
    public boolean getBooleanParam(String s)
    {
        Object o = params.get(s);
        return o == null ? null : Boolean.parseBoolean(o.toString());
    }   
    public void setParam(String s, Object o)
    {
        params.put(s, o);
    }  
    public boolean hasParam(String s)
    {
        return params.containsKey(s);
    }
    
    // </editor-fold>
}
