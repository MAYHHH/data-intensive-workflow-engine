/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import workflowengine.resource.ExecSite;
import workflowengine.resource.Worker;
import workflowengine.workflow.Task;
import workflowengine.workflow.Workflow;

/**
 *
 * @author udomo
 */
public class SchedulerSettings
{
    private Random r;
    private final Workflow wf;
    private final ExecSite es;
    private final List<Task> taskList;
    private final HashMap<Task, Worker> fixedMapping; //For finished tasks
    private final int totalTasks;
    private final int totalWorkers;
    private final Set<Task> fixedTasks;
    
    public SchedulerSettings(Workflow wf, ExecSite es, HashMap<Task, Worker> fixedMapping)
    {
        r = new Random();
        this.wf = wf;
        this.es = es;
        totalTasks = wf.getTotalTasks();
        totalWorkers = es.getTotalWorkers();
        taskList = wf.getTaskList();
        this.fixedMapping = new HashMap<>();
        if(fixedMapping != null)
        {
            taskList.removeAll(fixedMapping.keySet());
            this.fixedMapping.putAll(fixedMapping);
            fixedTasks = fixedMapping.keySet();
        }
        else
        {
            fixedTasks = new TreeSet<>();
        }
    }
    
    public SchedulerSettings(Workflow wf, ExecSite es)
    {
        this(wf, es, null);
    }

    public int getTotalTasks()
    {
        return totalTasks;
    }

    public int getTotalWorkers()
    {
        return totalWorkers;
    }
    
    public HashMap<Task, Worker> getFixedMapping()
    {
        return fixedMapping;
    }
    
    public Worker getWorker(int i)
    {
        return es.getWorker(i);
    }
    public Task getTask(int i)
    {
        return taskList.get(i);
    }
    
    public Worker getRandomWorker()
    {
        return es.getWorker(r.nextInt(totalWorkers));
    }
    
    public boolean isFixedMapping(Task t)
    {
        return fixedMapping.containsKey(t);
    }
    public boolean isFixedMapping(int taskIndex)
    {
        return isFixedMapping(getTask(taskIndex));
    }
    
    public Task getStartTask()
    {
        return wf.getStartTask();
    }

    public Workflow getWf()
    {
        return wf;
    }

    public ExecSite getEs()
    {
        return es;
    }

    public List<Task> getTaskList()
    {
        return taskList;
    }

    public Set<Task> getFixedTasks()
    {
        return fixedTasks;
    }
    
    public int getTaskIndex(Task t)
    {
        return taskList.indexOf(t);
    }
}
