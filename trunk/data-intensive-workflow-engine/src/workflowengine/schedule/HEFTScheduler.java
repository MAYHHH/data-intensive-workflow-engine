/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import workflowengine.workflow.Task;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author udomo
 */
public class HEFTScheduler implements Scheduler
{

    SchedulerSettings settings;
    int totalTasks;
    int totalWorkers;
    double[] avgExecTime;
    double[][] avgCommTime;
    double[] rank;

    @Override
    public Schedule getSchedule(SchedulerSettings settings)
    {
        this.settings = settings;
        totalTasks = settings.getTotalTasks();
        totalWorkers = settings.getTotalWorkers();
        avgExecTime = new double[totalTasks];
        avgCommTime = new double[totalTasks][totalTasks];
        rank = new double[totalTasks];
        
        calAvgExec();
        calAvgComm();
        for (int i = 0; i < totalTasks; i++)
        {
            rank[i] = -1;
        }
        rank(settings.getWf().getStartTask());
        LinkedList<Task> sortedTasks = getSortedTasks();
        
        while (!sortedTasks.isEmpty())
        {
            Task t = sortedTasks.poll();
        }
    }

    void calAvgExec()
    {
        for (int i = 0; i < totalTasks; i++)
        {
            double sum = 0;
            for (int j = 0; j < totalWorkers; j++)
            {

                sum = sum + settings.getWorker(j).getExecTime(settings.getTask(i));
            }
            avgExecTime[i] = sum / totalWorkers;
        }
    }
    
    void calAvgComm()
    {
        for (int t1 = 0; t1 < totalTasks; t1++)
        {
            Task parent = settings.getTask(t1);
            Collection<Task> children = settings.getWf().getChildTasks(parent);
            for (Task child : children)
            {
                WorkflowFile[] files = parent.getOutputFilesForTask(child);
                double sum = 0;
                for (int k = 0; k < totalWorkers; k++)
                {
                    for (int l = 0; l < totalWorkers; l++)
                    {
                        for (WorkflowFile f : files)
                        {
                            sum = sum + settings.getEs().getTransferTime(settings.getWorker(k), settings.getWorker(l), f);
                        }
                    }
                }
                avgCommTime[t1][settings.getTaskIndex(child)] = sum / totalWorkers / totalWorkers;
            }
        }
    }
    
    double rank(Task t)
    {
        int ti = settings.getTaskIndex(t);
        if (rank[ti] == -1)
        {
            double r = avgExecTime[ti];
            double max = 0;
            for (Task c : settings.getWf().getChildTasks(t))
            {
                int tc = settings.getTaskIndex(c);
                max = Math.max(max, avgCommTime[ti][tc] + rank(c));
            }
            r = r + max;
            rank[ti] = r;
            return r;
        }
        else
        {
            return rank[ti];
        }
    }
    
    LinkedList<Task> getSortedTasks()
    {
        LinkedList<Task> tasks = new LinkedList<>();
        for(int i=0;i<totalTasks;i++)
        {
            tasks.add(settings.getTask(i));
        }
        
        Collections.sort(tasks, new Comparator<Task>() {

            @Override
            public int compare(Task o1, Task o2)
            {
                double r1 = rank[settings.getTaskIndex(o1)];
                double r2 = rank[settings.getTaskIndex(o2)];
                if(r1 < r2)
                {
                    return 1;
                }
                if(r1 == r2)
                {
                    return 0;
                }
                return -1;
            }
        });
        return tasks;
    }
    
}

