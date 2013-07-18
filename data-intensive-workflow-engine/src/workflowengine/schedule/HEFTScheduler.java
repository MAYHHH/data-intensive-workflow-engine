/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

import java.util.Collection;
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
    double [] rank;

    @Override
    public Schedule getSchedule(SchedulerSettings settings)
    {
        this.settings = settings;
         totalTasks = settings.getTotalTasks();
         totalWorkers = settings.getTotalWorkers();
         avgExecTime = new double[totalTasks];
        avgCommTime = new double[totalTasks][totalTasks];
        for (int i = 0; i < totalTasks; i++)
        {
            double sum = 0;
            for( int j = 0; j < totalWorkers; j++)
            {
                
                sum = sum + settings.getWorker(j).getExecTime(settings.getTask(i));
            }
             avgExecTime[i] = sum/totalWorkers;
        }
        
        
        for(int t1 = 0; t1 < totalTasks; t1++)
        {
            Task parent = settings.getTask(t1);
            Collection<Task> children = settings.getWf().getChildTasks(parent);
            for(Task child : children)
            {
                WorkflowFile[] files = parent.getOutputFilesForTask(child);
                double sum = 0;
                for (int k = 0; k < totalWorkers; k++)
                {
                    for (int l = 0; l < totalWorkers; l++)
                    {
                        for(WorkflowFile f : files)
                        {
                            sum = sum + settings.getEs().getTransferTime(settings.getWorker(k), settings.getWorker(l), f);
                        }
                    }
                }
                avgCommTime[t1][settings.getTaskIndex(child)] = sum/totalWorkers/totalWorkers;
            }
        }
        rank = new double[totalTasks];
        for (int i = 0; i < totalTasks; i++)
        {
            rank[i] = -1;
        }
        rank(settings.getWf().getStartTask());
   }
        double rank (Task t)
    {
        int ti = settings.getTaskIndex(t);
        if (rank[ti] == -1)
        {
            double r = avgExecTime[ti];
            double max = 0;
            for(Task c : settings.getWf().getChildTasks(t))
            {
                int tc = settings.getTaskIndex(c);
                max = Math.max(max, avgCommTime[ti][tc]+rank(c));
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
}
