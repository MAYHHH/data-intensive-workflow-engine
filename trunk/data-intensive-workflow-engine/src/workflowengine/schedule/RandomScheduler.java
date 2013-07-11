/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

import java.util.HashMap;
import workflowengine.resource.ExecSite;
import workflowengine.resource.Worker;
import workflowengine.workflow.Task;
import workflowengine.workflow.Workflow;

/**
 *
 * @author udomo
 */
public class RandomScheduler implements Scheduler
{
    @Override
    public Schedule getSchedule(Workflow wf, ExecSite es)
    {
        return getSchedule(wf, es, null);
    }

    @Override
    public Schedule getSchedule(Workflow wf, ExecSite es, HashMap<Task, Worker> fixedMapping)
    {
        Schedule s = new Schedule(new SchedulerSettings(wf, es, fixedMapping));
        s.random();
        return s;
    }
    
    
    
}
