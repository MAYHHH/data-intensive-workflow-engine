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
 * @author Orachun
 */
public interface Scheduler
{
    public Schedule getSchedule(Workflow wf, ExecSite nw);
    public Schedule getSchedule(Workflow wf, ExecSite nw, HashMap<Task, Worker> fixedMapping);
}
