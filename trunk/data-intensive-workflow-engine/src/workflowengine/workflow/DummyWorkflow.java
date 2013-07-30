/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.workflow;

import java.util.Random;
import workflowengine.utils.DBException;
import workflowengine.utils.Utils;

/**
 *
 * @author orachun
 */
public class DummyWorkflow extends Workflow
{
    private int size;
    public DummyWorkflow(String name, int size) throws DBException
    {
        super(name);
        this.size = size;
        init();
        defineStartAndEndTasks();
    }
    
    private void init()
    {
        Random r = new Random();
        for(int i=0;i<size;i++)
        {
            int outputCount = r.nextInt(5)+2;
            int opr = r.nextInt(300)+50;
            StringBuilder cmd = new StringBuilder("dummy");
            cmd.append(";").append(opr);
            for(int j=0;j<outputCount;j++)
            {
                cmd.append(";").append("o").append(";").append(Utils.uuid()).append(";").append(r.nextInt(50));
            }
            Task t = Task.getWorkflowTask("dummy"+i, opr, this, cmd.toString(), "Dummy");
            this.addTask(t);
        }
        
        for(int i=0;i<size;i++)
        {
            for(int j=i+1;j<size;j++)
            {
                if(r.nextBoolean())
                {
                    this.addEdge(this.getTask(i), this.getTask(j));
                }
            }
        }
    }
}
