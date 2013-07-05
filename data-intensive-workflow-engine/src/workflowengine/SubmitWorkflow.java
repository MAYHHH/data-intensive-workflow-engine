/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.io.IOException;
import workflowengine.communication.Communicable;
import workflowengine.communication.Message;

/**
 *
 * @author udomo
 */
public class SubmitWorkflow
{
    public static void main(String[] args) throws IOException
    {
        if(args.length == 0)
        {
            System.err.println("Please specify DAG file.");
            System.exit(1);
        }
        Message msg = new Message(Message.TYPE_SUBMIT_WORKFLOW);
        msg.setParam("dax_file", args[0]);
        WorkflowEngine we = new WorkflowEngine();
        we.init();
        String host = WorkflowEngine.PROP.getProperty("task_manager_host");
        int port = Integer.parseInt(WorkflowEngine.PROP.getProperty("task_manager_port"));
        new Communicable().sendMessage(host, port, msg);
    }
}
