/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.io.IOException;
import workflowengine.communication.Communicable;
import workflowengine.communication.message.Message;
import workflowengine.utils.Utils;

/**
 *
 * @author udomo
 */
public class SubmitWorkflow
{
    public static void usage()
    {
        System.out.println("Usage: SubmitWorkflow DAX_FILE INPUT_FILE_DIR");
    }
    
    public static void main(String[] args) throws IOException
    {
        if(args.length != 2)
        {
            System.err.println("Please specify DAG file and input file directory.");
            usage();
            System.exit(1);
        }
        String daxFile = args[0];
        String inputDir = args[1];
        
        Message msg = new Message(Message.TYPE_SUBMIT_WORKFLOW);
        msg.set("dax_file", daxFile);
        msg.set("input_dir", inputDir);
        
        String host = Utils.getProp("task_manager_host");
        int port = Utils.getIntProp("task_manager_port");
        new Communicable("Workflow Submitor").sendMessage(host, port, msg);
    }
}
