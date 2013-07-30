/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.io.IOException;
import static workflowengine.TaskManager.logger;
import workflowengine.communication.Communicable;
import workflowengine.communication.HostAddress;
import workflowengine.communication.message.Message;
import workflowengine.utils.Logger;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author orachun
 */
public abstract class Service
{
    protected Communicable comm;
    protected HostAddress addr;
    protected Service(){};
    
    /**
     * Register files to execution site proxy
     * @param wfs
     * @param espAddr 
     */
    protected void registerFile(WorkflowFile[] wfs, HostAddress espAddr)
    {
        Message outputFileMsg = new Message(Message.TYPE_REGISTER_FILE);
        outputFileMsg.set("files", wfs);
        outputFileMsg.set("is_workflow_file", true);
        try
        {
            comm.sendMessage(espAddr, outputFileMsg);
        }
        catch (IOException ex)
        {
            logger.log("Cannot send file registering message to " + espAddr + ": " + ex.getMessage(), ex);
        }
    }
    protected void registerFile(String filename, double size, char type, HostAddress espAddr)
    {
        Message outputFileMsg = new Message(Message.TYPE_REGISTER_FILE);
        outputFileMsg.set("file", filename);
        outputFileMsg.set("size", size);
        outputFileMsg.set("type", type);
        outputFileMsg.set("is_workflow_file", false);
        try
        {
            comm.sendMessage(espAddr, outputFileMsg);
        }
        catch (IOException ex)
        {
            logger.log("Cannot send file registering message to " + espAddr + ": " + ex.getMessage(), ex);
        }
    }
    
    protected abstract void prepareComm();

    public HostAddress getAddr()
    {
        return addr;
    }

    public void setAddr(HostAddress addr)
    {
        this.addr = addr;
    }

    
}
