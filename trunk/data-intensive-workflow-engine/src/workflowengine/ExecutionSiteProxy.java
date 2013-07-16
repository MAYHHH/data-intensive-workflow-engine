/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import com.zehon.exception.FileTransferException;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import workflowengine.communication.Communicable;
import workflowengine.communication.HostAddress;
import workflowengine.communication.Message;
import workflowengine.utils.SFTPUtils;
import workflowengine.utils.Utils;

/**
 *
 * @author udomo
 */
public class ExecutionSiteProxy
{

    private static workflowengine.utils.Logger logger = new workflowengine.utils.Logger("execution-site-proxy.log");
    private HostAddress managerAddr;
    private HostAddress addr;
    private static ExecutionSiteProxy esp = null;
    private HashMap<String, HostAddress> workerMap = new HashMap<>(); //<uuid, host address>
    private Communicable comm = new Communicable()
    {
        @Override
        public void handleMessage(Message msg)
        {
            switch (msg.getType())
            {
                //From manager to executor
                case Message.TYPE_DISPATCH_TASK:
                case Message.TYPE_GET_NODE_STATUS:
                case Message.TYPE_SUSPEND_TASK:
                case Message.TYPE_RESPONSE_TO_WORKER:
                    forwardMsgToWorker(msg);
                    break;

                //From executor to manager
                case Message.TYPE_UPDATE_TASK_STATUS:
                case Message.TYPE_UPDATE_NODE_STATUS:
                case Message.TYPE_SUBMIT_WORKFLOW:
                case Message.TYPE_SUSPEND_TASK_COMPLETE:
                case Message.TYPE_REGISTER_FILE:
                case Message.TYPE_RESPONSE_TO_MANAGER:
                    forwardMsgToManager(msg);
                    break;
                    
                //From manager to ESP
                case Message.TYPE_FILE_UPLOAD_REQUEST:
                    uploadFile(msg);
                    break;
            }
        }
    };
    
    private void forwardMsgToWorker(Message msg)
    {
        HostAddress workerAddr = workerMap.get(msg.getParam(Message.PARAM_WORKER_UUID));
        try
        {
            comm.sendMessage(workerAddr, msg);
        }
        catch (IOException ex)
        {
            logger.log("Cannot sent message to " + workerAddr + ": " + ex.getMessage(), ex);
        }
    }
    
    private void forwardMsgToManager(Message msg)
    {
        String uuid = msg.getParam(Message.PARAM_WORKER_UUID);
        HostAddress workerAddr = workerMap.get(uuid);
        if (workerAddr == null)
        {
            workerAddr = new HostAddress(msg.getParam(Message.PARAM_FROM), msg.getIntParam(Message.PARAM_WORKER_PORT));
            workerMap.put(uuid, workerAddr);
        }
        msg.setParam(Message.PARAM_WORKER_ADDRESS, workerAddr);
        msg.setParam(Message.PARAM_ESP_ADDRESS, addr);
        try
        {
            comm.sendMessage(managerAddr, msg);
        }
        catch (IOException ex)
        {
            logger.log("Cannot sent message to " + managerAddr + ": " + ex.getMessage(), ex);
        }
    }

    private ExecutionSiteProxy() throws IOException
    {
        addr = new HostAddress(Utils.getPROP(), "exec_site_proxy_host", "exec_site_proxy_port");
        managerAddr = new HostAddress(Utils.getPROP(), "task_manager_host", "task_manager_port");
        comm.setLocalPort(addr.getPort());
        comm.startServer();
    }

    public static ExecutionSiteProxy startService()
    {
        try
        {
            if (esp == null)
            {
                esp = new ExecutionSiteProxy();
            }
            logger.log("Execution site proxy is started.");
            return esp;
        }
        catch (IOException ex)
        {
            logger.log("Cannot start execution site proxy: " + ex.getLocalizedMessage());
            return null;
        }
    }

    public void uploadFile(Message msg)
    {
        Message response = new Message(Message.TYPE_RESPONSE_TO_MANAGER);
        String filename = msg.getParam("filename");
        String dir = msg.getParam("dir");
        try
        {
            SFTPUtils.getSFTP(msg.getParam("upload_to")).sendFile(dir + filename, dir);
            response.setParam("status", "complete");
        }
        catch (FileTransferException ex)
        {
            response.setParam("status", "fail");
        }
        try
        {
            comm.sendResponseMsg(msg, response);
        }
        catch (IOException ex)
        {
            logger.log("Cannot send message: " + ex.getMessage());
        }
    }
}
