/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import com.zehon.exception.FileTransferException;
import com.zehon.sftp.SFTPClient;
import java.io.IOException;
import workflowengine.communication.Communicable;
import workflowengine.communication.HostAddress;
import workflowengine.communication.Message;
import workflowengine.utils.SFTPUtils;
import workflowengine.utils.SynchronizedHashMap;
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
    private SynchronizedHashMap<String, HostAddress> workerMap = new SynchronizedHashMap<>(); //<uuid, host address>
    private SynchronizedHashMap<String, Message> workerHeartbeatMsg = new SynchronizedHashMap<>(); //<uuid, host address>
    
    private Communicable comm = new Communicable("Execution Site Proxy")
    {
        @Override
        public void handleMessage(Message msg)
        {
//            logger.log("Received msg from "+msg.getParam(Message.PARAM_FROM));
//            logger.log(msg.toString());
            switch (msg.getType())
            {
                case Message.TYPE_UPDATE_NODE_STATUS:
                    if(msg.getBooleanParam(Message.PARAM_NEED_RESPONSE))
                    {
                        forwardMsgToManager(msg);
                    }
                    else
                    {
                        String uuid = msg.getParam(Message.PARAM_WORKER_UUID);
                        workerHeartbeatMsg.put(uuid, msg);
                    }
                    break;
                    
                //From manager to executor
                case Message.TYPE_RESPONSE_TO_WORKER:
                case Message.TYPE_DISPATCH_TASK:
                case Message.TYPE_GET_NODE_STATUS:
                case Message.TYPE_SUSPEND_TASK:
                    forwardMsgToWorker(msg);
                    break;

                //From executor to manager
                case Message.TYPE_UPDATE_TASK_STATUS:
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
    
    private void prepareMsgToManager(Message msg)
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
    }
    
    private void forwardMsgToManager(Message msg)
    {
        prepareMsgToManager(msg);
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
        comm.setTemplateMsgParam(Message.PARAM_ESP_ADDRESS, addr);
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
                new Thread(new Runnable() {

                    @Override
                    public void run()
                    {
                        while(true)
                        {
                            esp.sendHeartbeat();
                            try
                            {
                                Thread.sleep(10000);
                            }
                            catch (InterruptedException ex)
                            {
                                logger.log("Interrupted while sleeping.", ex);
                            }
                        }
                    }
                }).start();
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
    
    public void sendHeartbeat()
    {
        try
        {
            Message msg = new Message(Message.TYPE_UPDATE_NODE_STATUS);
            Message[] msgs = workerHeartbeatMsg.values().toArray(new Message[]{});
            for(Message m : msgs)
            {
                prepareMsgToManager(m);
            }
            msg.setParam(Message.PARAM_WORKER_MSGS, msgs);
            comm.sendMessage(managerAddr, msg);
            workerHeartbeatMsg.clear();
        }
        catch (IOException ex)
        {
            logger.log("Cannot send heartbeat messages.");
        }
        
    }

    public void uploadFile(Message msg)
    {
        Message response = new Message(Message.TYPE_RESPONSE_TO_MANAGER);
        String filename = msg.getParam("filename");
        String dir = msg.getParam("dir");
        String filepath = dir+filename;
        try
        {
            String uploadTo = msg.getParam("upload_to");
            SFTPClient client = SFTPUtils.getSFTP(uploadTo);
            logger.log("Sending file "+filepath+" to "+ uploadTo);
            if(Utils.isDir(filepath))
            {
                client.sendFolder(filepath, filepath, null);
            }
            else
            {
                client.sendFile(filepath, dir);
            }
            logger.log("Done.");
            response.setParam("upload_complete", true);
            response.setParam("status", "complete");
        }
        catch (FileTransferException ex)
        {
            logger.log("Cannot upload file "+dir+filename+" to "+ msg.getParam("upload_to")+".", ex);
            response.setParam("upload_complete", false);
        }
        try
        {
            comm.sendResponseMsg(managerAddr, msg, response);
        }
        catch (IOException ex)
        {
            logger.log("Cannot send message: " + ex.getMessage());
        }
    }
}
