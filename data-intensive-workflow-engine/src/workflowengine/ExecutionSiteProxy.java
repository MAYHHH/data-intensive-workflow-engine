/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.io.IOException;
import java.util.HashMap;
import workflowengine.communication.Communicable;
import workflowengine.communication.HostAddress;
import workflowengine.communication.Message;

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
    
    private Communicable comm = new Communicable(){

        @Override
        public void handleMessage(Message msg)
        {
            
            HostAddress target = managerAddr;
            try
            {
                HostAddress workerAddr;
                switch(msg.getType())
                {
                    //From manager to executor
                    case Message.TYPE_DISPATCH_TASK:
                    case Message.TYPE_GET_NODE_STATUS:
                    case Message.TYPE_SUSPEND_TASK:
                        workerAddr = workerMap.get(msg.getParam("uuid"));
                        target = workerAddr;
                        comm.sendMessage(workerAddr, msg);
                        break;
                        
                    //From executor to manager
                    case Message.TYPE_UPDATE_TASK_STATUS:
                    case Message.TYPE_UPDATE_NODE_STATUS:
                    case Message.TYPE_SUBMIT_WORKFLOW:
                    case Message.TYPE_SUSPEND_TASK_COMPLETE:
                        String uuid = msg.getParam("uuid");
                        workerAddr = workerMap.get(uuid);
                        if(workerAddr == null)
                        {
                            workerAddr = new HostAddress(msg.getParam("FROM"), msg.getIntParam("port"));
                            workerMap.put(uuid, workerAddr);
                        }
                        msg.setParam("address", workerAddr);
                        msg.setParam("esp_address", addr);
                        target = managerAddr;
                        comm.sendMessage(managerAddr, msg);
                        break;
                }
            }
            catch (IOException ex)
            {
                logger.log("Cannot sent message to "+target+": "+ex.getMessage());
            }
        }
    };
    
    
    private ExecutionSiteProxy()
    {
        addr = new HostAddress(WorkflowEngine.PROP, "exec_site_proxy_host", "exec_site_proxy_port");
        managerAddr = new HostAddress(WorkflowEngine.PROP, "task_manager_host", "task_manager_port");
        comm.setLocalPort(addr.getPort());
        comm.startServer();
    }
    
    public static ExecutionSiteProxy start()
    {
        if(esp == null)
        {
            esp = new ExecutionSiteProxy();
        }
        logger.log("Execution site proxy is started.");
        return esp;
    }
    
}
