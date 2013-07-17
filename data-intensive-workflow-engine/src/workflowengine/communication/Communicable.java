/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.communication;

/**
 *
 * @author Orachun
 */
import java.io.*;
import java.net.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import workflowengine.utils.SynchronizedHashMap;
import workflowengine.utils.Utils;

public class Communicable
{
    private String name;
    private int localPort = 0;
    private Message templateMsg = new Message();
    private SynchronizedHashMap<String, Message> waitingMsgs = new SynchronizedHashMap<>();
    private SynchronizedHashMap<String, Message> responseMsgs = new SynchronizedHashMap<>();
    private final String commUUID = Utils.uuid();

    public Communicable(String name)
    {
        this.name = name;
    }
    
    public void startServer() throws IOException
    {
        final ServerSocket server = new ServerSocket(localPort);
        new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                while (true)
                {
                    try
                    {
                        final Socket socket = server.accept();
                        new Thread(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                try
                                {
                                    Message msg = readMessage(socket);
                                    socket.close();
                                    
                                    String res = msg.getParam(Message.PARAM_STATE);
                                    String senderUUID = msg.getParam(Message.PARAM_SOURCE_UUID);
                                    if (res != null && res.equals(Message.STATE_RESPONSE) && senderUUID.equals(commUUID))
                                    {
                                        String uuid = msg.getParam(Message.PARAM_RESPONSE_FOR_MSG_UUID);
                                        Message orgMsg = waitingMsgs.get(uuid);
                                        responseMsgs.put(uuid, msg);
                                        synchronized (orgMsg)
                                        {
                                            orgMsg.notify();
                                        }
                                    }
                                    else
                                    {
                                        handleMessage(msg);
                                    }
                                }
                                catch (IOException | ClassNotFoundException ex)
                                {
                                    Logger.getLogger(Communicable.class.getName()).log(Level.SEVERE, null, ex);
                                }
                            }
                        }).start();
                    }
                    catch (IOException ex)
                    {
                        Logger.getLogger(Communicable.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        }).start();
    }

    public void handleMessage(Message msg)
    {
    }

    
    /**
     * Read a message object from the given socket
     * @param socket
     * @return
     * @throws ClassNotFoundException
     * @throws IOException 
     */
    private Message readMessage(Socket socket) throws ClassNotFoundException, IOException
    {
        synchronized(socket)
        {
            ObjectInputStream os = new ObjectInputStream(socket.getInputStream());
            Message msg = (Message) os.readObject();
            msg.setParam(Message.PARAM_FROM, socket.getInetAddress().getHostAddress());
            msg.setParam(Message.PARAM_FROM_PORT, socket.getPort());
            
            if(msg.getBooleanParam(Message.PARAM_PRINT_AFTER_RECEIVE))
            {
                System.err.println("-----" + name + "-----");
                System.err.println(msg);
                System.err.println("------------------");
            }
            return msg;
        }
    }
    
    private void prepareMsg(Message msg)
    {
        msg.addAllParamsFromMsg(templateMsg);
        msg.setParamIfNotExist(Message.PARAM_NEED_RESPONSE, false);
        msg.setParamIfNotExist(Message.PARAM_PRINT_BEFORE_SENT, false);
        msg.setParamIfNotExist(Message.PARAM_PRINT_AFTER_RECEIVE, false);
        msg.setParamIfNotExist(Message.PARAM_NEED_RESPONSE, false);
        msg.setParamIfNotExist(Message.PARAM_SOURCE_UUID, this.commUUID);
    }
    
    public void sendMessage(String host, int port, Message msg) throws IOException
    {
        prepareMsg(msg);
        if(msg.getBooleanParam(Message.PARAM_PRINT_BEFORE_SENT))
        {
            System.err.println("-----" + name + "-----");
            System.err.println(msg);
            System.err.println("------------------");
        }
        Socket s = new Socket(host, port);
        ObjectOutputStream os = new ObjectOutputStream(s.getOutputStream());
        os.writeObject(msg);
        os.close();
        s.close();
    }

    public void sendMessage(HostAddress addr, Message msg) throws IOException
    {
        sendMessage(addr.getHost(), addr.getPort(), msg);
    }

    public void setLocalPort(int localPort)
    {
        this.localPort = localPort;
    }

    public void setTemplateMsgParam(String key, Object val)
    {
        this.templateMsg.setParam(key, val);
    }

    /**
     * 
     * @param targetHost
     * @param targetPort
     * @param responsePort
     * @param msg
     * @param isSync block until the response message is returned or not
     * @return
     * @throws IOException 
     */
    private Message sendForResponse(String targetHost, int targetPort, int responsePort, Message msg, boolean isSync) throws IOException
    {
        String uuid = Utils.uuid();
        waitingMsgs.put(uuid, msg);
        msg.setParam(Message.PARAM_NEED_RESPONSE, true);
        msg.setParam(Message.PARAM_MSG_UUID, uuid);
        msg.setParam(Message.PARAM_STATE, Message.STATE_REQUEST);
        msg.setParam(Message.PARAM_RESPONSE_PORT, responsePort);
        sendMessage(targetHost, targetPort, msg);
        if(!isSync)
        {
            return null;
        }
        else
        {
            return getResponseMessage(msg, false);
        }
    }
    
    public Message sendForResponseSync(String targetHost, int targetPort, int responsePort, Message msg) throws IOException
    {
        return sendForResponse(targetHost, targetPort, responsePort, msg, true);
    }

    
    public Message sendForResponseSync(HostAddress target, int responsePort, Message msg) throws IOException, InterruptedException
    {
        return sendForResponseSync(target.getHost(), target.getPort(), responsePort, msg);
    }

    /**
     * Send message to the specified host and port and save response message to
     * be obtained later via getResponseMessage method
     * Must not reuse msg for other sendForResponse method
     *
     * @param targetHost 
     * @param targetPort
     * @param responsePort port number to receive response message
     * @param msg
     * @throws IOException
     */
    public void sendForResponseAsync(String targetHost, int targetPort, int responsePort, Message msg) throws IOException
    {
        sendForResponse(targetHost, targetPort, responsePort, msg, false);
    }

    /**
     * Send message to the specified host and port and save response message to
     * be obtained later via getResponseMessage method
     * Must not reuse msg for other sendForResponse method
     *
     * @param target
     * @param responsePort port number to receive response message
     * @param msg
     * @throws IOException
     */
    public void sendForResponseAsync(HostAddress target, int responsePort, Message msg) throws IOException
    {
        System.err.println(msg);
        sendForResponseAsync(target.getHost(), target.getPort(), responsePort, msg);
    }
    
    /**
     * Wait and get response message of sending from sendForResponseAsync method
     * @param sentMsg request message to get the response for
     * @param checkExisting whether the method should check the existing of sent message
     * @return 
     */
    
    private Message getResponseMessage(Message sentMsg, boolean checkExisting)
    {
        String uuid = sentMsg.getParam(Message.PARAM_MSG_UUID);
        if(checkExisting && !waitingMsgs.containsKey(uuid))
        {
            return null;
        }
        if (!responseMsgs.containsKey(uuid))
        {
            synchronized (sentMsg)
            {
                try
                {
                    sentMsg.wait();
                }
                catch (InterruptedException ex)
                {
                    Logger.getLogger(Communicable.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        Message respMsg = responseMsgs.get(uuid);
        responseMsgs.remove(uuid);
        waitingMsgs.remove(uuid);
        return respMsg;
    }

    /**
     * Wait and get response message of sending from sendForResponseAsync method
     *
     * @param sentMsg
     * @return response Message or null if the given message is not in 
     * the waiting list
     * @throws InterruptedException
     */
    public Message getResponseMessage(Message sentMsg)
    {
        return getResponseMessage(sentMsg, true);
    }

    /**
     * Send response message for the given original message
     * @param original
     * @param response
     * @throws IOException 
     */
    public void sendResponseMsg(HostAddress to, Message original, Message response) throws IOException
    {
        
//        response.setParamFromMsg(original, Message.PARAM_WORKER_UUID);
        response.setParamFromMsg(original, Message.PARAM_SOURCE_UUID);
        response.setParam(Message.PARAM_STATE, Message.STATE_RESPONSE);
        response.setParam(Message.PARAM_RESPONSE_FOR_MSG_UUID, original.getParam(Message.PARAM_MSG_UUID));
        response.setParam(Message.PARAM_RESPONSE_PORT, original.getParam(Message.PARAM_RESPONSE_PORT));
        sendMessage(to, response);
//        sendMessage(original.getParam(Message.PARAM_FROM), original.getIntParam(Message.PARAM_RESPONSE_PORT), response);
    }
    
    /**
     * Send empty response message as acknowledgment message
     * @param original original message
     * @param msgType message type indicate whether it's to manager or worker.
     * Only use Message.TYPE_RESPONSE_TO_WORKER or Message.TYPE_RESPONSE_TO_MANAGER 
     * @throws IOException 
     */
    public void sendEmptyResponseMsg(HostAddress to, Message original, short msgType) throws IOException
    {
        Message response = new Message(msgType);
        sendResponseMsg(to, original, response);
    }
}
