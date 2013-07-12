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
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import workflowengine.utils.Utils;

public class Communicable
{
    private int localPort = 0;
    private Message templateMsg = new Message(-1);
    private HashMap<String, Message> waitingThreads = new HashMap<>();
    private HashMap<String, Message> responseMsg = new HashMap<>();

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
                                    ObjectInputStream os = new ObjectInputStream(socket.getInputStream());
                                    Message msg = (Message) os.readObject();
                                    msg.setParam("FROM", socket.getInetAddress().getHostAddress());
                                    msg.setParam("FROM_PORT", socket.getPort());
                                    String res = msg.getParam("#STATE");
                                    if (res != null && res.equals("RESPONSE"))
                                    {
                                        String uuid = msg.getParam("#MSG_UUID");
                                        responseMsg.put(uuid, msg);
                                        Message orgMsg = waitingThreads.get(uuid);
                                        synchronized (orgMsg)
                                        {
                                            orgMsg.notify();
                                        }
                                    }
                                    else
                                    {
                                        handleMessage(msg);
                                    }
                                    socket.close();
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

    public void sendMessage(String host, int port, Message msg) throws IOException
    {
        msg.addParamFromMsg(templateMsg);
        Socket s = new Socket(host, port);
        ObjectOutputStream os = new ObjectOutputStream(s.getOutputStream());
        os.writeObject(msg);
        os.flush();
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

    public Message sendForResponseSync(String targetHost, int targetPort, int responsePort, Message msg) throws IOException
    {
        String uuid = Utils.uuid();
        msg.setParam("#MSG_UUID", uuid);
        msg.setParam("#STATE", "REQUEST");
        msg.setParam("#RESPONSE_PORT", responsePort);
        waitingThreads.put(uuid, msg);
        sendMessage(targetHost, targetPort, msg);

        synchronized (msg)
        {
            try
            {
                msg.wait();
            }
            catch (InterruptedException ex)
            {
                Logger.getLogger(Communicable.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        Message respMsg = responseMsg.get(uuid);
        responseMsg.remove(uuid);
        return respMsg;
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
        String uuid = Utils.uuid();
        msg.setParam("#MSG_UUID", uuid);
        msg.setParam("#STATE", "REQUEST");
        msg.setParam("#RESPONSE_PORT", responsePort);
        waitingThreads.put(uuid, msg);
        sendMessage(targetHost, targetPort, msg);
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
        sendForResponseAsync(target.getHost(), target.getPort(), responsePort, msg);
    }

    /**
     * Wait and get response message of sending from sendForResponseAsync method
     *
     * @param sentMsg
     * @return
     * @throws InterruptedException
     */
    public Message getResponseMessage(Message sentMsg)
    {
        String uuid = sentMsg.getParam("#MSG_UUID");
        if (!responseMsg.containsKey(uuid))
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
        Message respMsg = responseMsg.get(uuid);
        responseMsg.remove(uuid);
        return respMsg;
    }

    /**
     * Send response message for the given original message
     * @param original
     * @param response
     * @throws IOException 
     */
    public void sendResponseMsg(Message original, Message response) throws IOException
    {
        response.setParam("#STATE", "RESPONSE");
        response.setParam("#MSG_UUID", original.getParam("#MSG_UUID"));
        sendMessage(original.getParam("FROM"), original.getIntParam("#RESPONSE_PORT"), response);
    }
}
