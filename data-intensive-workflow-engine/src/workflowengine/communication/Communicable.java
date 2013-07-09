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

public class Communicable
{
    private HashMap<String, Socket> connections = new HashMap<>();
    private int localPort = 0;
    private Message templateMsg = new Message(-1);
    public void startServer()
    {
        Thread serverThread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    ServerSocket server = new ServerSocket(localPort);
                    while (true)
                    {
                        Socket socket = server.accept();
                        String address = socket.getInetAddress().getHostName()+":"+socket.getPort();
                        connections.put(address, socket);
                        ObjectInputStream os = new ObjectInputStream(socket.getInputStream());
                        Message msg = (Message)os.readObject();
                        
                        msg.setParam("FROM", socket.getInetAddress().getHostAddress());
                        msg.setParam("FROM_PORT", socket.getPort());
                        handleMessage(msg);
                        
                        socket.close();
                        connections.remove(address);
                    }
                }
                catch (IOException | ClassNotFoundException ex)
                {
                    Logger.getLogger(Communicable.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        });
        serverThread.start();
    }

    public void handleMessage(Message msg)
    {
        
    }
    
    public void closeSocket(String host, int port)
    {
        String address = host+":"+port;
        Socket s = connections.get(address);
        if(s!= null)
        {
            try
            {
                s.close();
            }
            catch (IOException ex)
            {
                Logger.getLogger(Communicable.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
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
    
    
}
