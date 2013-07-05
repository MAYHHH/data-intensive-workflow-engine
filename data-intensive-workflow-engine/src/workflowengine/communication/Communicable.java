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
import java.util.Random;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Communicable
{
    private HashMap<String, Socket> connections = new HashMap<>();
    private static Random r = new Random();
    public void startServer(final int port)
    {
        Thread serverThread = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    ServerSocket server = new ServerSocket(port);
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
//            String address = host+":"+port;
//            Socket s;
//            if(!connections.containsKey(address))
//            {
//                s = new Socket(host, port);
//                connections.put(address, s);
//            }
//            else
//            {
//                s = connections.get(address);
//            }
            Socket s = new Socket(host, port);
            ObjectOutputStream os = new ObjectOutputStream(s.getOutputStream());
            os.writeObject(msg);
            os.flush();
            s.close();
    }
}
