/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.resource;

import java.util.ArrayList;
import java.util.HashMap;
import workflowengine.workflow.WorkflowFile;
import workflowengine.utils.Utils;

/**
 *
 * @author Orachun
 */
public class ExecSite
{
    //private SparseGraph<Server, NetworkLink> network = new SparseGraph<>();
    private HashMap<String, NetworkLink> edges = new HashMap<>();
    private ArrayList<Worker> servers = new ArrayList<>();
    private double storageLinkSpd = 1.0;
    /*public void addLink(Worker s1, Worker s2, double MBps)
    {
        if(!servers.contains(s1))
        {
            servers.add(s1);
        }
        if(!servers.contains(s2))
        {
            servers.add(s2);
        }
        NetworkLink link = new NetworkLink(MBps);
        //network.addEdge(link, s1, s2);
        edges.put(s1.getEdgeName(s2), link);
    }*/
    
    public void addWorker(Worker w)
    {
        servers.add(w);
    }
    public double getTransferTime(Worker from, Worker to, WorkflowFile f)
    {
        //return f.getSize()/edges.get(from.getEdgeName(to)).getSpeed();
        return 2*f.getSize()/storageLinkSpd;
    }
    public void setStorageLinkSpeed(double spd)
    {
        this.storageLinkSpd = spd;
    }
    public int getTotalWorkers()
    {
        return servers.size();
    }
    public static ExecSite random(int count)
    {
        ExecSite n = new ExecSite();
        n.setStorageLinkSpeed((7+Math.random()*3)*Utils.GB);
        Worker[] servers = new Worker[count];
        for(int i=0;i<count;i++)
        {
            servers[i] = Worker.getWorker("Server-"+(i+1), 100+Math.random()*50, 100*Utils.GB+Math.random()*50*Utils.GB, 10+Math.random()*3);
            n.addWorker(servers[i]);
        }
        /*for(int i=0;i<count;i++)
        {
            for(int j=i+1;j<count;j++)
            {
                n.addLink(servers[i], servers[j], 0.8*Utils.GB+0.2*Utils.GB*Math.random());
            }
        }*/
        return n;
    }
    
    public Worker getWorker(int i)
    {
        return servers.get(i);
    }
    
    public void print()
    {
        for(String s: edges.keySet())
        {
            System.out.println(s+":"+edges.get(s).getSpeed());
        }
    }
}
