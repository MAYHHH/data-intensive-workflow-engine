/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.resource;

import java.util.ArrayList;
import java.util.HashMap;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author Orachun
 */
public class ExecSite
{
    //private SparseGraph<Server, NetworkLink> network = new SparseGraph<>();
    private HashMap<String, NetworkLink> edges = new HashMap<>();
    private ArrayList<Worker> workers = new ArrayList<>();
    private double storageLinkSpd = 1.0;

    public void addWorker(Worker w)
    {
        workers.add(w);
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
        return workers.size();
    }
//    public static ExecSite random(int count)
//    {
//        ExecSite n = new ExecSite();
//        n.setStorageLinkSpeed((7+Math.random()*3)*Utils.GB);
//        Worker[] servers = new Worker[count];
//        for(int i=0;i<count;i++)
//        {
//            servers[i] = Worker.getWorker("Server-"+(i+1), 100+Math.random()*50, 100*Utils.GB+Math.random()*50*Utils.GB, 10+Math.random()*3);
//            n.addWorker(servers[i]);
//        }
//        /*for(int i=0;i<count;i++)
//        {
//            for(int j=i+1;j<count;j++)
//            {
//                n.addLink(servers[i], servers[j], 0.8*Utils.GB+0.2*Utils.GB*Math.random());
//            }
//        }*/
//        return n;
//    }
    
    public Worker getWorker(int i)
    {
        return workers.get(i);
    }
    
    public void print()
    {
        for(String s: edges.keySet())
        {
            System.out.println(s+":"+edges.get(s).getSpeed());
        }
    }
}
