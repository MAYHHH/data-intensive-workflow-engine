/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import workflowengine.communication.HostAddress;
import workflowengine.utils.Utils;
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
    private double estLinkSpd = 174;
    private double estLatency = 0.5;
    private double transferUnitCost = 0.01/Utils.GB;
    private Iterable<Worker> workerIterable = new Iterable<Worker>()
    {
        @Override
        public Iterator<Worker> iterator()
        {
            return workers.iterator();
        }
    };

    public void addWorker(Worker w)
    {
        workers.add(w);
    }

    public double getTransferTime(Worker from, Worker to, WorkflowFile file)
    {
        return estLatency + file.getSize() / estLinkSpd;
    }

    public double getTransferTime(Worker from, Worker to, WorkflowFile[] files)
    {
        double total = 0;
        for (WorkflowFile f : files)
        {
            total += getTransferTime(from, to, f);
        }
        return total;
    }

    public double getTransferCost(Worker from, Worker to, WorkflowFile file)
    {
        return transferUnitCost*file.getSize();
    }
    public double getTransferCost(Worker from, Worker to, WorkflowFile[] files)
    {
        double total = 0;
        for (WorkflowFile f : files)
        {
            total += f.getSize();
        }
        return transferUnitCost*total;
    }
    
    public void setStorageLinkSpeed(double spd)
    {
        this.estLinkSpd = spd;
    }

    public int getTotalWorkers()
    {
        return workers.size();
    }
    
    
    public static ExecSite random(int count)
    {
        ExecSite n = new ExecSite();
        n.setStorageLinkSpeed((7+Math.random()*3)*Utils.GB);
        Worker[] servers = new Worker[count];
        HostAddress addr = new HostAddress("randomhost", 0);
        for(int i=0;i<count;i++)
        {
            servers[i] = Worker.updateWorkerStatus(addr, addr, count,  100+Math.random()*50, 100*Utils.GB+Math.random()*50*Utils.GB, 10+Math.random()*3, Utils.uuid());
//            servers[i] = Worker.getWorker("Server-"+(i+1), 100+Math.random()*50, 100*Utils.GB+Math.random()*50*Utils.GB, 10+Math.random()*3);
            n.addWorker(servers[i]);
        }
        return n;
    }
    
    public static ExecSite generate(int count)
    {
        ExecSite n = new ExecSite();
        n.setStorageLinkSpeed((7+Math.random()*3)*Utils.GB);
        Worker[] servers = new Worker[count];
        HostAddress addr = new HostAddress("randomhost", 0);
        for(int i=0;i<count;i++)
        {
            servers[i] = Worker.updateWorkerStatus(addr, addr, -1,  1, 1, 1, Utils.uuid());
//            servers[i] = Worker.getWorker("Server-"+(i+1), 100+Math.random()*50, 100*Utils.GB+Math.random()*50*Utils.GB, 10+Math.random()*3);
            n.addWorker(servers[i]);
        }
        return n;
    }

    public Worker getWorker(int i)
    {
        return workers.get(i);
    }

    public Iterable<Worker> getWorkerIterable()
    {
        return workerIterable;
    }

    public int getWorkerIndex(Worker w)
    {
        return workers.indexOf(w);
    }

    public void print()
    {
        for (String s : edges.keySet())
        {
            System.out.println(s + ":" + edges.get(s).getSpeed());
        }
    }
}
