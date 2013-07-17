/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package vseesim;

import java.util.ArrayList;
import static vseesim.Vseesim.*;

/**
 *
 * @author Dew
 */
public class Partition
{
    private static int count = 0;
    private int id;
    public ArrayList<Partition> parents = new ArrayList<>();
    public ArrayList<Partition> children = new ArrayList<>();
    private Snapshot snapshot;
    private boolean executing = false;
    private boolean finished = false;
    private double workload;

    public double getWorkload()
    {
        return workload;
    }

    public void setWorkload(double workload)
    {
        this.workload = workload;
    }


    public Partition()
    {
        id = ++count;
    }
    
    public void addChild(Partition p)
    {
        this.children.add(p);
        p.parents.add(this);
    }
    
    public double getOutputSize()
    {
        return 0;
    }
    public double getTransferTime()
    {
        double latency = RANDOM.nextGaussian()*LATENCY_SD+LATENCY_MEAN;
        double bw = RANDOM.nextGaussian()*BANDWIDTH_SD+BANDWIDTH_MEAN;
        return latency + getOutputSize()/bw;
    }

    public Snapshot getSnapshot()
    {
        return snapshot;
    }

    public void setSnapshot(Snapshot snapshot)
    {
        this.snapshot = snapshot;
    }
    
    
    public void setFinished(boolean f)
    {
        finished = f;
    }

    public boolean isExecuting()
    {
        return executing;
    }

    public void setExecuting(boolean executing)
    {
        this.executing = executing;
    }
    
    

    public boolean isFinished()
    {
        return finished;
    }
    
    public boolean hasParentSnapshotsOn(Machine m)
    {
        for(Partition p : parents)
        {
            if(!m.containsSnapshot(p.getSnapshot()))
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString()
    {
        return "P"+id;
    }
    
    
}
