/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package vseesim;

import java.util.LinkedList;
import static vseesim.Vseesim.*;

/**
 *
 * @author Dew
 */
public class Machine
{
    private static int count = 0;
    private int id;
    private LinkedList<Snapshot> snapShots = new LinkedList<>();
    private double spd = Vseesim.RANDOM.nextDouble()*50+100;
    
    
    public Machine()
    {
        this.id = ++count;
    }
    
    public double getExecTime(Partition p)
    {
        return p.getWorkload()/spd;
    }
    
    
    public void addSnapshot(Snapshot s)
    {
        snapShots.add(s);
    }
    public boolean containsSnapshot(Snapshot s)
    {
        return snapShots.contains(s);
    }
    
    public double getTransferTime(Snapshot s)
    {
        double latency = RANDOM.nextGaussian()*LATENCY_SD+LATENCY_MEAN;
        double bw = RANDOM.nextGaussian()*BANDWIDTH_SD+BANDWIDTH_MEAN;
        return latency + s.size/bw;
    }

    @Override
    public String toString()
    {
        return "M"+id;
    }
    
    
}
