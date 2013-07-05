/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.resource;

import java.util.ArrayList;

/**
 *
 * @author Orachun
 */
public class NetworkLink
{
    private static ArrayList<NetworkLink> links = new ArrayList<>();
    private static int count = 0;
    private int id;
    private double MBps; //MBps
    public NetworkLink(double MBps)
    {
        this.MBps = MBps;
        id = count++;
        links.add(this);
    }
    public double getSpeed()
    {
        return MBps;
    }
    public static NetworkLink getLink(int id)
    {
        return links.get(id);
    }
}
