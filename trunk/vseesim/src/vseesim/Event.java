/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package vseesim;

import java.util.HashMap;

public class Event implements Comparable<Event>
{
    public static final int TYPE_TASK_FIN = 1;
    public static final int TYPE_FILE_SENT = 2;
    private HashMap <String,Object> hmap = new HashMap<>();
    
    private int time;
    private int type;

    public Event(int type)
    {
        this.type = type;
    }
    

    public Event(int time, int type)
    {
        this.time = time;
        this.type = type;
    }

    public int getTime()
    {
        return time;
    }
    
    public void setTime(int time)
    {
        this.time = time;
    }
    
    public int getType()
    {
        return type;
    }

    public Object getProperty(String s)
    {
        return  hmap.get(s);
    }
    public void setProperty(String s, Object o)
    {
        hmap.put(s,o);
    }
    public int compareTo(Event e)
    {
        if(this.time < e.time)
        {
            return -1;
        }
        else if (this.time == e.time)
        {
            return 0;
        }
        else 
        {
            return 1;
        }
    }
    
    
}
