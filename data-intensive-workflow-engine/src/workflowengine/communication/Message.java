/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.communication;

import java.io.Serializable;
import java.util.HashMap;

/**
 *
 * @author Orachun
 */
public class Message implements Serializable
{

    public static final short TYPE_DISPATCH_TASK = 1;
    public static final short TYPE_GET_NODE_STATUS = 2;
    public static final short TYPE_REGISTER_NODE = 3;
    public static final short TYPE_UPDATE_NODE_STATUS = 4;
    public static final short TYPE_UPDATE_TASK_STATUS = 5;
    public static final short TYPE_SUSPEND_TASK = 6;
    public static final short TYPE_SUSPEND_TASK_COMPLETE = 9;
    public static final short TYPE_GET_TASK_STATUS = 7;
    public static final short TYPE_SUBMIT_WORKFLOW = 8;
    private int type;
    private HashMap<String, Object> params = new HashMap<>();

    public Message(int type)
    {
        this.type = type;
    }

    public int getType()
    {
        return type;
    }

    public String getParam(String s)
    {
        Object o = params.get(s);
        return o == null ? null : o.toString();
    }

    public Object getObjectParam(String s)
    {
        return params.get(s);
    }

    public double getDoubleParam(String s)
    {
        Object o = params.get(s);
        return o == null ? null : Double.parseDouble(o.toString());
    }
    public int getIntParam(String s)
    {
        Object o = params.get(s);
        return o == null ? null : Integer.parseInt(o.toString());
    }

    
    public void setParam(String s, Object o)
    {
        params.put(s, o);
    }
    
    public boolean hasParam(String s)
    {
        return params.containsKey(s);
    }
    
    public void addParamFromMsg(Message msg)
    {
        this.params.putAll(msg.params);
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for(String k : params.keySet())
        {
            sb.append(k).append(": ").append(params.get(k)).append("\n");
        }
        return sb.toString();
    }
    
}
