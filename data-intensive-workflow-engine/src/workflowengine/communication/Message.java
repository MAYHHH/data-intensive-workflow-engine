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
    public static final short TYPE_NONE = -1;
    public static final short TYPE_DISPATCH_TASK = 1;
    public static final short TYPE_GET_NODE_STATUS = 2;
    public static final short TYPE_REGISTER_NODE = 3;
    public static final short TYPE_UPDATE_NODE_STATUS = 4;
    public static final short TYPE_UPDATE_TASK_STATUS = 5;
    public static final short TYPE_SUSPEND_TASK = 6;
    public static final short TYPE_SUSPEND_TASK_COMPLETE = 7;
    public static final short TYPE_GET_TASK_STATUS = 8;
    public static final short TYPE_SUBMIT_WORKFLOW = 9;
    public static final short TYPE_FILE_UPLOAD_REQUEST = 10;
    public static final short TYPE_RESPONSE = 11;
    public static final short TYPE_REGISTER_FILE = 12;
    
    public static final String PARAM_NEED_RESPONSE = "#NEED_RESPONSE";
    public static final String PARAM_MSG_UUID = "#MSG_UUID";
    public static final String PARAM_STATE = "#STATE";
    public static final String PARAM_RESPONSE_PORT = "#RESPONSE_PORT";
    public static final String PARAM_FROM = "#FROM";
    public static final String PARAM_FROM_PORT = "#FROM_PORT";
    
    public static final String PARAM_FROM_SOURCE = "#FROM_SOURCE";
    public static final String PARAM_WORKER_UUID = "uuid";
    public static final String PARAM_WORKER_ADDRESS = "address";
    public static final String PARAM_WORKER_PORT = "port";
    public static final String PARAM_ESP_ADDRESS = "esp_address";
    
    public static final String STATE_REQUEST = "#REQUEST";
    public static final String STATE_RESPONSE = "#RESPONSE";
    
    public static final String SOURCE_TASK_MANAGER = "#SOURCE_TASK_MANAGER";
    public static final String SOURCE_TASK_EXECUTOR = "#SOURCE_TASK_EXECUTOR";
    
    private int type;
    private HashMap<String, Object> params = new HashMap<>();

    public Message()
    {
        this(TYPE_NONE);
    }
    
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
    public char getCharParam(String s)
    {
        Object o = params.get(s);
        return o == null ? null : (char)o;
    }

    public Object getObjectParam(String s)
    {
        return params.get(s);
    }
    
    public HostAddress getAddressParam(String s)
    {
        Object o = params.get(s);
        return o == null ? null : (HostAddress)o;
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

    public boolean getBooleanParam(String s)
    {
        Object o = params.get(s);
        return o == null ? null : (boolean)o;
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
