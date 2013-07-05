/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.workflow;

import workflowengine.utils.DBException;
import workflowengine.utils.DBRecord;

/**
 *
 * @author Orachun
 */
public class WorkflowFile
{
//    private static int count = 0;
//    private static ArrayList<WorkflowFile> files = new ArrayList<>();
//    private int id;
    private double size = 0;//MB
    private String name = "";
    private int dbid;
    private WorkflowFile(String name, double size)
    {
        this.size = size;
        this.name = name;
//        id = count++;
//        files.add(this);
    }
    public static WorkflowFile getFile(String name, double size) throws DBException
    {
        WorkflowFile f = getFileFromDB(name);
        if(f == null)
        {
            f = new WorkflowFile(name, size);
            f.dbid = new DBRecord("file", "name", name, "estsize", size).insert();
        }
        return f;
    }
    
    public static WorkflowFile getFileFromDB(String name)
    {
        WorkflowFile f;
        try
        {
            DBRecord res = DBRecord.select("file", new DBRecord("file", "name", name)).get(0);
            f = new WorkflowFile(res.get("name"), res.getDouble("estsize"));
            f.dbid = res.getInt("fid");
            return f;
        }
        catch(IndexOutOfBoundsException ex)
        {
            return null;
        }
    }
    
//    public static WorkflowFile get(int id)
//    {
//        return files.get(id);
//    }

    public double getSize()
    {
        return size;
    }

    public String getName()
    {
        return name;
    }

    public int getDbid()
    {
        return dbid;
    }


}
