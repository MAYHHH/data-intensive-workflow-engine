/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import com.zehon.sftp.SFTPClient;
import workflowengine.WorkflowEngine;

/**
 *
 * @author udomo
 */
public class SFTPUtils
{
    private static String sshUser = null;
    private static String sshPass = null;
    private static boolean isSFTPInited = false;

    private static void init()
    {
        if (!isSFTPInited)
        {
            sshUser = WorkflowEngine.PROP.getProperty("ssh_user");
            sshPass = WorkflowEngine.PROP.getProperty("ssh_pass");
            isSFTPInited = true;
        }
    }
    public static SFTPClient getSFTP(String host)
    {
        init();
        return new SFTPClient(host,sshUser, sshPass);
    }
    
    public static void main(String[] args) throws Exception
    {
        getSFTP("localhost").sendFile("/root/a.a", "/root/we/");
    }
}
