/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import java.io.File;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author udomo
 */
public class SFTPClient
{

    private ChannelSftp sftpChannel;
    private Session session = null;

    private SFTPClient(ChannelSftp sftpChannel, Session s)
    {
        this.sftpChannel = sftpChannel;
        this.session = s;
    }

    public void close()
    {
        sftpChannel.exit();
        session.disconnect();
    }

    public static SFTPClient getSFTPClient(String hostname) throws JSchException
    {
        return getSFTPClient(hostname, 22, Utils.getProp("ssh_user"), Utils.getProp("ssh_pass"));
    }
    
    public static SFTPClient getSFTPClient(String hostname, int port, String user, String pass) throws JSchException
    {
        JSch jsch = new JSch();
        Session session = null;
        session = jsch.getSession(user, hostname, port);
        session.setConfig("StrictHostKeyChecking", "no");
        session.setPassword(pass);
        session.connect();

        Channel channel = session.openChannel("sftp");
        channel.connect();
        ChannelSftp sftpChannel = (ChannelSftp) channel;
        return new SFTPClient(sftpChannel, session);
        
    }

    public void put(String src, String dst) throws SftpException
    {
        sftpChannel.put(src, dst);
    }

    public void get(String src, String dst) throws SftpException
    {
        new File(dst).getParentFile().mkdirs();
        sftpChannel.get(src, dst);
    }

}
