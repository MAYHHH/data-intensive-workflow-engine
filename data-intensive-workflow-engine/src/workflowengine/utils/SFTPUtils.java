///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package workflowengine.utils;
//
//import com.zehon.FileTransferStatus;
//import com.zehon.exception.FileTransferException;
//import com.zehon.sftp.SFTPClient;
//
///**
// *
// * @author udomo
// */
//public class SFTPUtils
//{
//    public static final int STATUS_FAILURE = FileTransferStatus.FAILURE;
//    public static final int STATUS_FILE_NOT_EXISTS = FileTransferStatus.FILE_NOT_EXISTS;
//    public static final int STATUS_INVALID_SETTINGS = FileTransferStatus.INVALID_SETTINGS;
//    public static final int STATUS_SUCCESS = FileTransferStatus.SUCCESS;
//    private static String sshUser = null;
//    private static String sshPass = null;
//    private static boolean isSFTPInited = false;
//
//    private static void init()
//    {
//        if (!isSFTPInited)
//        {
//            sshUser = Utils.getProp("ssh_user");
//            sshPass = Utils.getProp("ssh_pass");
//            isSFTPInited = true;
//        }
//    }
////    public static SFTPClient getSFTP(String host)
////    {
////        init();
////        return new SFTPClient(host,sshUser, sshPass);
////    }
//    
//    public static void createFolderPath(SFTPClient client, String folderPath) throws FileTransferException
//    {
//        String[] dirs = folderPath.split("/");
//        String path = "";
//        for(String d : dirs)
//        {
//            if(d.equals(""))
//            {
//                continue;
//            }
//            String p = path+"/"+d;
//            if(!client.folderExists(p))
//            {
//                System.err.println("Crating dir "+p);
//                client.createFolder(d, path);
//            }
//            path = p;
//        }
//    }
//}
