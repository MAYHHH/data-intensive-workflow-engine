/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/**
 *
 * @author udomo
 */
public class XMLUtils
{

    private static Transformer tx = null;
    private static boolean isInited = false;

    public static void init()
    {
        if (isInited)
        {
            return;
        }
        try
        {
            tx = TransformerFactory.newInstance().newTransformer();
            tx.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            tx.setOutputProperty(OutputKeys.INDENT, "no");
            isInited = true;
        }
        catch (TransformerFactoryConfigurationError | TransformerConfigurationException | IllegalArgumentException e)
        {
            e.printStackTrace();
        }
    }

    public static String nodeToString(Node n)
    {
        init();
        try
        {
            DOMSource src = new DOMSource(n);
            StringWriter sr = new StringWriter();
            Result res = new StreamResult(sr);
            tx.transform(src, res);
            return sr.toString();
        }
        catch (Exception e)
        {
            return (e.getMessage());
        }
    }
    
    public static Element strToNode(String xmlStr)
    {
        try
        {
            return DocumentBuilderFactory
                    .newInstance()
                    .newDocumentBuilder()
                    .parse(new ByteArrayInputStream(xmlStr.getBytes()))
                    .getDocumentElement();
        }
        catch (ParserConfigurationException | SAXException | IOException e)
        {
            return null;
        }
    }
    
    public static String argumentTagToCmd(Element jobElement)
    {
        Node n = jobElement.getElementsByTagName("argument").item(0);
        String nodeString = nodeToString(n);
        nodeString = nodeString.replace("<"+n.getNodeName()+">", "");
        nodeString = nodeString.replace("</"+n.getNodeName()+">", "");
        nodeString = nodeString.trim();
        String[] lines = nodeString.split("\n");
        StringBuilder cmd = new StringBuilder();
        for(int i=0;i<lines.length;i++)
        {
            lines[i] = lines[i].trim();
            Element e = strToNode(lines[i]);
            if(e != null)
            {
                lines[i] = e.getAttribute("name");
            }
            cmd.append(lines[i]).append(";");
        }
        cmd.replace(cmd.length()-1, cmd.length(), "");
        return cmd.toString();
    }
}
