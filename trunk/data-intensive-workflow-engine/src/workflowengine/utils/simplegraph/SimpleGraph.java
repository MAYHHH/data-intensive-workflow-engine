/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils.simplegraph;

import java.io.Serializable;
import java.util.HashMap;


/**
 *
 * @author orachun
 */
public class SimpleGraph<T> implements Serializable
{
    private SimpleGraphNode<T> root;
    private HashMap<T, SimpleGraphNode<T>> nodeMap = new HashMap<>();

    public SimpleGraph()
    {
    }
    
    public SimpleGraph(T rootInfo)
    {
        root = new SimpleGraphNode<>(rootInfo);
    }

    public SimpleGraphNode<T> getRoot()
    {
        return root;
    }

    public void setRoot(SimpleGraphNode<T> root)
    {
        this.root = root;
    }
    
    public SimpleGraphNode<T> getNode(T info)
    {
        SimpleGraphNode<T> node = nodeMap.get(info);
        if(node == null)
        {
            node = new SimpleGraphNode<>(info);
            nodeMap.put(info, node);
        }
        return node;
    }
    
}
