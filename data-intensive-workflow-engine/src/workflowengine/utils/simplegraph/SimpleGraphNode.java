/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils.simplegraph;

import java.io.Serializable;
import java.util.LinkedList;

/**
 *
 * @author orachun
 */
public class SimpleGraphNode<T>  implements Serializable
{
    private LinkedList<SimpleGraphNode> children = new LinkedList<>();
    private LinkedList<SimpleGraphNode> parents = new LinkedList<>();
    private T info;

    SimpleGraphNode()
    {
    }
    
    SimpleGraphNode(T info)
    {
        this.info = info;
    }
    public void setInfo(T info)
    {
        this.info = info;
    }

    public T getInfo()
    {
        return info;
    }
    public void addChildNode(SimpleGraphNode<T> n)
    {
        this.children.add(n);
        n.parents.add(n);
    }
    public LinkedList<SimpleGraphNode> getParents()
    {
        return new LinkedList<>(parents);
    }
    public LinkedList<SimpleGraphNode> getChildren()
    {
        return new LinkedList<>(children);
    }
}
