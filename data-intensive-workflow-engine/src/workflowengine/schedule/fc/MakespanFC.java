/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.fc;

import workflowengine.schedule.Schedule;

/**
 *
 * @author orachun
 */
public class MakespanFC implements FC
{

    @Override
    public double getFitness(Schedule sch)
    {
        return sch.getMakespan();
    }
    
}
