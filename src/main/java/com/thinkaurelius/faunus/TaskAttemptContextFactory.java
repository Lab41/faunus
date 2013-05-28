package com.thinkaurelius.faunus;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * @author kramachandran
 */
public class TaskAttemptContextFactory {

    private static boolean useV21 = false;
    static
    {
        boolean v21 = true;
        final String PACKAGE = "org.apache.hadoop.mapreduce";
        try {
            Class.forName("org.apache.hadoop.mapreduce.task.MapContextImpl");
        } catch (ClassNotFoundException cnfe) {
            v21 = false;
        }
        useV21 = v21;


    }

    public static TaskAttemptContext createTaskAttemptContext(Configuration configuration, TaskAttemptID taskAttemptID) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        if(useV21)
        {
            Class classTaskAttemptContextImpl = Class.forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");

            Constructor constructor = classTaskAttemptContextImpl.getConstructor(Configuration.class, TaskAttemptID.class);
            return (TaskAttemptContext) constructor.newInstance(configuration, taskAttemptID);
        }
        else
        {
            Class classTaskAttemptContextImpl = Class.forName("org.apache.hadoop.mapreduce.TaskAttemptContext");

            Constructor constructor = classTaskAttemptContextImpl.getConstructor(Configuration.class, TaskAttemptID.class);
            return (TaskAttemptContext) constructor.newInstance(configuration, taskAttemptID);
        }


    }


}
