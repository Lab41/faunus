package com.thinkaurelius.faunus.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;


/**
 * @author kramachandran
 */
public class MemmoryMapperContextFactoryTest {
    @Test
    public void testCreateMemoryMapperContext() throws Exception {
        MemoryMapper mapper = new MemoryMapper();
        Mapper.Context mockedContext = mock(Mapper.Context.class);
        Configuration mockConfiguration =  new Configuration();
        OutputCommitter mockedOutputCommitter = mock(OutputCommitter.class);
        InputSplit mockInputSplit = mock(InputSplit.class);

        when(mockedContext.getConfiguration()).thenReturn(mockConfiguration);
        when(mockedContext.getTaskAttemptID()).thenReturn(new TaskAttemptID());
        when(mockedContext.getOutputCommitter()).thenReturn(mockedOutputCommitter);
        when(mockedContext.getInputSplit()).thenReturn(mockInputSplit);


        Mapper.Context context = (Mapper.Context) MemoryMapContextFactory.createMemoryMapperContext(mapper, mockedContext);

        MemoryMapper.MemoryMapContext memoryMapContext = (MemoryMapper.MemoryMapContext) context;

        //checking to make sure that the functions that don't exists on the super class execut.
        memoryMapContext.stageConfiguration(1);
        memoryMapContext.write(new Text("hi"), new Text("hi"));


    }


}
