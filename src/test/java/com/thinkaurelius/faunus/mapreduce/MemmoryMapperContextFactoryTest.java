package com.thinkaurelius.faunus.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;
import static org.mockito.Mockito.*;


/**
 * @author kramachandran
 */
public class MemmoryMapperContextFactoryTest {
    @Test
    public void testCreateMemoryMapperContext() throws Exception {
        Mapper mapper = new Mapper();
        Mapper.Context mockedContext = mock(Mapper.Context.class);
        Configuration mockedConfigure =  new Configuration();
        OutputCommitter mockedOutputCommitter = mock(OutputCommitter.class);
        InputSplit mockInputSplit = mock(InputSplit.class);

        when(mockedContext.getConfiguration()).thenReturn(mockedConfigure);
        when(mockedContext.getTaskAttemptID()).thenReturn(new TaskAttemptID());
        when(mockedContext.getOutputCommitter()).thenReturn(mockedOutputCommitter);
        when(mockedContext.getInputSplit()).thenReturn(mockInputSplit);


   //     Mapper.Context context = MemoryMapContextFactory.createMemoryMapperContext(mapper, mockedContext);

     //   Configuration configuration = context.getConfiguration();

    }


}
