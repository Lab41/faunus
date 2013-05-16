package com.thinkaurelius.faunus.mapreduce;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author kramachandran
 */
public class MemoryMapContextFactory {


    private static Logger logger = LoggerFactory.getLogger(MemoryMapContextFactory.class);

    public static class ContextInterceptor implements MethodInterceptor{

        private Logger logger = LoggerFactory.getLogger(ContextInterceptor.class);
        private MemoryMapper.MemoryMapContext realObject;
        private Object delagate;

        public ContextInterceptor(MemoryMapper.MemoryMapContext realObject, Object delagate)
        {
            this.realObject = realObject;
            this.delagate = delagate;
        }

        public Object intercept(Object o,
                                Method method,
                                Object[] objects,
                                MethodProxy methodProxy) throws Throwable
        {

            logger.debug("Intercepting call : " + method.getName() + "for class " + o.getClass().getName());
            for(Method real_object_method : realObject.getClass().getDeclaredMethods())
            {

                //If it is a method implemented by the object itself the we
                //call the real object
                if(real_object_method.equals(method))
                {
                    logger.debug("call routed to realObject");
                    return methodProxy.invoke(realObject, objects);

                }
            }

            //doesn't match any of the realObject so we will send this to the delgate

            //TODO figure out if methodProxy can be used here instead of method.invoke.
            // If we can use methodProxy then this call might be faster?
            logger.debug("call rounted to delgate");
            return method.invoke(delagate, o);


        }

    }



    public static <KEYIN, VALUEIN, KEYOUT, VALUEOUT> MemoryMapper.MemoryMapContext createMemoryMapperContext(MemoryMapper mapper, Mapper.Context context)
            throws IOException, InterruptedException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException
    {
        boolean isV2 = false;
        Enhancer enhancer = new Enhancer();

        MemoryMapper.MemoryMapContext current = new MemoryMapper(). new MemoryMapContextImpl(context);

        if(isV2)
        {
            Constructor constructor = Class.forName("").getConstructor();
            Object delegate = constructor.newInstance(context.getConfiguration(), context.getTaskAttemptID() == null ? new TaskAttemptID() : context.getTaskAttemptID(), null, null, context.getOutputCommitter(), null, context.getInputSplit());
            return null;
        }
        else
        {
            Mapper.Context delegate = new Mapper().new Context(context.getConfiguration(), context.getTaskAttemptID() == null ? new TaskAttemptID() : context.getTaskAttemptID(), null, null, context.getOutputCommitter(), null, context.getInputSplit());
            ContextInterceptor contextInterceptor = new ContextInterceptor(current, delegate);
            enhancer.setCallback(contextInterceptor);
            enhancer.setSuperclass(Mapper.Context.class);

            Class[] interfaces = {MemoryMapper.MemoryMapContextImpl.class};
            enhancer.setInterfaces(interfaces);

            Class[] argTypes = { Mapper.class, Configuration.class, TaskAttemptID.class, RecordReader.class, RecordWriter.class, OutputCommitter.class, StatusReporter.class};
            Object[] arguments = {mapper, context.getConfiguration(), context.getTaskAttemptID() == null ? new TaskAttemptID() : context.getTaskAttemptID(), null, null, context.getOutputCommitter(), null, context.getInputSplit()};
            

            return (MemoryMapper.MemoryMapContext) enhancer.create(argTypes, arguments);
        }
    }
}
