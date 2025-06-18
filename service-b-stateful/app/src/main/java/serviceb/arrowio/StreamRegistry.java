package serviceb.arrowio;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StreamRegistry {
    private final Map<String,Map<String,Stream>> Registry=new ConcurrentHashMap<>();

    public void createContext(String ctxName) throws Exception{
        if(this.Registry.containsKey(ctxName)){
            throw  new Exception("Each context must have a unique identifying string");
        }

        this.Registry.put(ctxName,new ConcurrentHashMap<>());

    }

    public void delContext(String ctxName)throws Exception{
        if(!this.Registry.containsKey(ctxName)){
            throw new Exception("No context with name "+ctxName+" exists in the registry");
        }
        this.Registry.remove(ctxName);
    }

    private Map<String,Stream> getContext(String ctxName)throws Exception{
        if(!this.Registry.containsKey(ctxName)){
            throw new Exception("No context with name "+ctxName+" exists in the registry");
        }   
        return this.Registry.get(ctxName);
    }

    public void AddStream(String ctxName,String StreamName,Stream stream)throws Exception{
        try{
            Map<String,Stream> Context=getContext(ctxName);
            if(Context.containsKey(StreamName)){
                throw new Exception("This stream name is used in this context");
            }
            Context.put(StreamName,stream);
        }catch(Exception e){
            throw e;
        }
    }

    
    public Stream getStream(String ctxName,String StreamName)throws Exception{
        try{
            Map<String,Stream> Context=getContext(ctxName);
            Stream stream=Context.get(StreamName);
            if(stream==null){
                throw new Exception("No such stream exists in the context");
            }
            return stream;
        }catch(Exception e){    
            throw e;
        }
    }


}
