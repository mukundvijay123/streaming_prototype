package serviceb.flightServer;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;

class SystemMetadata {
    private final Set<String> subscribedTopics;

    public SystemMetadata(){
        this.subscribedTopics= ConcurrentHashMap.newKeySet();
    }

    public void add(String Topic){
        this.subscribedTopics.add(Topic);
    }

    public void remove(String Topic){
        this.subscribedTopics.remove(Topic);
    }

    public boolean contains(String Topic){
        return this.subscribedTopics.contains(Topic);
    }

    public List<String> getSubscribedTopicsAsList() {
        return new ArrayList<>(subscribedTopics);
    }




    
}
