package serviceb.arrowio;


public class outputMessage {
    public final String querySession;
    public final String outputRow;

    public outputMessage(String QuerySession,String row){
        this.querySession=QuerySession;
        this.outputRow=row;
    }
}
